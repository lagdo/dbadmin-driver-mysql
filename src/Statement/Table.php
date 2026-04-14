<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableAlterDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableCreateDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractTable;

use function addcslashes;
use function array_map;
use function implode;
use function preg_match;
use function preg_replace;

class Table extends AbstractTable
{
    /**
     * @param ColumnDto $column
     *
     * @return string
     */
    private function getTableColumnClause(ColumnDto $column): string
    {
        if (preg_match('~ GENERATED~', $column->field->default ?? '')) {
            // swap default and null
            // MariaDB doesn't support NULL on virtual columns
            $column->field->default = $this->_engine()->flavor() === 'maria' ? "" : $column->field->nullable;
            $column->field->nullable = $column->field->hasDefault();
        }
        return $column->clause();
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQueries(TableCreateDto $table): array
    {
        $clauses = array_map($this->getTableColumnClause(...), $table->columns);
        $clauses = [
            ...$clauses,
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];

        $status = $table->options($this->_engine()->quote(...));
        // Todo: append partitioning clauses to $status

        $tableName = $this->_statement()->escapeTableName($table->name);
        $clauses = implode(', ', $clauses);
        return ["CREATE TABLE $tableName ($clauses) $status"];
    }

    /**
     * @inheritDoc
     */
    public function getAlterTableQueries(TableAlterDto $table): array
    {
        $clauses = [];
        foreach ($table->addedColumns as $column) {
            $clauses[] = 'ADD ' . $this->getTableColumnClause($column) . $column->after;
        }
        foreach ($table->changedColumns as $fieldName => $column) {
            $fieldName = $this->_statement()->escapeId($fieldName);
            // The field rename is done here.
            $clauses[] = "CHANGE $fieldName " . $this->getTableColumnClause($column) . $column->after;
        }
        foreach ($table->droppedColumns as $fieldName) {
            $clauses[] = 'DROP ' . $this->_statement()->escapeId($fieldName);
        }
        $clauses = [
            ...$clauses,
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];

        if ($table->name !== $table->current->name) {
            $clauses[] = 'RENAME TO ' . $this->_statement()->escapeTableName($table->name);
        }

        $status = $table->options($this->_engine()->quote(...));
        // Todo: append partitioning clauses to $status
        if ($status !== '') {
            $clauses[] = $status;
        }

        $tableName = $this->_statement()->escapeTableName($table->name);
        $clauses = implode(', ', $clauses);
        return ["ALTER TABLE $tableName $clauses"];
    }

    /**
     * @inheritDoc
     */
    public function getExportTableQueries(string $table, bool $autoIncrement, string $style): string
    {
        $query = $this->_engine()->result("SHOW CREATE TABLE " .
            $this->_statement()->escapeTableName($table), 1);
        if (!$autoIncrement) {
            $query = preg_replace('~ AUTO_INCREMENT=\d+~', '', $query); //! skip comments
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function getTruncateTableQuery(string $table): string
    {
        return "TRUNCATE " . $this->_statement()->escapeTableName($table);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTriggerQuery(string $table): string
    {
        $query = "";
        $tableName = $this->_engine()->quote(addcslashes($table, "%_\\"));
        foreach ($this->_engine()->rows("SHOW TRIGGERS LIKE $tableName") as $row) {
            $query .= "\nCREATE TRIGGER " . $this->_statement()->escapeId($row["Trigger"]) .
                " $row[Timing] $row[Event] ON " . $this->_statement()->escapeTableName($row["Table"]) .
                " FOR EACH ROW\n$row[Statement];;\n";
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function getAlterIndexQueries(string $table, array $alter, array $drop): array
    {
        $clauses = [];
        foreach ($drop as $index) {
            $clauses[] = 'DROP INDEX ' . $this->_statement()->escapeId($index->name);
        }
        foreach ($alter as $index) {
            $indexType = $index->type === 'PRIMARY' ? 'PRIMARY KEY' :  $index->type;
            if ($index->name !== '') {
                $indexType .= ' ' . $this->_statement()->escapeId($index->name);
            }
            $columns = implode(', ', $index->columns);
            $clauses[] = "ADD $indexType ($columns)";
        }
        $tableName = $this->_statement()->escapeTableName($table);
        return ["ALTER TABLE $tableName " . implode(', ', $clauses)];
    }
}
