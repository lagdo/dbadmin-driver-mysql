<?php

namespace Lagdo\DbAdmin\Support\MySql\Grammar;

use Lagdo\DbAdmin\Support\Db\Engine\Grammar\AbstractTable;
use Lagdo\DbAdmin\Support\Dto\ColumnDto;
use Lagdo\DbAdmin\Support\Dto\TableAlterDto;
use Lagdo\DbAdmin\Support\Dto\TableCreateDto;

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
            $column->field->default = $this->driver->flavor() === 'maria' ? "" : $column->field->nullable;
            $column->field->nullable = $column->field->hasDefault();
        }
        return $column->clause();
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQueries(TableCreateDto $table): array
    {
        $clauses = array_map(fn(ColumnDto $column) =>
            $this->getTableColumnClause($column), $table->columns);
        $clauses = [
            ...$clauses,
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];

        $status = $table->options(fn($str) => $this->driver->quote($str));
        // Todo: append partitioning clauses to $status

        $tableName = $this->grammar->escapeTableName($table->name);
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
            $fieldName = $this->grammar->escapeId($fieldName);
            // The field rename is done here.
            $clauses[] = "CHANGE $fieldName " . $this->getTableColumnClause($column) . $column->after;
        }
        foreach ($table->droppedColumns as $fieldName) {
            $clauses[] = 'DROP ' . $this->grammar->escapeId($fieldName);
        }
        $clauses = [
            ...$clauses,
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];

        if ($table->name !== $table->current->name) {
            $clauses[] = 'RENAME TO ' . $this->grammar->escapeTableName($table->name);
        }

        $status = $table->options(fn($str) => $this->driver->quote($str));
        // Todo: append partitioning clauses to $status
        if ($status !== '') {
            $clauses[] = $status;
        }

        $tableName = $this->grammar->escapeTableName($table->name);
        $clauses = implode(', ', $clauses);
        return ["ALTER TABLE $tableName $clauses"];
    }

    /**
     * @inheritDoc
     */
    public function getTableDefinitionQueries(string $table, bool $autoIncrement, string $style): string
    {
        $query = $this->driver->result("SHOW CREATE TABLE " .
            $this->grammar->escapeTableName($table), 1);
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
        return "TRUNCATE " . $this->grammar->escapeTableName($table);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTriggerQuery(string $table): string
    {
        $query = "";
        foreach ($this->driver->rows("SHOW TRIGGERS LIKE " .
            $this->driver->quote(addcslashes($table, "%_\\"))) as $row) {
            $query .= "\nCREATE TRIGGER " . $this->grammar->escapeId($row["Trigger"]) .
                " $row[Timing] $row[Event] ON " . $this->grammar->escapeTableName($row["Table"]) .
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
            $clauses[] = 'DROP INDEX ' . $this->grammar->escapeId($index->name);
        }
        foreach ($alter as $index) {
            $indexType = $index->type === 'PRIMARY' ? 'PRIMARY KEY' :  $index->type;
            if ($index->name !== '') {
                $indexType .= ' ' . $this->grammar->escapeId($index->name);
            }
            $columns = implode(', ', $index->columns);
            $clauses[] = "ADD $indexType ($columns)";
        }
        $tableName = $this->grammar->escapeTableName($table);
        return ["ALTER TABLE $tableName " . implode(', ', $clauses)];
    }
}
