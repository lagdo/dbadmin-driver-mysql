<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnInputDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;
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
     * @param ColumnInputDto $input
     *
     * @return string
     */
    private function getTableColumnClause(ColumnInputDto $input): string
    {
        if (preg_match('~ GENERATED~', $input->column->default ?? '')) {
            // swap default and null
            // MariaDB doesn't support NULL on virtual inputs
            $input->column->default = $this->_engine()->maria() ? '' : $input->column->nullable;
            $input->column->nullable = $input->column->hasDefault();
        }
        return $input->clauses();
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQueries(TableCreateDto $table): array
    {
        $clauses = array_map($this->getTableColumnClause(...), $table->inputs['added']);
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
        foreach ($table->inputs['added'] as $input) {
            $columnClause = $this->getTableColumnClause($input);
            $clauses[] = "ADD $columnClause{$input->after}";
        }
        foreach ($table->inputs['edited'] as $input) {
            $columnName = $this->_statement()->escapeId($input->column->name);
            // The column rename is done here.
            $columnClause = $this->getTableColumnClause($input);
            $clauses[] = "CHANGE $columnName $columnClause{$input->after}";
        }
        foreach ($table->droppedColumns as $columnName) {
            $clauses[] = 'DROP ' . $this->_statement()->escapeId($columnName);
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
        $tableName = $this->_engine()->quote(addcslashes($table, "%_\\"));
        $queries = array_map(function(array $row) {
            $trigger = $this->_statement()->escapeId($row['Trigger']);
            $triggerTable = $this->_statement()->escapeTableName($row['Table']);
            return "
CREATE TRIGGER $trigger {$row['Timing']} {$row['Event']} ON $triggerTable FOR EACH ROW
{$row['Statement']};;
";
        }, $this->_engine()->rows("SHOW TRIGGERS LIKE $tableName"));

        return implode('', $queries);
    }

    /**
     * @inheritDoc
     */
    public function getAlterIndexQueries(string $table, array $alter, array $drop): array
    {
        $dropClauses = array_map(fn(IndexDto $index) =>
            'DROP INDEX ' . $this->_statement()->escapeId($index->name), $drop);
        $alterClauses = array_map(function(IndexDto $index) {
            $indexType = $index->type === 'PRIMARY' ? 'PRIMARY KEY' :  $index->type;
            if ($index->name !== '') {
                $indexType .= ' ' . $this->_statement()->escapeId($index->name);
            }
            $columns = implode(', ', $index->columns);
            return "ADD $indexType ($columns)";
        }, $alter);
        $clauses = [...$dropClauses, ...$alterClauses];

        $tableName = $this->_statement()->escapeTableName($table);
        return ["ALTER TABLE $tableName " . implode(', ', $clauses)];
    }
}
