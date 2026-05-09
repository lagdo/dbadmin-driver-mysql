<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnInputDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableAlterDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableCreateDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractTable;

use function addcslashes;
use function array_map;
use function count;
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
    protected function getAddColumnClause(ColumnInputDto $input): string
    {
        if (preg_match('~ GENERATED~', $input->default ?? '')) {
            // swap default and null
            // MariaDB doesn't support NULL on virtual inputs
            $input->default = $this->_engine()->maria() ? '' : $input->nullable;
            $input->nullable = $input->hasDefault();
        }
        return parent::getAddColumnClause($input);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQueries(TableCreateDto $table): array
    {
        if ($table->name === '') {
            return [];
        }

        $clauses = array_map($this->getAddColumnClause(...), $table->columns['added']);
        $clauses = [
            ...$clauses,
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];
        if (count($clauses) === 0) {
            return [];
        }

        // Todo: append partitioning clauses to $status
        $tableName = $this->_statement()->escapeTableName($table->name);
        $clauses = implode(",\n  ", $clauses);
        $status = $table->options($this->_engine()->quote(...));

        return ["CREATE TABLE $tableName (\n  $clauses\n) $status"];
    }

    /**
     * @inheritDoc
     */
    public function getAlterTableQueries(TableAlterDto $table): array
    {
        if ($table->name === '') {
            return [];
        }

        $addColumnsClauses = array_map(function(ColumnInputDto $input) {
            $columnClause = $this->getAddColumnClause($input);
            return "ADD $columnClause{$input->after}";
        }, $table->columns['added']);
        $editColumnsClauses = array_map(function(ColumnInputDto $input) {
            $currColumnName = $this->_statement()->escapeId($input->column->name);
            // The column rename is done here, if the new name is different.
            $columnClause = $this->getAddColumnClause($input);
            return "CHANGE $currColumnName $columnClause{$input->after}";
        }, $table->columns['edited']);
        $dropColumnsClauses = array_map(fn(string $columnName) =>
            'DROP ' . $this->_statement()->escapeId($columnName), $table->columns['dropped']);

        $tableClauses = [];
        if ($table->name !== $table->current->name) {
            $tableClauses[] = 'RENAME TO ' . $this->_statement()->escapeTableName($table->name);
        }
        $status = $table->options($this->_engine()->quote(...));
        // Todo: append partitioning clauses to $status
        if ($status !== '') {
            $tableClauses[] = $status;
        }

        $clauses = [
            ...$addColumnsClauses,
            ...$editColumnsClauses,
            ...$dropColumnsClauses,
            ...$this->getForeignKeyClauses($table, 'ADD '),
            ...$tableClauses,
        ];
        if (count($clauses) === 0) {
            return [];
        }

        $tableName = $this->_statement()->escapeTableName($table->current->name);

        return ["ALTER TABLE $tableName\n  " . implode(",\n  ", $clauses)];
    }

    /**
     * @inheritDoc
     */
    public function getExportTableQueries(string $table, bool $autoIncrement, string $style): string
    {
        $query = $this->_engine()->columnValue("SHOW CREATE TABLE " .
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

        if (count($clauses) === 0) {
            return [];
        }

        $tableName = $this->_statement()->escapeTableName($table);
        return ["ALTER TABLE $tableName " . implode(', ', $clauses)];
    }
}
