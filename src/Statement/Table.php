<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnInputDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableAlterDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableCreateDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDdDto;
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
        return parent::getAddColumnClause($input) . $input->after;
    }

    /**
     * @param TableDdDto $table
     *
     * @return int
     */
    private function getAutoIncrementValue(TableDdDto $table): int
    {
        $table->setupAutoIncrement();

        return match(true) {
            // Nothing to do for auto increment.
            !$table->autoIncrementDefined() => 0,
            // Create a new sequence.
            $table->autoIncrementEnabled() =>
                $table->hasAutoIncrement() ? $table->autoIncrement : 1,
            // change the current auto increment value.
            $table->autoIncrementValueChanged() => $table->autoIncrement,
            // Drop the current sequence.
            $table->autoIncrementDisabled() => 1,
            default => 0,
        };
    }

    /**
     * @param TableDdDto $table
     *
     * @return string
     */
    private function getTableOptions(TableDdDto $table): string
    {
        $tableOptions = [];
        if ($table->hasComment()) {
            $tableOptions[] = 'COMMENT=' . $this->_engine()->quote($table->comment);
        }
        if ($table->engineChanged()) {
            $tableOptions[] = 'ENGINE=' . $this->_engine()->quote($table->engine);
        }
        if ($table->collationChanged()) {
            $tableOptions[] = 'COLLATE ' . $this->_engine()->quote($table->collation);
        }
        $autoIncrement = $this->getAutoIncrementValue($table);
        if ($autoIncrement > 0) {
            $tableOptions[] = "AUTO_INCREMENT={$autoIncrement}";
        }

        // Todo: append partitioning clauses to $status

        return implode(' ', $tableOptions);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQueries(TableCreateDto $table): array
    {
        if ($table->name === '') {
            return [];
        }

        $clauses = array_map($this->getAddColumnClause(...), $table->addedColumns());
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
        $tableOptions = $this->getTableOptions($table);

        return ["CREATE TABLE $tableName (\n  $clauses\n)" .
            ($tableOptions === '' ? '' : " $tableOptions")];
    }

    /**
     * @return string
     */
    private function getEditColumnClause(ColumnInputDto $input): string
    {
        $currName = $this->_statement()->escapeId($input->column->name);

        // The column rename is done here, if the new name is different.
        return "CHANGE $currName " . $this->getAddColumnClause($input);
    }

    /**
     * @param TableAlterDto $table
     *
     * @return array
     */
    public function getRenameTableClause(TableAlterDto $table): array
    {
        return !$table->nameChanged() ? [] :
            ['RENAME TO ' . $this->_statement()->escapeTableName($table->name)];
    }

    /**
     * @param ColumnInputDto $input
     *
     * @return bool
     */
    public function columnChanged(ColumnInputDto $input): bool
    {
        return $input->nameChanged() || $input->nullableChanged() ||
            $input->autoIncrementDefined() || $input->defaultChanged() ||
            $input->typeChanged() || $input->onUpdateChanged() ||
            $input->hasComment()/* || $input->after !== ''*/;
    }

    /**
     * @inheritDoc
     */
    public function getAlterTableQueries(TableAlterDto $table): array
    {
        if ($table->name === '') {
            return [];
        }

        $addColumnsClauses = array_map(fn(ColumnInputDto $input) =>
            "ADD " . $this->getAddColumnClause($input), $table->addedColumns());

        $changedColumns = array_filter($table->editedColumns(), $this->columnChanged(...));
        $editColumnsClauses = array_map($this->getEditColumnClause(...), $changedColumns);

        $clauses = [
            ...$addColumnsClauses,
            ...$editColumnsClauses,
            ...$this->getDropColumnClauses($table),
            ...$this->getForeignKeyClauses($table, 'ADD '),
            ...$this->getRenameTableClause($table),
        ];
        $tableOptions = $this->getTableOptions($table);
        if ($tableOptions !== '') {
            $clauses[] = $tableOptions;
        }
        if (count($clauses) === 0) {
            return [];
        }

        $tableName = $this->_statement()->escapeTableName($table->status->name);
        return ["ALTER TABLE $tableName\n  " . implode(",\n  ", $clauses)];
    }

    /**
     * @inheritDoc
     */
    public function getExportTableQueries(string $table, bool $autoIncrement, string $style): string
    {
        $tableName = $this->_statement()->escapeTableName($table);
        $query = $this->_engine()->columnValue("SHOW CREATE TABLE $tableName", 1);
        if (!$autoIncrement) {
            //! skip comments
            $query = preg_replace('~ AUTO_INCREMENT=\d+~', '', $query);
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
        $triggers = $this->_engine()->rows("SHOW TRIGGERS LIKE $tableName");
        $queries = array_map(function(array $row) {
            $trigger = $this->_statement()->escapeId($row['Trigger']);
            $triggerTable = $this->_statement()->escapeTableName($row['Table']);
            return "
CREATE TRIGGER $trigger {$row['Timing']} {$row['Event']} ON $triggerTable FOR EACH ROW
{$row['Statement']}";
        }, $triggers);

        return implode(";;\n", $queries);
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
