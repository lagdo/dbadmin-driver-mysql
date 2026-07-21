<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Exception\DriverException;
use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDdDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\ForeignKeyDdDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableAlterDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableCreateDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDdDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractTable;

use function addcslashes;
use function array_map;
use function count;
use function implode;
use function in_array;
use function preg_match;
use function preg_replace;

class Table extends AbstractTable
{
    /**
     * @inheritDoc
     */
    protected function getColumnModifier(ColumnDdDto $input, TableDdDto $table): string
    {
        // $indexModifier = $this->getPrimaryKeyModifier($input, $table);
        if (!$input->autoIncrement) {
            return ''; // No auto increment.
        }

        // From function auto_increment() in mysql.inc.php.
        // don't overwrite primary key by auto increment (in ALTER TABLE queries)
        $autoIncrementColumn = $table->statusAutoIncrementColumn();
        if ($autoIncrementColumn === null) {
            return ' AUTO_INCREMENT'; // No previous auto increment.
        }

        $indexModifier = '';
        foreach ($this->_engine()->indexes($table->name) as $index) {
            if (in_array($autoIncrementColumn->name, $index->columns, true)) {
                $indexModifier = ''; // Can erase the UNIQUE value.
                break;
            }

            if ($index->type === 'PRIMARY') {
                $indexModifier = ' UNIQUE';
            }
        }
        return " AUTO_INCREMENT$indexModifier";
    }

    /**
     * @param TableDdDto $table
     *
     * @return bool
     */
    protected function primaryKeyChanged(TableDdDto $table): bool
    {
        // Check if the auto increment is removed on a primary key.
        foreach ($table->editedColumns() as $column) {
            if ($column->column->primary && $column->autoIncrementChanged()) {
                return true;
            }
        }
        return $table->primaryKeyChanged();
    }

    /**
     * @param ColumnDdDto $input
     * @param TableDdDto $table
     *
     * @return string
     */
    protected function getAddColumnClause(ColumnDdDto $input, TableDdDto $table): string
    {
        if (preg_match('~ GENERATED~', $input->default ?? '')) {
            // swap default and null
            // MariaDB doesn't support NULL on virtual inputs
            $input->default = $this->_engine()->maria() ? '' : $input->nullable;
            $input->nullable = $input->hasDefault();
        }
        return parent::getAddColumnClause($input, $table) . $input->after;
    }

    /**
     * @param TableDdDto $table
     *
     * @return int
     */
    private function getAutoIncrementValue(TableDdDto $table): int
    {
        return match(true) {
            // Nothing to do for auto increment.
            !$table->autoIncrementChanged() => 0,
            // Create a new sequence.
            $table->autoIncrementAdded() =>
                $table->hasAutoIncrement() ? $table->autoIncrement : 1,
            // Change the current auto increment value.
            $table->autoIncrementValueChanged() => $table->autoIncrement,
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
        if ($table->autoIncrementInput() !== null) {
            $tableOptions[] = 'AUTO_INCREMENT=' . $this->getAutoIncrementValue($table);
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
            throw new DriverException($this->_utils()->lang('The table name must be defined.'));
        }

        $clauses = array_map(fn(ColumnDdDto $input) =>
            $this->getAddColumnClause($input, $table), $table->addedColumns());

        $clauses = [
            ...$clauses,
            ...$this->getCreatePrimaryKeyClause($table),
            ...$this->getCreateForeignKeyClauses($table),
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
     * @param ColumnDdDto $input
     * @param TableAlterDto $table
     *
     * @return string
     */
    private function getEditColumnClause(ColumnDdDto $input, TableAlterDto $table): string
    {
        // The column rename is done here, if the new name is different.
        $currName = $this->_statement()->escapeId($input->statusName());
        $clause = $this->getAddColumnClause($input, $table);
        return "CHANGE $currName $clause";
    }

    /**
     * @param ColumnDdDto $input
     *
     * @return bool
     */
    private function columnChanged(ColumnDdDto $input): bool
    {
        return $input->nameChanged() || $input->nullableChanged() ||
            $input->autoIncrementChanged() || $input->defaultChanged() ||
            $input->typeChanged() || $input->onUpdateChanged() ||
            $input->hasComment()/* || $input->after !== ''*/;
    }

    /**
    * @inheritDoc
     */
    protected function getDropPrimaryKeyClause(TableAlterDto $table): array
    {
        return $this->primaryKeyChanged($table) ? ['DROP PRIMARY KEY'] : [];
    }

    /**
    * @inheritDoc
     */
    protected function getDeleteForeignKeyClauses(TableAlterDto $table): array
    {
        $filter = fn(ForeignKeyDdDto $foreignKey) =>
            $foreignKey->edited() || $foreignKey->dropped();
        $foreignKeys = array_filter($table->foreignKeys, $filter);
        $formatter = fn(ForeignKeyDdDto $foreignKey) =>
            'DROP FOREIGN KEY ' . $this->_statement()->escapeId($foreignKey->name);
        return array_map($formatter, $foreignKeys);
    }

    /**
     * @param TableAlterDto $table
     *
     * @return array
     */
    private function getRenameTableQuery(TableAlterDto $table): array
    {
        if (!$table->nameChanged()) {
            return [];
        }

        $currName = $this->_statement()->escapeTableName($table->statusName());
        $nextName = $this->_statement()->escapeTableName($table->name);
        return ["ALTER TABLE $currName RENAME TO $nextName"];
    }

    /**
     * @inheritDoc
     */
    public function getAlterTableQueries(TableAlterDto $table): array
    {
        if ($table->name === '') {
            throw new DriverException($this->_utils()->lang('The table name must be defined.'));
        }

        $addColumnsClauses = array_map(fn(ColumnDdDto $input) => "ADD " .
            $this->getAddColumnClause($input, $table), $table->addedColumns());

        $changedColumns = array_filter($table->editedColumns(), $this->columnChanged(...));
        $changeColumnsClauses = array_map(fn(ColumnDdDto $input) =>
            $this->getEditColumnClause($input, $table), $changedColumns);

        $clauses = [
            ...$addColumnsClauses,
            ...$changeColumnsClauses,
            ...$this->getDropColumnClauses($table),
            ...$this->getCreatePrimaryKeyClause($table, 'ADD '),
            ...$this->getCreateForeignKeyClauses($table, 'ADD '),
        ];
        $tableOptions = $this->getTableOptions($table);
        if ($tableOptions !== '') {
            $clauses[] = $tableOptions;
        }
        if (count($clauses) === 0) {
            return [];
        }

        $tableName = $this->_statement()->escapeTableName($table->name);
        return [
            ...$this->getDropConstraintsQuery($table),
            ...$this->getRenameTableQuery($table),
            "ALTER TABLE $tableName\n  " . implode(",\n  ", $clauses),
        ];
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
