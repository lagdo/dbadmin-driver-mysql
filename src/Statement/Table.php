<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDdDto;
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
        $indexModifier = $this->getPrimaryKeyModifier($input, $table);
        if (!$input->autoIncrement) {
            return $indexModifier;
        }

        // From function auto_increment() in mysql.inc.php.
        // don't overwrite primary key by auto increment (in ALTER TABLE queries)
        $autoIncrementColumn = $table->statusAutoIncrementColumn();
        if ($autoIncrementColumn !== null) {
            foreach ($this->_engine()->indexes($table->name) as $index) {
                if (in_array($autoIncrementColumn->name, $index->columns, true)) {
                    $indexModifier = '';
                    break;
                }

                if ($index->type === 'PRIMARY') {
                    $indexModifier = ' UNIQUE';
                }
            }
        }

        return " AUTO_INCREMENT$indexModifier";
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
        $table->setupAutoIncrement();

        return match(true) {
            // Nothing to do for auto increment.
            !$table->autoIncrementChanged() => 0,
            // Create a new sequence.
            $table->autoIncrementAdded() =>
                $table->hasAutoIncrement() ? $table->autoIncrement : 1,
            // change the current auto increment value.
            $table->autoIncrementValueChanged() => $table->autoIncrement,
            // Drop the current sequence.
            $table->autoIncrementRemoved() => 1,
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

        $inputs = $table->addedColumns();
        $clauses = array_map(fn(ColumnDdDto $input) =>
            $this->getAddColumnClause($input, $table), $inputs);

        if ($table->primaryKeyColumnCount() > 1) {
            $clauses[] = $table->primaryKeyClause($this->_statement()->escapeId(...));
        }

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
     * @param ColumnDdDto $input
     * @param TableAlterDto $table
     *
     * @return string
     */
    private function getEditColumnClause(ColumnDdDto $input, TableAlterDto $table): string
    {
        $currName = $this->_statement()->escapeId($input->column->name);

        // The column rename is done here, if the new name is different.
        return "CHANGE $currName " . $this->getAddColumnClause($input, $table);
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
     * @param TableAlterDto $table
     * @param string $nextName
     *
     * @return string
     */
    private function getRenameTableQuery(TableAlterDto $table, string $nextName): string
    {
        $currName = $this->_statement()->escapeTableName($table->status->name);
        return "ALTER TABLE $currName RENAME TO $nextName";
    }

    /**
     * @inheritDoc
     */
    public function getAlterTableQueries(TableAlterDto $table): array
    {
        if ($table->name === '') {
            return [];
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
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];
        $tableOptions = $this->getTableOptions($table);
        if ($tableOptions !== '') {
            $clauses[] = $tableOptions;
        }
        if (count($clauses) === 0) {
            return [];
        }

        $tableName = $this->_statement()->escapeTableName($table->name);
        return $table->nameChanged() ? [
            $this->getRenameTableQuery($table, $tableName),
            "ALTER TABLE $tableName\n  " . implode(",\n  ", $clauses),
        ] : [
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
