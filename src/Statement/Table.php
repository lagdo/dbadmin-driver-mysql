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
        return parent::getAddColumnClause($input) . $input->after;
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
        $status = $table->options($this->_engine()->quote(...));

        return ["CREATE TABLE $tableName (\n  $clauses\n) $status"];
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
     * @return string
     */
    public function getRenameTableClause(TableAlterDto $table): string
    {
        return 'RENAME TO ' . $this->_statement()->escapeTableName($table->name);
    }

    /**
     * @param ColumnInputDto $input
     *
     * @return bool
     */
    public function columnChanged(ColumnInputDto $input): bool
    {
        return $input->nameChanged() || $input->nullableChanged() ||
            $input->valueChanged() || $input->typeChanged() ||
            $input->onUpdateChanged() || $input->commentChanged()
            /* || $input->after !== ''*/;
    }

    private function getTableClauses(TableAlterDto $table): array
    {
        $tableClauses = $table->nameChanged() ? [$this->getRenameTableClause($table)] : [];

        $tableOptions = [];
        if ($table->commentChanged()) {
            $tableOptions[] = 'COMMENT=' . $this->_engine()->quote($table->comment);
        }
        if ($table->engineChanged()) {
            $tableOptions[] = 'ENGINE=' . $this->_engine()->quote($table->engine);
        }
        if ($table->collationChanged()) {
            $tableOptions[] = 'COLLATE ' . $this->_engine()->quote($table->collation);
        }
        if ($table->hasAutoIncrement()) {
            $tableOptions[] = "AUTO_INCREMENT={$table->autoIncrement}";
        }
        if (count($tableOptions) > 0) {
            $tableClauses[] = implode(' ', $tableOptions);
        }

        // Todo: append partitioning clauses to $status

        return $tableClauses;
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
            ...$this->getTableClauses($table),
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
        $tableName = $this->_statement()->escapeTableName($table);
        $query = $this->_engine()->columnValue("SHOW CREATE TABLE $tableName", 1);
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
