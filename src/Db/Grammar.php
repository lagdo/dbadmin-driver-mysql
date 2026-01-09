<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Db\AbstractGrammar;
use Lagdo\DbAdmin\Driver\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Dto\TableAlterDto;
use Lagdo\DbAdmin\Driver\Dto\TableCreateDto;
use Lagdo\DbAdmin\Driver\Dto\TableFieldDto;
use Lagdo\DbAdmin\Driver\Dto\TableSelectDto;

use function array_map;
use function count;
use function in_array;
use function preg_match;
use function preg_replace;
use function str_replace;

class Grammar extends AbstractGrammar
{
    /**
     * @inheritDoc
     */
    public function escapeId(string $idf): string
    {
        return "`" . str_replace("`", "``", $idf) . "`";
    }

    /**
     * @inheritDoc
     */
    public function getAutoIncrementModifier(): string
    {
        $autoIncrementIndex = " PRIMARY KEY";
        // don't overwrite primary key by auto increment
        $table = $this->utils->input->getTable();
        $fields = $this->utils->input->getFields();
        $autoIncrementField = $this->utils->input->getAutoIncrementField();
        if ($table != "" && $autoIncrementField) {
            foreach ($this->driver->indexes($table) as $index) {
                if (in_array($fields[$autoIncrementField]["orig"], $index->columns, true)) {
                    $autoIncrementIndex = "";
                    break;
                }
                if ($index->type == "PRIMARY") {
                    $autoIncrementIndex = " UNIQUE";
                }
            }
        }
        return " AUTO_INCREMENT$autoIncrementIndex";
    }

    /**
     * @inheritDoc
     */
    protected function limitToOne(string $table, string $query, string $where): string
    {
        return $this->getLimitClause($query, $where, 1, 0);
    }

    /**
     * @inheritDoc
     */
    public function buildSelectQuery(TableSelectDto $select): string
    {
        $prefix = '';
        if (($select->page) && ($select->limit) && !empty($select->group) &&
            count($select->group) < count($select->fields)) {
            $prefix = 'SQL_CALC_FOUND_ROWS ';
        }

        return $prefix . parent::buildSelectQuery($select);
    }

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
    public function getTableCreationQueries(TableCreateDto $table): array
    {
        $clauses = array_map(fn(ColumnDto $column) =>
            $this->getTableColumnClause($column), $table->columns);
        $clauses = [
            ...$clauses,
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];

        $status = $table->options(fn($str) => $this->driver->quote($str));
        // Todo: append partitioning clauses to $status

        $tableName = $this->driver->escapeTableName($table->name);
        $clauses = implode(', ', $clauses);
        return ["CREATE TABLE $tableName ($clauses) $status"];
    }

    /**
     * @inheritDoc
     */
    public function getTableAlterationQueries(TableAlterDto $table): array
    {
        $clauses = [];
        foreach ($table->addedColumns as $column) {
            $clauses[] = 'ADD ' . $this->getTableColumnClause($column) . $column->after;
        }
        foreach ($table->changedColumns as $fieldName => $column) {
            $fieldName = $this->driver->escapeId($fieldName);
            // The field rename is done here.
            $clauses[] = "CHANGE $fieldName " . $this->getTableColumnClause($column) . $column->after;
        }
        foreach ($table->droppedColumns as $fieldName) {
            $clauses[] = 'DROP ' . $this->driver->escapeId($fieldName);
        }
        $clauses = [
            ...$clauses,
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];

        if ($table->name !== $table->current->name) {
            $clauses[] = 'RENAME TO ' . $this->driver->escapeTableName($table->name);
        }

        $status = $table->options(fn($str) => $this->driver->quote($str));
        // Todo: append partitioning clauses to $status
        if ($status !== '') {
            $clauses[] = $status;
        }

        $tableName = $this->driver->escapeTableName($table->name);
        $clauses = implode(', ', $clauses);
        return ["ALTER TABLE $tableName $clauses"];
    }

    /**
     * @inheritDoc
     */
    public function getTableDefinitionQueries(string $table, bool $autoIncrement, string $style): string
    {
        $query = $this->driver->result("SHOW CREATE TABLE " .
            $this->driver->escapeTableName($table), 1);
        if (!$autoIncrement) {
            $query = preg_replace('~ AUTO_INCREMENT=\d+~', '', $query); //! skip comments
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function getTableTruncationQuery(string $table): string
    {
        return "TRUNCATE " . $this->driver->escapeTableName($table);
    }

    /**
     * @param string $database
     * @param string $style
     *
     * @return string
     */
    private function getCreateDatabaseQuery(string $database, string $style = ''): string
    {
        if (!preg_match('~CREATE~', $style)) {
            return '';
        }
        $create = $this->driver->result("SHOW CREATE DATABASE $database", 1);
        if (!$create) {
            return '';
        }

        $this->driver->setUtf8mb4($create);
        $drop = $style !== 'DROP+CREATE' ? '' : "DROP DATABASE IF EXISTS $database;\n";
        return "{$drop}{$create};\n";
    }

    /**
     * @inheritDoc
     */
    public function getUseDatabaseQuery(string $database, string $style = ''): string
    {
        $name = $this->escapeId($database);
        return $this->getCreateDatabaseQuery($name, $style) . "USE $name;";
    }

    /**
     * @inheritDoc
     */
    public function getTriggerCreationQuery(string $table): string
    {
        $query = "";
        foreach ($this->driver->rows("SHOW TRIGGERS LIKE " .
            $this->driver->quote(addcslashes($table, "%_\\"))) as $row) {
            $query .= "\nCREATE TRIGGER " . $this->escapeId($row["Trigger"]) .
                " $row[Timing] $row[Event] ON " . $this->driver->escapeTableName($row["Table"]) .
                " FOR EACH ROW\n$row[Statement];;\n";
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function convertField(TableFieldDto $field): string
    {
        if (preg_match("~binary~", $field->type)) {
            return "HEX(" . $this->escapeId($field->name) . ")";
        }
        if ($field->type == "bit") {
            return "BIN(" . $this->escapeId($field->name) . " + 0)"; // + 0 is required outside MySQLnd
        }
        if (preg_match("~geometry|point|linestring|polygon~", $field->type)) {
            return ($this->driver->minVersion(8) ? "ST_" : "") . "AsWKT(" . $this->escapeId($field->name) . ")";
        }
        return '';
    }

    /**
     * @inheritDoc
     */
    public function unconvertField(TableFieldDto $field, string $value): string
    {
        if (preg_match("~binary~", $field->type)) {
            $value = "UNHEX($value)";
        }
        if ($field->type == "bit") {
            $value = "CONV($value, 2, 10) + 0";
        }
        if (preg_match("~geometry|point|linestring|polygon~", $field->type)) {
            $value = ($this->driver->minVersion(8) ? "ST_" : "") .
                "GeomFromText($value, SRID({$field->name}))";
        }
        return $value;
    }
}
