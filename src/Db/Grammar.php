<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableSelectEntity;

use Lagdo\DbAdmin\Driver\Db\Grammar as AbstractGrammar;

class Grammar extends AbstractGrammar
{
    /**
     * @inheritDoc
     */
    public function escapeId(string $idf)
    {
        return "`" . str_replace("`", "``", $idf) . "`";
    }

    /**
     * @inheritDoc
     */
    public function getAutoIncrementModifier()
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
    public function buildSelectQuery(TableSelectEntity $select)
    {
        $prefix = '';
        if (($select->page) && ($select->limit) && !empty($select->group) &&
            count($select->group) < count($select->fields)) {
            $prefix = 'SQL_CALC_FOUND_ROWS ';
        }

        return $prefix . parent::buildSelectQuery($select);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQuery(string $table, bool $autoIncrement, string $style)
    {
        $query = $this->driver->result("SHOW CREATE TABLE " . $this->escapeTableName($table), 1);
        if (!$autoIncrement) {
            $query = preg_replace('~ AUTO_INCREMENT=\d+~', '', $query); //! skip comments
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function getTruncateTableQuery(string $table)
    {
        return "TRUNCATE " . $this->escapeTableName($table);
    }

    /**
     * @inheritDoc
     */
    public function getUseDatabaseQuery(string $database)
    {
        return "USE " . $this->escapeId($database);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTriggerQuery(string $table)
    {
        $query = "";
        foreach ($this->driver->rows("SHOW TRIGGERS LIKE " .
            $this->driver->quote(addcslashes($table, "%_\\")), null) as $row) {
            $query .= "\nCREATE TRIGGER " . $this->escapeId($row["Trigger"]) .
                " $row[Timing] $row[Event] ON " . $this->escapeTableName($row["Table"]) .
                " FOR EACH ROW\n$row[Statement];;\n";
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function convertField(TableFieldEntity $field)
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
    public function unconvertField(TableFieldEntity $field, string $value)
    {
        if (preg_match("~binary~", $field->type)) {
            $value = "UNHEX($value)";
        }
        if ($field->type == "bit") {
            $value = "CONV($value, 2, 10) + 0";
        }
        if (preg_match("~geometry|point|linestring|polygon~", $field->type)) {
            $value = ($this->driver->minVersion(8) ? "ST_" : "") . "GeomFromText($value, SRID($field[field]))";
        }
        return $value;
    }

    /**
     * @inheritDoc
     */
    // public function connectionId()
    // {
    //     return "SELECT CONNECTION_ID()";
    // }

    /**
     * @inheritDoc
     */
    protected function queryRegex()
    {
        return '\\s*|[\'"`#]|/\*|-- |$';
    }
}
