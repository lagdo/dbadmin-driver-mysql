<?php

namespace Lagdo\DbAdmin\Driver\MySql;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;
use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;

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
    public function limit(string $query, string $where, int $limit, int $offset = 0, string $separator = " ")
    {
        return " $query$where" . ($limit !== null ? $separator . "LIMIT $limit" . ($offset ? " OFFSET $offset" : "") : "");
    }

    /**
     * @inheritDoc
     */
    public function autoIncrement()
    {
        $autoIncrementIndex = " PRIMARY KEY";
        // don't overwrite primary key by auto increment
        $query = $this->util->input();
        $table = $query->getTable();
        $fields = $query->getFields();
        $autoIncrementField = $query->getAutoIncrementField();
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
    public function createTableSql(string $table, bool $autoIncrement, string $style)
    {
        $query = $this->connection->result("SHOW CREATE TABLE " . $this->table($table), 1);
        if (!$autoIncrement) {
            $query = preg_replace('~ AUTO_INCREMENT=\d+~', '', $query); //! skip comments
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function truncateTableSql(string $table)
    {
        return "TRUNCATE " . $this->table($table);
    }

    /**
     * @inheritDoc
     */
    public function useDatabaseSql(string $database)
    {
        return "USE " . $this->escapeId($database);
    }

    /**
     * @inheritDoc
     */
    public function createTriggerSql(string $table)
    {
        $query = "";
        foreach ($this->driver->rows("SHOW TRIGGERS LIKE " .
            $this->quote(addcslashes($table, "%_\\")), null, "-- ") as $row) {
            $query .= "\nCREATE TRIGGER " . $this->escapeId($row["Trigger"]) .
                " $row[Timing] $row[Event] ON " . $this->table($row["Table"]) .
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
    public function connectionId()
    {
        return "SELECT CONNECTION_ID()";
    }
}
