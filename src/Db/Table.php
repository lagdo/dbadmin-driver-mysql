<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;

use Lagdo\DbAdmin\Driver\Db\Table as AbstractTable;

class Table extends AbstractTable
{
    use Traits\TableFieldTrait;
    use Traits\TableIndexTrait;
    use Traits\TableTrait;

    /**
     * @param bool $fast
     * @param string $table
     *
     * @return array
     */
    private function queryStatus(bool $fast, string $table = '')
    {
        $query = ($fast && $this->driver->minVersion(5)) ?
            "SELECT TABLE_NAME AS Name, ENGINE AS Engine, TABLE_COMMENT AS Comment " .
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() " .
            ($table != "" ? "AND TABLE_NAME = " . $this->driver->quote($table) : "ORDER BY Name") :
            "SHOW TABLE STATUS" . ($table != "" ? " LIKE " . $this->driver->quote(addcslashes($table, "%_\\")) : "");
        return $this->driver->rows($query);
    }

    /**
     * @param array $row
     *
     * @return TableEntity
     */
    private function makeStatus(array $row)
    {
        $status = new TableEntity($row['Name']);
        $status->engine = $row['Engine'];
        if ($row["Engine"] == "InnoDB") {
            // ignore internal comment, unnecessary since MySQL 5.1.21
            $status->comment = preg_replace('~(?:(.+); )?InnoDB free: .*~', '\1', $row["Comment"]);
        }
        // if (!isset($row["Engine"])) {
        //     $row["Comment"] = "";
        // }

        return $status;
    }

    /**
     * @inheritDoc
     */
    public function tableStatus(string $table, bool $fast = false)
    {
        $rows = $this->queryStatus($fast, $table);
        if (!($row = reset($rows))) {
            return null;
        }
        return $this->makeStatus($row);
    }

    /**
     * @inheritDoc
     */
    public function tableStatuses(bool $fast = false)
    {
        $tables = [];
        $rows = $this->queryStatus($fast);
        foreach ($rows as $row) {
            $tables[$row["Name"]] = $this->makeStatus($row);
        }
        return $tables;
    }

    /**
     * @inheritDoc
     */
    public function tableNames()
    {
        $tables = [];
        $rows = $this->queryStatus(true);
        foreach ($rows as $row) {
            $tables[] = $row["Name"];
        }
        return $tables;
    }

    /**
     * @inheritDoc
     */
    public function isView(TableEntity $tableStatus)
    {
        return $tableStatus->engine === null;
    }

    /**
     * @inheritDoc
     */
    public function trigger(string $name, string $table = '')
    {
        if ($name == "") {
            return null;
        }
        $rows = $this->driver->rows("SHOW TRIGGERS WHERE `Trigger` = " . $this->driver->quote($name));
        if (!($row = reset($rows))) {
            return null;
        }
        return new TriggerEntity($row["Timing"], $row["Event"], '', '', $row["Trigger"]);
    }

    /**
     * @inheritDoc
     */
    public function triggers(string $table)
    {
        $triggers = [];
        foreach ($this->driver->rows("SHOW TRIGGERS LIKE " . $this->driver->quote(addcslashes($table, "%_\\"))) as $row) {
            $triggers[$row["Trigger"]] = new TriggerEntity($row["Timing"], $row["Event"], '', '', $row["Trigger"]);
        }
        return $triggers;
    }

    /**
     * @inheritDoc
     */
    public function triggerOptions()
    {
        return [
            "Timing" => ["BEFORE", "AFTER"],
            "Event" => ["INSERT", "UPDATE", "DELETE"],
            "Type" => ["FOR EACH ROW"],
        ];
    }

    /**
     * @inheritDoc
     */
    public function tableHelp(string $name)
    {
        $maria = preg_match('~MariaDB~', $this->driver->serverInfo());
        if ($this->driver->isInformationSchema($this->driver->database())) {
            return strtolower(($maria ? "information-schema-$name-table/" : str_replace("_", "-", $name) . "-table.html"));
        }
        if ($this->driver->database() == "mysql") {
            return ($maria ? "mysql$name-table/" : "system-database.html"); //! more precise link
        }
    }
}
