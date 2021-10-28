<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;

use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;

use Lagdo\DbAdmin\Driver\Db\Table as AbstractTable;

class Table extends AbstractTable
{
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
    public function fields(string $table)
    {
        $fields = [];
        foreach ($this->driver->rows("SHOW FULL COLUMNS FROM " . $this->driver->table($table)) as $row) {
            preg_match('~^([^( ]+)(?:\((.+)\))?( unsigned)?( zerofill)?$~', $row["Type"], $match);
            $matchCount = count($match);
            $match1 = $matchCount > 1 ? $match[1] : '';
            $match2 = $matchCount > 2 ? $match[2] : '';
            $match3 = $matchCount > 3 ? $match[3] : '';
            $match4 = $matchCount > 4 ? $match[4] : '';

            $field = new TableFieldEntity();

            $field->name = $row["Field"];
            $field->fullType = $row["Type"];
            $field->type = $match1;
            $field->length = intval($match2);
            $field->unsigned = ltrim($match3 . $match4);
            $field->default = ($row["Default"] != "" || preg_match("~char|set~", $match1) ?
                (preg_match('~text~', $match1) ? stripslashes(preg_replace("~^'(.*)'\$~", '\1',
                $row["Default"])) : $row["Default"]) : null);
            $field->null = ($row["Null"] == "YES");
            $field->autoIncrement = ($row["Extra"] == "auto_increment");
            $field->onUpdate = (preg_match('~^on update (.+)~i', $row["Extra"], $match) ? $match1 : ""); //! available since MySQL 5.1.23
            $field->collation = $row["Collation"];
            $field->privileges = array_flip(preg_split('~, *~', $row["Privileges"]));
            $field->comment = $row["Comment"];
            $field->primary = ($row["Key"] == "PRI");
            // https://mariadb.com/kb/en/library/show-columns/
            // https://github.com/vrana/adminer/pull/359#pullrequestreview-276677186
            $field->generated = preg_match('~^(VIRTUAL|PERSISTENT|STORED)~', $row["Extra"]) > 0;

            $fields[$field->name] = $field;
        }
        return $fields;
    }

    /**
     * @inheritDoc
     */
    public function supportForeignKeys(TableEntity $tableStatus)
    {
        return preg_match('~InnoDB|IBMDB2I~i', $tableStatus->engine)
            || (preg_match('~NDB~i', $tableStatus->engine) && $this->driver->minVersion(5.6));
    }

    /**
     * @inheritDoc
     */
    public function referencableTables(string $table)
    {
        $fields = []; // table_name => [field]
        foreach ($this->tableStatuses(true) as $tableName => $tableStatus) {
            if ($tableName === $table || !$this->supportForeignKeys($tableStatus)) {
                continue;
            }
            foreach ($this->fields($tableName) as $field) {
                if ($field->primary) {
                    if (!isset($fields[$tableName])) {
                        $fields[$tableName] = $field;
                    } else {
                        // No multi column primary key
                        $fields[$tableName] = null;
                    }
                }
            }
        }
        return array_filter($fields, function($field) {
            return $field !== null;
        });
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
    public function indexes(string $table, ConnectionInterface $connection = null)
    {
        $indexes = [];
        foreach ($this->driver->rows("SHOW INDEX FROM " . $this->driver->table($table), $connection) as $row) {
            $index = new IndexEntity();

            $name = $row["Key_name"];
            $index->type = ($name == "PRIMARY" ? "PRIMARY" :
                ($row["Index_type"] == "FULLTEXT" ? "FULLTEXT" : ($row["Non_unique"] ?
                ($row["Index_type"] == "SPATIAL" ? "SPATIAL" : "INDEX") : "UNIQUE")));
            $index->columns[] = $row["Column_name"];
            $index->lengths[] = ($row["Index_type"] == "SPATIAL" ? null : $row["Sub_part"]);
            $index->descs[] = null;

            $indexes[$name] = $index;
        }
        return $indexes;
    }

    /**
     * @inheritDoc
     */
    public function foreignKeys(string $table)
    {
        static $pattern = '(?:`(?:[^`]|``)+`|"(?:[^"]|"")+")';
        $foreignKeys = [];
        $onActions = $this->driver->actions();
        $create_table = $this->connection->result("SHOW CREATE TABLE " . $this->driver->table($table), 1);
        if ($create_table) {
            preg_match_all("~CONSTRAINT ($pattern) FOREIGN KEY ?\\(((?:$pattern,? ?)+)\\) REFERENCES " .
                "($pattern)(?:\\.($pattern))? \\(((?:$pattern,? ?)+)\\)(?: ON DELETE ($onActions))" .
                "?(?: ON UPDATE ($onActions))?~", $create_table, $matches, PREG_SET_ORDER);

            foreach ($matches as $match) {
                $matchCount = count($match);
                $match1 = $matchCount > 1 ? $match[1] : '';
                $match2 = $matchCount > 2 ? $match[2] : '';
                $match3 = $matchCount > 3 ? $match[3] : '';
                $match4 = $matchCount > 4 ? $match[4] : '';
                $match5 = $matchCount > 5 ? $match[5] : '';

                preg_match_all("~$pattern~", $match2, $source);
                preg_match_all("~$pattern~", $match5, $target);

                $foreignKey = new ForeignKeyEntity();

                $foreignKey->database = $this->driver->unescapeId($match4 != "" ? $match3 : $match4);
                $foreignKey->table = $this->driver->unescapeId($match4 != "" ? $match4 : $match3);
                $foreignKey->source = array_map(function ($idf) {
                    return $this->driver->unescapeId($idf);
                }, $source[0]);
                $foreignKey->target = array_map(function ($idf) {
                    return $this->driver->unescapeId($idf);
                }, $target[0]);
                $foreignKey->onDelete = $matchCount > 6 ? $match[6] : "RESTRICT";
                $foreignKey->onUpdate = $matchCount > 7 ? $match[7] : "RESTRICT";

                $foreignKeys[$this->driver->unescapeId($match1)] = $foreignKey;
            }
        }
        return $foreignKeys;
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
        $maria = preg_match('~MariaDB~', $this->connection->serverInfo());
        if ($this->driver->isInformationSchema($this->driver->database())) {
            return strtolower(($maria ? "information-schema-$name-table/" : str_replace("_", "-", $name) . "-table.html"));
        }
        if ($this->driver->database() == "mysql") {
            return ($maria ? "mysql$name-table/" : "system-database.html"); //! more precise link
        }
    }
}
