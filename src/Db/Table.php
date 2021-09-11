<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;
use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;

use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;

use Lagdo\DbAdmin\Driver\Db\Table as AbstractTable;

class Table extends AbstractTable
{
    /**
     * @inheritDoc
     */
    public function tableStatus(string $table = "", bool $fast = false)
    {
        $tables = [];
        foreach ($this->driver->rows($fast && $this->driver->minVersion(5) ?
            "SELECT TABLE_NAME AS Name, ENGINE AS Engine, TABLE_COMMENT AS Comment FROM information_schema.TABLES " .
            "WHERE TABLE_SCHEMA = DATABASE() " . ($table != "" ? "AND TABLE_NAME = " . $this->driver->quote($table) : "ORDER BY Name") :
            "SHOW TABLE STATUS" . ($table != "" ? " LIKE " . $this->driver->quote(addcslashes($table, "%_\\")) : "")
        ) as $row) {
            $status = new TableEntity($row['Name']);
            $status->engine = $row['Engine'];
            if ($row["Engine"] == "InnoDB") {
                // ignore internal comment, unnecessary since MySQL 5.1.21
                $status->comment = preg_replace('~(?:(.+); )?InnoDB free: .*~', '\1', $row["Comment"]);
            }
            // if (!isset($row["Engine"])) {
            //     $row["Comment"] = "";
            // }

            if ($table != "") {
                return $status;
            }
            $tables[$row["Name"]] = $status;
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
            $field->length = $match2;
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
                // https://mariadb.com/kb/en/library/show-columns/, https://github.com/vrana/adminer/pull/359#pullrequestreview-276677186
            $field->generated = preg_match('~^(VIRTUAL|PERSISTENT|STORED)~', $row["Extra"]);

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
        $create_table = $this->connection->result("SHOW CREATE TABLE " . $this->driver->table($table), 1);
        if ($create_table) {
            preg_match_all("~CONSTRAINT ($pattern) FOREIGN KEY ?\\(((?:$pattern,? ?)+)\\) REFERENCES " .
                "($pattern)(?:\\.($pattern))? \\(((?:$pattern,? ?)+)\\)(?: ON DELETE ({$this->driver->onActions}))" .
                "?(?: ON UPDATE ({$this->driver->onActions}))?~", $create_table, $matches, PREG_SET_ORDER);

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

                $foreignKey->db = $this->driver->unescapeId($match4 != "" ? $match3 : $match4);
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
    public function alterTable(string $table, string $name, array $fields, array $foreign,
        string $comment, string $engine, string $collation, int $autoIncrement, string $partitioning)
    {
        $alter = [];
        foreach ($fields as $field) {
            $alter[] = (
                $field[1]
                ? ($table != "" ? ($field[0] != "" ? "CHANGE " . $this->driver->escapeId($field[0]) : "ADD") : " ") . " " . implode($field[1]) . ($table != "" ? $field[2] : "")
                : "DROP " . $this->driver->escapeId($field[0])
            );
        }
        $alter = array_merge($alter, $foreign);
        $status = ($comment !== null ? " COMMENT=" . $this->driver->quote($comment) : "")
            . ($engine ? " ENGINE=" . $this->driver->quote($engine) : "")
            . ($collation ? " COLLATE " . $this->driver->quote($collation) : "")
            . ($autoIncrement != "" ? " AUTO_INCREMENT=$autoIncrement" : "")
        ;
        if ($table == "") {
            return $this->driver->driver->queries("CREATE TABLE " . $this->driver->table($name) . " (\n" . implode(",\n", $alter) . "\n)$status$partitioning");
        }
        if ($table != $name) {
            $alter[] = "RENAME TO " . $this->driver->table($name);
        }
        if ($status) {
            $alter[] = ltrim($status);
        }
        return ($alter || $partitioning ? $this->driver->queries("ALTER TABLE " . $this->driver->table($table) . "\n" . implode(",\n", $alter) . $partitioning) : true);
    }

    /**
     * @inheritDoc
     */
    public function alterIndexes(string $table, array $alter)
    {
        foreach ($alter as $key => $val) {
            $alter[$key] = (
                $val[2] == "DROP"
                ? "\nDROP INDEX " . $this->driver->escapeId($val[1])
                : "\nADD $val[0] " . ($val[0] == "PRIMARY" ? "KEY " : "") . ($val[1] != "" ? $this->driver->escapeId($val[1]) . " " : "") . "(" . implode(", ", $val[2]) . ")"
            );
        }
        return $this->driver->queries("ALTER TABLE " . $this->driver->table($table) . implode(",", $alter));
    }

    /**
     * @inheritDoc
     */
    public function trigger(string $trigger)
    {
        if ($trigger == "") {
            return [];
        }
        $rows = $this->driver->rows("SHOW TRIGGERS WHERE `Trigger` = " . $this->driver->quote($trigger));
        return reset($rows);
    }

    /**
     * @inheritDoc
     */
    public function triggers(string $table)
    {
        $triggers = [];
        foreach ($this->driver->rows("SHOW TRIGGERS LIKE " . $this->driver->quote(addcslashes($table, "%_\\"))) as $row) {
            $triggers[$row["Trigger"]] = new Trigger($row["Timing"], $row["Event"]);
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
        if ($this->driver->isInformationSchema($this->driver->selectedDatabase())) {
            return strtolower(($maria ? "information-schema-$name-table/" : str_replace("_", "-", $name) . "-table.html"));
        }
        if ($this->driver->selectedDatabase() == "mysql") {
            return ($maria ? "mysql$name-table/" : "system-database.html"); //! more precise link
        }
    }
}
