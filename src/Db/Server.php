<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;

use Lagdo\DbAdmin\Driver\Db\Server as AbstractServer;

class Server extends AbstractServer
{
    /**
     * @inheritDoc
     */
    public function databases(bool $flush)
    {
        // !!! Caching and slow query handling are temporarily disabled !!!
        $query = $this->driver->minVersion(5) ?
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME" :
            "SHOW DATABASES";
        return $this->driver->values($query);

        // SHOW DATABASES can take a very long time so it is cached
        // $databases = get_session("dbs");
        // if ($databases === null) {
        //     $query = ($this->driver->minVersion(5)
        //         ? "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME"
        //         : "SHOW DATABASES"
        //     ); // SHOW DATABASES can be disabled by skip_show_database
        //     $databases = ($flush ? slow_query($query) : $this->driver->values($query));
        //     restart_session();
        //     set_session("dbs", $databases);
        //     stop_session();
        // }
        // return $databases;
    }

    /**
     * @inheritDoc
     */
    public function databaseSize(string $database)
    {
        $statement = $this->connection->query("SELECT SUM(data_length + index_length) " .
            "FROM information_schema.tables where table_schema=" . $this->driver->quote($database));
        if (is_object($statement) && ($row = $statement->fetchRow())) {
            return intval($row[0]);
        }
        return 0;
    }

    /**
     * @inheritDoc
     */
    public function databaseCollation(string $database, array $collations)
    {
        $collation = null;
        $create = $this->connection->result("SHOW CREATE DATABASE " . $this->driver->escapeId($database), 1);
        if (preg_match('~ COLLATE ([^ ]+)~', $create, $match)) {
            $collation = $match[1];
        } elseif (preg_match('~ CHARACTER SET ([^ ]+)~', $create, $match)) {
            // default collation
            $collation = $collations[$match[1]][-1];
        }
        return $collation;
    }

    /**
     * @inheritDoc
     */
    public function engines()
    {
        $engines = [];
        foreach ($this->driver->rows("SHOW ENGINES") as $row) {
            if (preg_match("~YES|DEFAULT~", $row["Support"])) {
                $engines[] = $row["Engine"];
            }
        }
        return $engines;
    }

    /**
     * @inheritDoc
     */
    public function tables()
    {
        return $this->driver->keyValues($this->driver->minVersion(5) ?
            "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME" :
            "SHOW TABLES");
    }

    /**
     * @inheritDoc
     */
    public function countTables(array $databases)
    {
        $counts = [];
        foreach ($databases as $database) {
            $counts[$database] = count($this->driver->values("SHOW TABLES IN " . $this->driver->escapeId($database)));
        }
        return $counts;
    }

    /**
     * @inheritDoc
     */
    public function dropViews(array $views)
    {
        return $this->driver->queries("DROP VIEW " . implode(", ", array_map(function ($view) {
            return $this->driver->table($view);
        }, $views)));
    }

    /**
     * @inheritDoc
     */
    public function dropTables(array $tables)
    {
        return $this->driver->queries("DROP TABLE " . implode(", ", array_map(function ($table) {
            return $this->driver->table($table);
        }, $tables)));
    }

    /**
     * @inheritDoc
     */
    public function truncateTables(array $tables)
    {
        return $this->driver->applyQueries("TRUNCATE TABLE", $tables);
    }

    /**
     * @inheritDoc
     */
    public function moveTables(array $tables, array $views, string $target)
    {
        $rename = [];
        foreach ($tables as $table) {
            $rename[] = $this->driver->table($table) . " TO " . $this->driver->escapeId($target) . "." . $this->driver->table($table);
        }
        if (!$rename || $this->driver->queries("RENAME TABLE " . implode(", ", $rename))) {
            $definitions = [];
            foreach ($views as $table) {
                $definitions[$this->driver->table($table)] = $this->driver->view($table);
            }
            $this->connection->open($target);
            $database = $this->driver->escapeId($this->driver->database());
            foreach ($definitions as $name => $view) {
                if (!$this->driver->queries("CREATE VIEW $name AS " . str_replace(" $database.", " ", $view["select"])) || !$this->driver->queries("DROP VIEW $database.$name")) {
                    return false;
                }
            }
            return true;
        }
        //! move triggers
        return false;
    }

    /**
     * @inheritDoc
     */
    public function copyTables(array $tables, array $views, string $target)
    {
        $this->driver->queries("SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO'");
        $overwrite = $this->util->input()->getOverwrite();
        foreach ($tables as $table) {
            $name = ($target == $this->driver->database() ? $this->driver->table("copy_$table") : $this->driver->escapeId($target) . "." . $this->driver->table($table));
            if (($overwrite && !$this->driver->queries("\nDROP TABLE IF EXISTS $name"))
                || !$this->driver->queries("CREATE TABLE $name LIKE " . $this->driver->table($table))
                || !$this->driver->queries("INSERT INTO $name SELECT * FROM " . $this->driver->table($table))
            ) {
                return false;
            }
            foreach ($this->driver->rows("SHOW TRIGGERS LIKE " . $this->driver->quote(addcslashes($table, "%_\\"))) as $row) {
                $trigger = $row["Trigger"];
                if (!$this->driver->queries("CREATE TRIGGER " . ($target == $this->driver->database() ? $this->driver->escapeId("copy_$trigger") : $this->driver->escapeId($target) . "." . $this->driver->escapeId($trigger)) . " $row[Timing] $row[Event] ON $name FOR EACH ROW\n$row[Statement];")) {
                    return false;
                }
            }
        }
        foreach ($views as $table) {
            $name = ($target == $this->driver->database() ? $this->driver->table("copy_$table") : $this->driver->escapeId($target) . "." . $this->driver->table($table));
            $view = $this->driver->view($table);
            if (($overwrite && !$this->driver->queries("DROP VIEW IF EXISTS $name"))
                || !$this->driver->queries("CREATE VIEW $name AS $view[select]")) { //! USE to avoid db.table
                return false;
            }
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function collations()
    {
        $collations = [];
        foreach ($this->driver->rows("SHOW COLLATION") as $row) {
            if ($row["Default"]) {
                $collations[$row["Charset"]][-1] = $row["Collation"];
            } else {
                $collations[$row["Charset"]][] = $row["Collation"];
            }
        }
        ksort($collations);
        foreach ($collations as $key => $val) {
            asort($collations[$key]);
        }
        return $collations;
    }

    /**
     * @inheritDoc
     */
    public function isInformationSchema(string $database)
    {
        return ($this->driver->minVersion(5) && $database == "information_schema")
            || ($this->driver->minVersion(5.5) && $database == "performance_schema");
    }

    /**
     * @inheritDoc
     */
    public function createDatabase(string $database, string $collation)
    {
        return $this->driver->queries("CREATE DATABASE " . $this->driver->escapeId($database) .
            ($collation ? " COLLATE " . $this->driver->quote($collation) : ""));
    }

    /**
     * @inheritDoc
     */
    public function dropDatabases(array $databases)
    {
        return $this->driver->applyQueries("DROP DATABASE", $databases, function ($database) {
            return $this->driver->escapeId($database);
        });
    }

    /**
     * @inheritDoc
     */
    public function renameDatabase(string $name, string $collation)
    {
        $renamed = false;
        if ($this->createDatabase($name, $collation)) {
            $tables = [];
            $views = [];
            foreach ($this->tables() as $table => $type) {
                if ($type == 'VIEW') {
                    $views[] = $table;
                } else {
                    $tables[] = $table;
                }
            }
            $renamed = (!$tables && !$views) || $this->driver->moveTables($tables, $views, $name);
            $this->dropDatabases($renamed ? [$this->driver->database()] : []);
        }
        return $renamed;
    }

    /**
     * @inheritDoc
     */
    public function routine(string $name, string $type)
    {
        $aliases = ["bool", "boolean", "integer", "double precision", "real", "dec", "numeric", "fixed", "national char", "national varchar"];
        $space = "(?:\\s|/\\*[\s\S]*?\\*/|(?:#|-- )[^\n]*\n?|--\r?\n)";
        $type_pattern = "((" . implode("|", array_merge(array_keys($this->driver->config->types), $aliases)) . ")\\b(?:\\s*\\(((?:[^'\")]|$this->driver->enumLength)++)\\))?\\s*(zerofill\\s*)?(unsigned(?:\\s+zerofill)?)?)(?:\\s*(?:CHARSET|CHARACTER\\s+SET)\\s*['\"]?([^'\"\\s,]+)['\"]?)?";
        $pattern = "$space*(" . ($type == "FUNCTION" ? "" : $this->driver->inout) . ")?\\s*(?:`((?:[^`]|``)*)`\\s*|\\b(\\S+)\\s+)$type_pattern";
        $create = $this->connection->result("SHOW CREATE $type " . $this->driver->escapeId($name), 2);
        preg_match("~\\(((?:$pattern\\s*,?)*)\\)\\s*" . ($type == "FUNCTION" ? "RETURNS\\s+$type_pattern\\s+" : "") . "(.*)~is", $create, $match);
        $fields = [];
        preg_match_all("~$pattern\\s*,?~is", $match[1], $matches, PREG_SET_ORDER);
        foreach ($matches as $param) {
            $fields[] = [
                "field" => str_replace("``", "`", $param[2]) . $param[3],
                "type" => strtolower($param[5]),
                "length" => preg_replace_callback("~{$this->driver->enumLength}~s", 'normalize_enum', $param[6]),
                "unsigned" => strtolower(preg_replace('~\s+~', ' ', trim("$param[8] $param[7]"))),
                "null" => 1,
                "full_type" => $param[4],
                "inout" => strtoupper($param[1]),
                "collation" => strtolower($param[9]),
            ];
        }
        if ($type != "FUNCTION") {
            return ["fields" => $fields, "definition" => $match[11]];
        }
        return [
            "fields" => $fields,
            "returns" => ["type" => $match[12], "length" => $match[13], "unsigned" => $match[15], "collation" => $match[16]],
            "definition" => $match[17],
            "language" => "SQL", // available in information_schema.ROUTINES.PARAMETER_STYLE
        ];
    }

    /**
     * @inheritDoc
     */
    public function routines()
    {
        $rows = $this->driver->rows("SELECT ROUTINE_NAME, ROUTINE_TYPE, DTD_IDENTIFIER " .
            "FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = " . $this->driver->quote($this->driver->database()));
        return array_map(function($row) {
            return new RoutineEntity($row['ROUTINE_NAME'], $row['ROUTINE_NAME'], $row['ROUTINE_TYPE'], $row['DTD_IDENTIFIER']);
        }, $rows);
    }

    /**
     * @inheritDoc
     */
    public function routineLanguages()
    {
        return []; // "SQL" not required
    }

    /**
     * @inheritDoc
     */
    public function routineId(string $name, array $row)
    {
        return $this->driver->escapeId($name);
    }

    /**
     * @inheritDoc
     */
    public function variables()
    {
        return $this->driver->keyValues("SHOW VARIABLES");
    }

    /**
     * @inheritDoc
     */
    public function processes()
    {
        return $this->driver->rows("SHOW FULL PROCESSLIST");
    }

    /**
     * @inheritDoc
     */
    public function statusVariables()
    {
        return $this->driver->keyValues("SHOW STATUS");
    }

    /**
     * @inheritDoc
     */
    // public function killProcess($val)
    // {
    //     return $this->driver->queries("KILL " . $this->util->number($val));
    // }

    /**
     * @inheritDoc
     */
    // public function maxConnections()
    // {
    //     return $this->connection->result("SELECT @@max_connections");
    // }
}
