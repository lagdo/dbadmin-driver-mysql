<?php

namespace Lagdo\DbAdmin\Driver\MySql;

use Lagdo\DbAdmin\Driver\Db\Server as AbstractServer;
use Lagdo\DbAdmin\Driver\Entity\TableField;
use Lagdo\DbAdmin\Driver\Entity\Table;
use Lagdo\DbAdmin\Driver\Entity\Index;
use Lagdo\DbAdmin\Driver\Entity\ForeignKey;
use Lagdo\DbAdmin\Driver\Entity\Trigger;
use Lagdo\DbAdmin\Driver\Entity\Routine;

class Server extends AbstractServer
{
    /**
     * @inheritDoc
     */
    public function name()
    {
        return "MySQL";
    }

    /**
     * @inheritDoc
     */
    public function connect()
    {
        $connection = null;
        if (extension_loaded("mysqli")) {
            $connection = new MySqli\Connection($this->db, $this->util, $this, 'MySQLi');
        }
        elseif (extension_loaded("pdo_mysql")) {
            $connection = new Pdo\Connection($this->db, $this->util, $this, 'PDO_MySQL');
        }
        else {
            throw new AuthException($this->util->lang('No package installed to connect to a MySQL server.'));
        }

        if ($this->connection === null) {
            $this->connection = $connection;
            $this->driver = new Driver($this->db, $this->util, $this, $connection);
        }

        if (!$connection->open($this->db->options('server'), $this->db->options())) {
            $error = $this->util->error();
            // windows-1250 - most common Windows encoding
            if (function_exists('iconv') && !$this->util->isUtf8($error) &&
                strlen($s = iconv("windows-1250", "utf-8", $error)) > strlen($error)) {
                $error = $s;
            }
            throw new AuthException($error);
        }

        // Available in MySQLi since PHP 5.0.5
        $connection->setCharset($this->charset());
        $connection->query("SET sql_quote_show_create = 1, autocommit = 1");
        if ($this->minVersion('5.7.8', 10.2, $connection)) {
            $this->config->structuredTypes[$this->util->lang('Strings')][] = "json";
            $this->config->types["json"] = 4294967295;
        }

        return $connection;
    }

    /**
     * @inheritDoc
     */
    public function escapeId($idf)
    {
        return "`" . str_replace("`", "``", $idf) . "`";
    }

    /**
     * Get cached list of databases
     * @param bool
     * @return array
     */
    public function databases($flush)
    {
        // !!! Caching and slow query handling are temporarily disabled !!!
        $query = $this->minVersion(5) ?
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME" :
            "SHOW DATABASES";
        return $this->db->values($query);

        // SHOW DATABASES can take a very long time so it is cached
        // $return = get_session("dbs");
        // if ($return === null) {
        //     $query = ($this->minVersion(5)
        //         ? "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME"
        //         : "SHOW DATABASES"
        //     ); // SHOW DATABASES can be disabled by skip_show_database
        //     $return = ($flush ? slow_query($query) : $this->db->values($query));
        //     restart_session();
        //     set_session("dbs", $return);
        //     stop_session();
        // }
        // return $return;
    }

    /**
     * @inheritDoc
     */
    public function databaseSize($database)
    {
        $statement = $this->connection->query("SELECT SUM(data_length + index_length) " .
            "FROM information_schema.tables where table_schema=" . $this->quote($database));
        if (is_object($statement) && ($row = $statement->fetchRow())) {
            return intval($row[0]);
        }
        return 0;
    }

    /**
     * Formulate SQL query with limit
     * @param string everything after SELECT
     * @param string including WHERE
     * @param int
     * @param int
     * @param string
     * @return string
     */
    public function limit($query, $where, $limit, $offset = 0, $separator = " ")
    {
        return " $query$where" . ($limit !== null ? $separator . "LIMIT $limit" . ($offset ? " OFFSET $offset" : "") : "");
    }

    /**
     * Get database collation
     * @param string
     * @param array result of collations()
     * @return string
     */
    public function databaseCollation($db, $collations)
    {
        $return = null;
        $create = $this->connection->result("SHOW CREATE DATABASE " . $this->escapeId($db), 1);
        if (preg_match('~ COLLATE ([^ ]+)~', $create, $match)) {
            $return = $match[1];
        } elseif (preg_match('~ CHARACTER SET ([^ ]+)~', $create, $match)) {
            // default collation
            $return = $collations[$match[1]][-1];
        }
        return $return;
    }

    /**
     * Get supported engines
     * @return array
     */
    public function engines()
    {
        $return = [];
        foreach ($this->db->rows("SHOW ENGINES") as $row) {
            if (preg_match("~YES|DEFAULT~", $row["Support"])) {
                $return[] = $row["Engine"];
            }
        }
        return $return;
    }

    /**
     * Get logged user
     * @return string
     */
    public function loggedUser()
    {
        return $this->connection->result("SELECT USER()");
    }

    /**
     * Get tables list
     * @return array
     */
    public function tables()
    {
        return $this->db->keyValues(
            $this->minVersion(5)
            ? "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME"
            : "SHOW TABLES"
        );
    }

    /**
     * Count tables in all databases
     * @param array
     * @return array
     */
    public function countTables($databases)
    {
        $counts = [];
        foreach ($databases as $database) {
            $counts[$database] = count($this->db->values("SHOW TABLES IN " . $this->escapeId($database)));
        }
        return $counts;
    }

    /**
     * @inheritDoc
     */
    public function tableStatus($name = "", $fast = false)
    {
        $tables = [];
        foreach ($this->db->rows($fast && $this->minVersion(5) ?
            "SELECT TABLE_NAME AS Name, ENGINE AS Engine, TABLE_COMMENT AS Comment FROM information_schema.TABLES " .
            "WHERE TABLE_SCHEMA = DATABASE() " . ($name != "" ? "AND TABLE_NAME = " . $this->quote($name) : "ORDER BY Name") :
            "SHOW TABLE STATUS" . ($name != "" ? " LIKE " . $this->quote(addcslashes($name, "%_\\")) : "")
        ) as $row) {
            $status = new Table($row['Name']);
            $status->engine = $row['Engine'];
            if ($row["Engine"] == "InnoDB") {
                // ignore internal comment, unnecessary since MySQL 5.1.21
                $status->comment = preg_replace('~(?:(.+); )?InnoDB free: .*~', '\1', $row["Comment"]);
            }
            // if (!isset($row["Engine"])) {
            //     $row["Comment"] = "";
            // }

            if ($name != "") {
                return $status;
            }
            $tables[$row["Name"]] = $status;
        }
        return $tables;
    }

    /**
     * Find out whether the identifier is view
     * @param array
     * @return bool
     */
    public function isView($tableStatus)
    {
        return $tableStatus->engine === null;
    }

    /**
     * Check if table supports foreign keys
     * @param array result of table_status
     * @return bool
     */
    public function supportForeignKeys($tableStatus)
    {
        return preg_match('~InnoDB|IBMDB2I~i', $tableStatus->engine)
            || (preg_match('~NDB~i', $tableStatus->engine) && $this->minVersion(5.6));
    }

    /**
     * Get information about fields
     * @param string
     * @return array
     */
    public function fields($table)
    {
        $fields = [];
        foreach ($this->db->rows("SHOW FULL COLUMNS FROM " . $this->table($table)) as $row) {
            preg_match('~^([^( ]+)(?:\((.+)\))?( unsigned)?( zerofill)?$~', $row["Type"], $match);
            $matchCount = count($match);
            $match1 = $matchCount > 1 ? $match[1] : '';
            $match2 = $matchCount > 2 ? $match[2] : '';
            $match3 = $matchCount > 3 ? $match[3] : '';
            $match4 = $matchCount > 4 ? $match[4] : '';

            $field = new TableField();

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
     * Get table indexes
     * @param string
     * @param string ConnectionInterface to use
     * @return array
     */
    public function indexes($table, $connection = null)
    {
        $indexes = [];
        foreach ($this->db->rows("SHOW INDEX FROM " . $this->table($table), $connection) as $row) {
            $index = new Index();

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
     * Get foreign keys in table
     * @param string
     * @return array
     */
    public function foreignKeys($table)
    {
        static $pattern = '(?:`(?:[^`]|``)+`|"(?:[^"]|"")+")';
        $foreignKeys = [];
        $create_table = $this->connection->result("SHOW CREATE TABLE " . $this->table($table), 1);
        if ($create_table) {
            preg_match_all("~CONSTRAINT ($pattern) FOREIGN KEY ?\\(((?:$pattern,? ?)+)\\) REFERENCES " .
                "($pattern)(?:\\.($pattern))? \\(((?:$pattern,? ?)+)\\)(?: ON DELETE ($this->onActions))" .
                "?(?: ON UPDATE ($this->onActions))?~", $create_table, $matches, PREG_SET_ORDER);

            foreach ($matches as $match) {
                $matchCount = count($match);
                $match1 = $matchCount > 1 ? $match[1] : '';
                $match2 = $matchCount > 2 ? $match[2] : '';
                $match3 = $matchCount > 3 ? $match[3] : '';
                $match4 = $matchCount > 4 ? $match[4] : '';
                $match5 = $matchCount > 5 ? $match[5] : '';

                preg_match_all("~$pattern~", $match2, $source);
                preg_match_all("~$pattern~", $match5, $target);

                $foreignKey = new ForeignKey();

                $foreignKey->db = $this->unescapeId($match4 != "" ? $match3 : $match4);
                $foreignKey->table = $this->unescapeId($match4 != "" ? $match4 : $match3);
                $foreignKey->source = array_map(function ($idf) {
                    return $this->unescapeId($idf);
                }, $source[0]);
                $foreignKey->target = array_map(function ($idf) {
                    return $this->unescapeId($idf);
                }, $target[0]);
                $foreignKey->onDelete = $matchCount > 6 ? $match[6] : "RESTRICT";
                $foreignKey->onUpdate = $matchCount > 7 ? $match[7] : "RESTRICT";

                $foreignKeys[$this->unescapeId($match1)] = $foreignKey;
            }
        }
        return $foreignKeys;
    }

    /**
     * Get view SELECT
     * @param string
     * @return array
     */
    public function view($name)
    {
        return [
            "select" => preg_replace('~^(?:[^`]|`[^`]*`)*\s+AS\s+~isU', '',
                $this->connection->result("SHOW CREATE VIEW " . $this->table($name), 1)),
        ];
    }

    /**
     * Get sorted grouped list of collations
     * @return array
     */
    public function collations()
    {
        $return = [];
        foreach ($this->db->rows("SHOW COLLATION") as $row) {
            if ($row["Default"]) {
                $return[$row["Charset"]][-1] = $row["Collation"];
            } else {
                $return[$row["Charset"]][] = $row["Collation"];
            }
        }
        ksort($return);
        foreach ($return as $key => $val) {
            asort($return[$key]);
        }
        return $return;
    }

    /**
     * Find out if database is information_schema
     * @param string
     * @return bool
     */
    public function isInformationSchema($db)
    {
        return ($this->minVersion(5) && $db == "information_schema")
            || ($this->minVersion(5.5) && $db == "performance_schema");
    }

    /**
     * Get escaped error message
     *
     * @return string
     */
    public function error()
    {
        return $this->util->html(preg_replace('~^You have an error.*syntax to use~U', "Syntax error", $this->db->error()));
    }

    /**
     * Create database
     * @param string
     * @param string
     * @return string
     */
    public function createDatabase($db, $collation)
    {
        return $this->db->queries("CREATE DATABASE " . $this->escapeId($db) . ($collation ? " COLLATE " . $this->quote($collation) : ""));
    }

    /**
     * Drop databases
     * @param array
     * @return bool
     */
    public function dropDatabases($databases)
    {
        return $this->db->applyQueries("DROP DATABASE", $databases, function ($database) {
            return $this->escapeId($database);
        });
    }

    /**
     * Rename database from DB
     * @param string new name
     * @param string
     * @return bool
     */
    public function renameDatabase($name, $collation)
    {
        $return = false;
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
            $return = (!$tables && !$views) || $this->moveTables($tables, $views, $name);
            $this->dropDatabases($return ? [$this->selectedDatabase()] : []);
        }
        return $return;
    }

    /**
     * Generate modifier for auto increment column
     * @return string
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
            foreach ($this->indexes($table) as $index) {
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
     * Run commands to create or alter table
     * @param string $table "" to create
     * @param string $name new name
     * @param array $fields of [$orig, $process_field, $after]
     * @param array $foreign of strings
     * @param string $comment
     * @param string $engine
     * @param string $collation
     * @param string $autoIncrement number
     * @param string $partitioning
     * @return bool
     */
    public function alterTable($table, $name, $fields, $foreign, $comment, $engine, $collation, $autoIncrement, $partitioning)
    {
        $alter = [];
        foreach ($fields as $field) {
            $alter[] = (
                $field[1]
                ? ($table != "" ? ($field[0] != "" ? "CHANGE " . $this->escapeId($field[0]) : "ADD") : " ") . " " . implode($field[1]) . ($table != "" ? $field[2] : "")
                : "DROP " . $this->escapeId($field[0])
            );
        }
        $alter = array_merge($alter, $foreign);
        $status = ($comment !== null ? " COMMENT=" . $this->quote($comment) : "")
            . ($engine ? " ENGINE=" . $this->quote($engine) : "")
            . ($collation ? " COLLATE " . $this->quote($collation) : "")
            . ($autoIncrement != "" ? " AUTO_INCREMENT=$autoIncrement" : "")
        ;
        if ($table == "") {
            return $this->db->queries("CREATE TABLE " . $this->table($name) . " (\n" . implode(",\n", $alter) . "\n)$status$partitioning");
        }
        if ($table != $name) {
            $alter[] = "RENAME TO " . $this->table($name);
        }
        if ($status) {
            $alter[] = ltrim($status);
        }
        return ($alter || $partitioning ? $this->db->queries("ALTER TABLE " . $this->table($table) . "\n" . implode(",\n", $alter) . $partitioning) : true);
    }

    /**
     * Run commands to alter indexes
     * @param string escaped table name
     * @param array of ["index type", "name", ["column definition", ...]] or ["index type", "name", "DROP"]
     * @return bool
     */
    public function alterIndexes($table, $alter)
    {
        foreach ($alter as $key => $val) {
            $alter[$key] = (
                $val[2] == "DROP"
                ? "\nDROP INDEX " . $this->escapeId($val[1])
                : "\nADD $val[0] " . ($val[0] == "PRIMARY" ? "KEY " : "") . ($val[1] != "" ? $this->escapeId($val[1]) . " " : "") . "(" . implode(", ", $val[2]) . ")"
            );
        }
        return $this->db->queries("ALTER TABLE " . $this->table($table) . implode(",", $alter));
    }

    /**
     * Run commands to truncate tables
     * @param array
     * @return bool
     */
    public function truncateTables($tables)
    {
        return $this->db->applyQueries("TRUNCATE TABLE", $tables);
    }

    /**
     * Drop views
     * @param array
     * @return bool
     */
    public function dropViews($views)
    {
        return $this->db->queries("DROP VIEW " . implode(", ", array_map(function ($view) {
            return $this->table($view);
        }, $views)));
    }

    /**
     * Drop tables
     * @param array
     * @return bool
     */
    public function dropTables($tables)
    {
        return $this->db->queries("DROP TABLE " . implode(", ", array_map(function ($table) {
            return $this->table($table);
        }, $tables)));
    }

    /**
     * Move tables to other schema
     * @param array
     * @param array
     * @param string
     * @return bool
     */
    public function moveTables($tables, $views, $target)
    {
        $rename = [];
        foreach ($tables as $table) {
            $rename[] = $this->table($table) . " TO " . $this->escapeId($target) . "." . $this->table($table);
        }
        if (!$rename || $this->db->queries("RENAME TABLE " . implode(", ", $rename))) {
            $definitions = [];
            foreach ($views as $table) {
                $definitions[$this->server->table($table)] = $this->view($table);
            }
            $this->connection->selectDatabase($target);
            $db = $this->escapeId($this->selectedDatabase());
            foreach ($definitions as $name => $view) {
                if (!$this->db->queries("CREATE VIEW $name AS " . str_replace(" $db.", " ", $view["select"])) || !$this->db->queries("DROP VIEW $db.$name")) {
                    return false;
                }
            }
            return true;
        }
        //! move triggers
        return false;
    }

    /**
     * Copy tables to other schema
     * @param array
     * @param array
     * @param string
     * @return bool
     */
    public function copyTables($tables, $views, $target)
    {
        $this->db->queries("SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO'");
        $overwrite = $this->util->input()->getOverwrite();
        foreach ($tables as $table) {
            $name = ($target == $this->selectedDatabase() ? $this->table("copy_$table") : $this->escapeId($target) . "." . $this->table($table));
            if (($overwrite && !$this->db->queries("\nDROP TABLE IF EXISTS $name"))
                || !$this->db->queries("CREATE TABLE $name LIKE " . $this->table($table))
                || !$this->db->queries("INSERT INTO $name SELECT * FROM " . $this->table($table))
            ) {
                return false;
            }
            foreach ($this->db->rows("SHOW TRIGGERS LIKE " . $this->quote(addcslashes($table, "%_\\"))) as $row) {
                $trigger = $row["Trigger"];
                if (!$this->db->queries("CREATE TRIGGER " . ($target == $this->selectedDatabase() ? $this->escapeId("copy_$trigger") : $this->escapeId($target) . "." . $this->escapeId($trigger)) . " $row[Timing] $row[Event] ON $name FOR EACH ROW\n$row[Statement];")) {
                    return false;
                }
            }
        }
        foreach ($views as $table) {
            $name = ($target == $this->selectedDatabase() ? $this->table("copy_$table") : $this->escapeId($target) . "." . $this->table($table));
            $view = $this->view($table);
            if (($overwrite && !$this->db->queries("DROP VIEW IF EXISTS $name"))
                || !$this->db->queries("CREATE VIEW $name AS $view[select]")) { //! USE to avoid db.table
                return false;
            }
        }
        return true;
    }

    /**
     * Get information about trigger
     * @param string trigger name
     * @return array
     */
    public function trigger($name)
    {
        if ($name == "") {
            return [];
        }
        $rows = $this->db->rows("SHOW TRIGGERS WHERE `Trigger` = " . $this->quote($name));
        return reset($rows);
    }

    /**
     * Get defined triggers
     * @param string
     * @return array
     */
    public function triggers($table)
    {
        $triggers = [];
        foreach ($this->db->rows("SHOW TRIGGERS LIKE " . $this->quote(addcslashes($table, "%_\\"))) as $row) {
            $triggers[$row["Trigger"]] = new Trigger($row["Timing"], $row["Event"]);
        }
        return $triggers;
    }

    /**
     * Get trigger options
     * @return array ("Timing" => [], "Event" => [], "Type" => [])
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
    public function routine($name, $type)
    {
        $aliases = ["bool", "boolean", "integer", "double precision", "real", "dec", "numeric", "fixed", "national char", "national varchar"];
        $space = "(?:\\s|/\\*[\s\S]*?\\*/|(?:#|-- )[^\n]*\n?|--\r?\n)";
        $type_pattern = "((" . implode("|", array_merge(array_keys($this->config->types), $aliases)) . ")\\b(?:\\s*\\(((?:[^'\")]|$this->enumLength)++)\\))?\\s*(zerofill\\s*)?(unsigned(?:\\s+zerofill)?)?)(?:\\s*(?:CHARSET|CHARACTER\\s+SET)\\s*['\"]?([^'\"\\s,]+)['\"]?)?";
        $pattern = "$space*(" . ($type == "FUNCTION" ? "" : $this->inout) . ")?\\s*(?:`((?:[^`]|``)*)`\\s*|\\b(\\S+)\\s+)$type_pattern";
        $create = $this->connection->result("SHOW CREATE $type " . $this->escapeId($name), 2);
        preg_match("~\\(((?:$pattern\\s*,?)*)\\)\\s*" . ($type == "FUNCTION" ? "RETURNS\\s+$type_pattern\\s+" : "") . "(.*)~is", $create, $match);
        $fields = [];
        preg_match_all("~$pattern\\s*,?~is", $match[1], $matches, PREG_SET_ORDER);
        foreach ($matches as $param) {
            $fields[] = [
                "field" => str_replace("``", "`", $param[2]) . $param[3],
                "type" => strtolower($param[5]),
                "length" => preg_replace_callback("~$this->enumLength~s", 'normalize_enum', $param[6]),
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
        $rows = $this->db->rows("SELECT ROUTINE_NAME, ROUTINE_TYPE, DTD_IDENTIFIER " .
            "FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = " . $this->quote($this->selectedDatabase()));
        return array_map(function($row) {
            return new Routine($row['ROUTINE_NAME'], $row['ROUTINE_NAME'], $row['ROUTINE_TYPE'], $row['DTD_IDENTIFIER']);
        }, $rows);
    }

    /**
     * Get list of available routine languages
     * @return array
     */
    public function routineLanguages()
    {
        return []; // "SQL" not required
    }

    /**
     * Get routine signature
     * @param string
     * @param array result of routine()
     * @return string
     */
    public function routineId($name, $row)
    {
        return $this->escapeId($name);
    }

    /**
     * Get last auto increment ID
     * @return string
     */
    public function lastAutoIncrementId()
    {
        return $this->connection->result("SELECT LAST_INSERT_ID()"); // mysql_insert_id() truncates bigint
    }

    /**
     * Explain select
     * @param ConnectionInterface
     * @param string
     * @return Statement
     */
    public function explain($connection, $query)
    {
        return $connection->query("EXPLAIN " . ($this->minVersion(5.1) && !$this->minVersion(5.7) ? "PARTITIONS " : "") . $query);
    }

    /**
     * Get approximate number of rows
     * @param array
     * @param array
     * @return int or null if approximate number can't be retrieved
     */
    public function countRows($tableStatus, $where)
    {
        return ($where || $tableStatus->engine != "InnoDB" ? null : $tableStatus->rows);
    }

    /**
     * Get SQL command to create table
     * @param string
     * @param bool
     * @param string
     * @return string
     */
    public function createTableSql($table, $autoIncrement, $style)
    {
        $return = $this->connection->result("SHOW CREATE TABLE " . $this->table($table), 1);
        if (!$autoIncrement) {
            $return = preg_replace('~ AUTO_INCREMENT=\d+~', '', $return); //! skip comments
        }
        return $return;
    }

    /**
     * Get SQL command to truncate table
     * @param string
     * @return string
     */
    public function truncateTableSql($table)
    {
        return "TRUNCATE " . $this->table($table);
    }

    /**
     * Get SQL command to change database
     * @param string
     * @return string
     */
    public function useDatabaseSql($database)
    {
        return "USE " . $this->escapeId($database);
    }

    /**
     * Get SQL commands to create triggers
     * @param string
     * @return string
     */
    public function createTriggerSql($table)
    {
        $return = "";
        foreach ($this->db->rows("SHOW TRIGGERS LIKE " . $this->quote(addcslashes($table, "%_\\")), null, "-- ") as $row) {
            $return .= "\nCREATE TRIGGER " . $this->escapeId($row["Trigger"]) . " $row[Timing] $row[Event] ON " . $this->table($row["Table"]) . " FOR EACH ROW\n$row[Statement];;\n";
        }
        return $return;
    }

    /**
     * Get server variables
     * @return array ($name => $value)
     */
    public function variables()
    {
        return $this->db->keyValues("SHOW VARIABLES");
    }

    /**
     * Get process list
     * @return array ($row)
     */
    public function processes()
    {
        return $this->db->rows("SHOW FULL PROCESSLIST");
    }

    /**
     * Get status variables
     * @return array ($name => $value)
     */
    public function statusVariables()
    {
        return $this->db->keyValues("SHOW STATUS");
    }

    /**
     * @inheritDoc
     */
    public function convertField($field)
    {
        if (preg_match("~binary~", $field->type)) {
            return "HEX(" . $this->escapeId($field->name) . ")";
        }
        if ($field->type == "bit") {
            return "BIN(" . $this->escapeId($field->name) . " + 0)"; // + 0 is required outside MySQLnd
        }
        if (preg_match("~geometry|point|linestring|polygon~", $field->type)) {
            return ($this->minVersion(8) ? "ST_" : "") . "AsWKT(" . $this->escapeId($field->name) . ")";
        }
    }

    /**
     * @inheritDoc
     */
    public function unconvertField($field, $return)
    {
        if (preg_match("~binary~", $field->type)) {
            $return = "UNHEX($return)";
        }
        if ($field->type == "bit") {
            $return = "CONV($return, 2, 10) + 0";
        }
        if (preg_match("~geometry|point|linestring|polygon~", $field->type)) {
            $return = ($this->minVersion(8) ? "ST_" : "") . "GeomFromText($return, SRID($field[field]))";
        }
        return $return;
    }

    /**
     * Check whether a feature is supported
     * @param string "comment", "copy", "database", "descidx", "drop_col", "dump", "event", "indexes", "kill", "materializedview", "partitioning", "privileges", "procedure", "processlist", "routine", "scheme", "sequence", "status", "table", "trigger", "type", "variables", "view", "view_trigger"
     * @return bool
     */
    public function support($feature)
    {
        return !preg_match("~scheme|sequence|type|view_trigger|materializedview" . ($this->minVersion(8) ? "" : "|descidx" . ($this->minVersion(5.1) ? "" : "|event|partitioning" . ($this->minVersion(5) ? "" : "|routine|trigger|view"))) . "~", $feature);
    }

    /**
     * Kill a process
     * @param int
     * @return bool
     */
    public function killProcess($val)
    {
        return $this->db->queries("KILL " . $this->util->number($val));
    }

    /**
     * Return query to get connection ID
     * @return string
     */
    public function connectionId()
    {
        return "SELECT CONNECTION_ID()";
    }

    /**
     * Get maximum number of connections
     * @return int
     */
    public function maxConnections()
    {
        return $this->connection->result("SELECT @@max_connections");
    }

    /**
     * @inheritDoc
     */
    protected function setConfig()
    {
        $this->config->jush = 'sql';
        $this->config->drivers = ["MySQLi", "PDO_MySQL"];

        $types = [
            $this->util->lang('Numbers') => ["tinyint" => 3, "smallint" => 5, "mediumint" => 8, "int" => 10, "bigint" => 20, "decimal" => 66, "float" => 12, "double" => 21],
            $this->util->lang('Date and time') => ["date" => 10, "datetime" => 19, "timestamp" => 19, "time" => 10, "year" => 4],
            $this->util->lang('Strings') => ["char" => 255, "varchar" => 65535, "tinytext" => 255, "text" => 65535, "mediumtext" => 16777215, "longtext" => 4294967295],
            $this->util->lang('Lists') => ["enum" => 65535, "set" => 64],
            $this->util->lang('Binary') => ["bit" => 20, "binary" => 255, "varbinary" => 65535, "tinyblob" => 255, "blob" => 65535, "mediumblob" => 16777215, "longblob" => 4294967295],
            $this->util->lang('Geometry') => ["geometry" => 0, "point" => 0, "linestring" => 0, "polygon" => 0, "multipoint" => 0, "multilinestring" => 0, "multipolygon" => 0, "geometrycollection" => 0],
        ];
        foreach ($types as $group => $_types) {
            $this->config->structuredTypes[$group] = array_keys($_types);
            $this->config->types = array_merge($this->config->types, $_types);
        }

        $this->config->unsigned = ["unsigned", "zerofill", "unsigned zerofill"]; ///< @var array number variants
        $this->config->operators = ["=", "<", ">", "<=", ">=", "!=", "LIKE", "LIKE %%", "REGEXP", "IN", "FIND_IN_SET", "IS NULL", "NOT LIKE", "NOT REGEXP", "NOT IN", "IS NOT NULL", "SQL"]; ///< @var array operators used in select
        $this->config->functions = ["char_length", "date", "from_unixtime", "lower", "round", "floor", "ceil", "sec_to_time", "time_to_sec", "upper"]; ///< @var array functions used in select
        $this->config->grouping = ["avg", "count", "count distinct", "group_concat", "max", "min", "sum"]; ///< @var array grouping functions used in select];
        $this->config->editFunctions = [[
            "char" => "md5/sha1/password/encrypt/uuid",
            "binary" => "md5/sha1",
            "date|time" => "now",
        ],[
            $this->db->numberRegex() => "+/-",
            "date" => "+ interval/- interval",
            "time" => "addtime/subtime",
            "char|text" => "concat",
        ]];
    }
}
