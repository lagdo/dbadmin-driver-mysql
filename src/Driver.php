<?php

namespace Lagdo\DbAdmin\Driver\MySql;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;
use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;

use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;

class Driver extends AbstractDriver
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
    public function createConnection()
    {
        $connection = null;
        if (extension_loaded("mysqli")) {
            $connection = new MySqli\Connection($this, $this->util, 'MySQLi');
        }
        elseif (extension_loaded("pdo_mysql")) {
            $connection = new Pdo\Connection($this, $this->util, 'PDO_MySQL');
        }
        else {
            throw new AuthException($this->util->lang('No package installed to connect to a MySQL server.'));
        }

        if ($this->connection === null) {
            $this->connection = $connection;
            $this->server = new Server($this, $this->util, $connection);
            $this->table = new Table($this, $this->util, $connection);
            $this->query = new Query($this, $this->util, $connection);
            $this->grammar = new Grammar($this, $this->util, $connection);
        }

        if (!$connection->open($this->options('server'), $this->options())) {
            $error = $this->error();
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
            $this->numberRegex() => "+/-",
            "date" => "+ interval/- interval",
            "time" => "addtime/subtime",
            "char|text" => "concat",
        ]];
    }

    /**
     * @inheritDoc
     */
    public function support(string $feature)
    {
        return !preg_match("~scheme|sequence|type|view_trigger|materializedview" .
            ($this->minVersion(8) ? "" : "|descidx" . ($this->minVersion(5.1) ? "" :
            "|event|partitioning" . ($this->minVersion(5) ? "" : "|routine|trigger|view"))) . "~", $feature);
    }

    /**
     * @inheritDoc
     */
    // public function warnings() {
    //     $result = $this->connection->query("SHOW WARNINGS");
    //     if ($result && $result->numRows) {
    //         ob_start();
    //         select($result); // select() usually needs to print a big table progressively
    //         return ob_get_clean();
    //     }
    // }
}
