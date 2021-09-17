<?php

namespace Lagdo\DbAdmin\Driver\MySql;

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
            $connection = new Db\MySqli\Connection($this, $this->util, $this->trans, 'MySQLi');
        }
        elseif (extension_loaded("pdo_mysql")) {
            $connection = new Db\Pdo\Connection($this, $this->util, $this->trans, 'PDO_MySQL');
        }
        else {
            throw new AuthException($this->trans->lang('No package installed to connect to a MySQL server.'));
        }

        if ($this->connection === null) {
            $this->connection = $connection;
            $this->server = new Db\Server($this, $this->util, $this->trans, $connection);
            $this->table = new Db\Table($this, $this->util, $this->trans, $connection);
            $this->query = new Db\Query($this, $this->util, $this->trans, $connection);
            $this->grammar = new Db\Grammar($this, $this->util, $this->trans, $connection);
        }

        // if (!$connection->open($this->options('server'), $this->options())) {
        //     $error = $this->error();
        //     // windows-1250 - most common Windows encoding
        //     if (function_exists('iconv') && !$this->util->isUtf8($error) &&
        //         strlen($s = iconv("windows-1250", "utf-8", $error)) > strlen($error)) {
        //         $error = $s;
        //     }
        //     throw new AuthException($error);
        // }

        return $connection;
    }

    /**
     * @inheritDoc
     */
    public function connect(string $database, string $schema)
    {
        parent::connect($database, $schema);

        if ($this->minVersion('5.7.8', 10.2)) {
            $this->config->structuredTypes[$this->trans->lang('Strings')][] = "json";
            $this->config->types["json"] = 4294967295;
        }
    }

    /**
     * @inheritDoc
     */
    protected function initConfig()
    {
        $this->config->jush = 'sql';
        $this->config->drivers = ["MySQLi", "PDO_MySQL"];

        $groups = [
            $this->trans->lang('Numbers') => ["tinyint" => 3, "smallint" => 5, "mediumint" => 8, "int" => 10, "bigint" => 20, "decimal" => 66, "float" => 12, "double" => 21],
            $this->trans->lang('Date and time') => ["date" => 10, "datetime" => 19, "timestamp" => 19, "time" => 10, "year" => 4],
            $this->trans->lang('Strings') => ["char" => 255, "varchar" => 65535, "tinytext" => 255, "text" => 65535, "mediumtext" => 16777215, "longtext" => 4294967295],
            $this->trans->lang('Lists') => ["enum" => 65535, "set" => 64],
            $this->trans->lang('Binary') => ["bit" => 20, "binary" => 255, "varbinary" => 65535, "tinyblob" => 255, "blob" => 65535, "mediumblob" => 16777215, "longblob" => 4294967295],
            $this->trans->lang('Geometry') => ["geometry" => 0, "point" => 0, "linestring" => 0, "polygon" => 0, "multipoint" => 0, "multilinestring" => 0, "multipolygon" => 0, "geometrycollection" => 0],
        ];
        foreach ($groups as $name => $types) {
            $this->config->structuredTypes[$name] = array_keys($types);
            $this->config->types = array_merge($this->config->types, $types);
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
