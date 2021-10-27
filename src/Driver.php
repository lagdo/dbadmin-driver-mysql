<?php

namespace Lagdo\DbAdmin\Driver\MySql;

use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;
use Lagdo\DbAdmin\Driver\Exception\AuthException;

class Driver extends AbstractDriver
{
    /**
     * Features not available
     *
     * @var array
     */
    private $features = ['scheme', 'sequence', 'type', 'view_trigger', 'materializedview'];

    /**
     * Data types
     *
     * @var array
     */
    private $types = [
        'Numbers' => ["tinyint" => 3, "smallint" => 5, "mediumint" => 8, "int" => 10,
            "bigint" => 20, "decimal" => 66, "float" => 12, "double" => 21],
        'Date and time' => ["date" => 10, "datetime" => 19, "timestamp" => 19, "time" => 10, "year" => 4],
        'Strings' => ["char" => 255, "varchar" => 65535, "tinytext" => 255,
            "text" => 65535, "mediumtext" => 16777215, "longtext" => 4294967295],
        'Lists' => ["enum" => 65535, "set" => 64],
        'Binary' => ["bit" => 20, "binary" => 255, "varbinary" => 65535, "tinyblob" => 255,
            "blob" => 65535, "mediumblob" => 16777215, "longblob" => 4294967295],
        'Geometry' => ["geometry" => 0, "point" => 0, "linestring" => 0, "polygon" => 0,
            "multipoint" => 0, "multilinestring" => 0, "multipolygon" => 0, "geometrycollection" => 0],
    ];

    /**
     * Number variants
     *
     * @var array
     */
    private $unsigned = ["unsigned", "zerofill", "unsigned zerofill"];

    /**
     * Operators used in select
     *
     * @var array
     */
    private $operators = ["=", "<", ">", "<=", ">=", "!=", "LIKE", "LIKE %%",
        "REGEXP", "IN", "FIND_IN_SET", "IS NULL", "NOT LIKE", "NOT REGEXP",
        "NOT IN", "IS NOT NULL", "SQL"];

    /**
     * Functions used in select
     *
     * @var array
     */
    private $functions = ["char_length", "date", "from_unixtime", "lower",
        "round", "floor", "ceil", "sec_to_time", "time_to_sec", "upper"];

    /**
     * Grouping functions used in select
     *
     * @var array
     */
    private $grouping = ["avg", "count", "count distinct", "group_concat", "max", "min", "sum"];

    /**
     * Functions used to edit data
     *
     * @var array
     */
    private $editFunctions = [[
        "char" => "md5/sha1/password/encrypt/uuid",
        "binary" => "md5/sha1",
        "date|time" => "now",
    ],[
        // $this->numberRegex() => "+/-",
        "date" => "+ interval/- interval",
        "time" => "addtime/subtime",
        "char|text" => "concat",
    ]];

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
        if (extension_loaded("pdo_mysql")) {
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

        if (!$this->minVersion(8)) {
            $this->features[] = 'descidx';
            if (!$this->minVersion(5.1)) {
                $this->features[] = 'event';
                $this->features[] = 'partitioning';
                if (!$this->minVersion(5)) {
                    $this->features[] = 'routine';
                    $this->features[] = 'trigger';
                    $this->features[] = 'view';
                }
            }
        }

        if ($this->minVersion('5.7.8', 10.2)) {
            $this->config->structuredTypes[$this->trans->lang('Strings')][] = "json";
            $this->config->types["json"] = 4294967295;
        }
    }

    /**
     * @inheritDoc
     */
    public function support(string $feature)
    {
        // $this->features contains features that are not available.
        return !in_array($feature, $this->features);
    }

    /**
     * @inheritDoc
     */
    protected function initConfig()
    {
        $this->config->jush = 'sql';
        $this->config->drivers = ["MySQLi", "PDO_MySQL"];
        $this->config->setTypes($this->types, $this->trans);
        $this->config->unsigned = $this->unsigned;
        $this->config->operators = $this->operators;
        $this->config->functions = $this->functions;
        $this->config->grouping = $this->grouping;
        $this->config->editFunctions = $this->editFunctions;
        $this->config->editFunctions[1][$this->numberRegex()] = "+/-";
    }
}
