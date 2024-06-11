<?php

namespace Lagdo\DbAdmin\Driver\MySql;

use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;
use Lagdo\DbAdmin\Driver\Exception\AuthException;
use Lagdo\DbAdmin\Driver\TranslatorInterface;
use Lagdo\DbAdmin\Driver\UtilInterface;

class Driver extends AbstractDriver
{
    /**
     * The constructor
     *
     * @param UtilInterface $util
     * @param TranslatorInterface $trans
     * @param array $options
     */
    public function __construct(UtilInterface $util, TranslatorInterface $trans, array $options)
    {
        parent::__construct($util, $trans, $options);

        $this->server = new Db\Server($this, $this->util, $this->trans);
        $this->database = new Db\Database($this, $this->util, $this->trans);
        $this->table = new Db\Table($this, $this->util, $this->trans);
        $this->query = new Db\Query($this, $this->util, $this->trans);
        $this->grammar = new Db\Grammar($this, $this->util, $this->trans);
    }

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
    protected function initConfig()
    {
        // Init config
        $this->config->jush = 'sql';
        $this->config->drivers = ["MySQLi", "PDO_MySQL"];
        $this->config->setTypes([
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
        ]);
        $this->config->unsigned = ["unsigned", "zerofill", "unsigned zerofill"];
        $this->config->operators = ["=", "<", ">", "<=", ">=", "!=", "LIKE", "LIKE %%",
            "REGEXP", "IN", "FIND_IN_SET", "IS NULL", "NOT LIKE", "NOT REGEXP",
            "NOT IN", "IS NOT NULL", "SQL"];
        $this->config->functions = ["char_length", "date", "from_unixtime", "lower",
            "round", "floor", "ceil", "sec_to_time", "time_to_sec", "upper"];
        $this->config->grouping = ["avg", "count", "count distinct", "group_concat", "max", "min", "sum"];
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
        /**
         * Features not available
         *
         * @var array
         */
        $this->config->features = ['database', 'table', 'columns', 'sql', 'indexes', 'descidx',
            'comment', 'processlist', 'variables', 'drop_col', 'kill', 'dump', 'fkeys_sql'];
    }

    /**
     * @inheritDoc
     */
    protected function postConnectConfig()
    {
        if ($this->minVersion(5)) {
            $this->config->features[] = 'routine';
            $this->config->features[] = 'trigger';
            $this->config->features[] = 'view';
            if ($this->minVersion(5.1)) {
                $this->config->features[] = 'event';
                $this->config->features[] = 'partitioning';
            }
            if ($this->minVersion(8)) {
                $this->config->features[] = 'descidx';
            }
        }
        if ($this->minVersion('5.7.8', 10.2)) {
            $this->config->structuredTypes[$this->trans->lang('Strings')][] = "json";
            $this->config->types["json"] = 4294967295;
        }
    }

    /**
     * @inheritDoc
     * @throws AuthException
     */
    protected function createConnection()
    {
        if (!$this->options('prefer_pdo', false) && extension_loaded("mysqli")) {
            $connection = new Db\MySqli\Connection($this, $this->util, $this->trans, 'MySQLi');
            return $this->connection = $connection;
        }
        if (extension_loaded("pdo_mysql")) {
            $connection = new Db\Pdo\Connection($this, $this->util, $this->trans, 'PDO_MySQL');
            return $this->connection = $connection;
        }
        throw new AuthException($this->trans->lang('No package installed to connect to a MySQL server.'));
    }

    /**
     * @inheritDoc
     */
    public function error()
    {
        $error = preg_replace('~^You have an error.*syntax to use~U', 'Syntax error', parent::error());
        // windows-1250 - most common Windows encoding
        // if (function_exists('iconv') && !$this->util->isUtf8($error) &&
        //     strlen($s = iconv("windows-1250", "utf-8", $error)) > strlen($error)) {
        //     $error = $s;
        // }
        return $this->util->html($error);
    }
}
