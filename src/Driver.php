<?php

namespace Lagdo\DbAdmin\Driver\MySql;

use Lagdo\DbAdmin\Driver\Utils\Utils;
use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;
use Lagdo\DbAdmin\Driver\Exception\AuthException;

use function extension_loaded;

class Driver extends AbstractDriver
{
    /**
     * The constructor
     *
     * @param Utils $utils
     * @param array $options
     */
    public function __construct(Utils $utils, array $options)
    {
        parent::__construct($utils, $options);

        $this->server = new Db\Server($this, $this->utils);
        $this->database = new Db\Database($this, $this->utils);
        $this->table = new Db\Table($this, $this->utils);
        $this->query = new Db\Query($this, $this->utils);
        $this->grammar = new Db\Grammar($this, $this->utils);
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
    protected function beforeConnection()
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
        $this->config->insertFunctions = [
            "char" => "md5/sha1/password/encrypt/uuid",
            "binary" => "md5/sha1",
            "date|time" => "now",
        ];
        $this->config->editFunctions = [
            $this->numberRegex() => "+/-",
            "date" => "+ interval/- interval",
            "time" => "addtime/subtime",
            "char|text" => "concat",
        ];
        // Features always available
        $this->config->features = ['comment', 'columns', 'copy', 'database', 'drop_col',
            'dump', 'indexes', 'kill', 'privileges', 'move_col', 'procedure', 'processlist',
            'routine', 'sql', 'status', 'table', 'trigger', 'variables', 'view'];
    }

    /**
     * @inheritDoc
     */
    protected function configConnection()
    {
        if ($this->minVersion(5.1)) {
            $this->config->features[] = 'event';
        }
        if ($this->minVersion(8)) {
            $this->config->features[] = 'descidx';
        }
        if ($this->minVersion('8.0.16', '10.2.1')) {
            $this->config->features[] = 'check';
        }

        if ($this->minVersion('5.7.8', 10.2)) {
            $this->config->setTypes(['Strings' => ["json" => 4294967295]]);
        }
        if ($this->minVersion('', 10.7)) {
            $this->config->setTypes(['Strings' => ["uuid" => 128]]);
            $this->config->insertFunctions['uuid'] = 'uuid';
        }
        if ($this->minVersion(9, '')) {
            $this->config->setTypes(['Numbers' => ["vecto" => 16383]]);
            $this->config->insertFunctions['vector'] = 'string_to_vector';
        }
        if ($this->minVersion(5.1, '')) {
            $this->config->partitionBy = ["HASH", "LINEAR HASH", "KEY", "LINEAR KEY", "RANGE", "LIST"];
        }
        if ($this->minVersion(5.7, 10.2)) {
            $this->config->generated = ["STORED", "VIRTUAL"];
        }
    }

    /**
     * @inheritDoc
     */
    protected function openedConnection()
    {
        $this->server->setConnection($this->connection);
    }

    /**
     * @inheritDoc
     * @throws AuthException
     */
    public function createConnection(array $options)
    {
        $preferPdo = $options['prefer_pdo'] ?? false;
        if (!$preferPdo && extension_loaded("mysqli")) {
            return new Db\MySqli\Connection($this, $this->utils, $options, 'MySQLi');
        }
        if (extension_loaded("pdo_mysql")) {
            return new Db\Pdo\Connection($this, $this->utils, $options, 'PDO_MySQL');
        }
        throw new AuthException($this->utils->trans->lang('No package installed to connect to a MySQL server.'));
    }
}
