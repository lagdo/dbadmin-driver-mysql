<?php

namespace Lagdo\DbAdmin\Driver\MySql;

use Lagdo\DbAdmin\Driver\Db\Connection;
use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;
use Lagdo\DbAdmin\Driver\Exception\AuthException;

use function extension_loaded;

class Driver extends AbstractDriver
{
    /**
     * @var Db\Server|null
     */
    private Db\Server|null $server = null;

    /**
     * @var Db\Database|null
     */
    private Db\Database|null $database = null;

    /**
     * @var Db\Table|null
     */
    private Db\Table|null $table = null;

    /**
     * @var Db\Query|null
     */
    private Db\Query|null $query = null;

    /**
     * @var Db\Grammar|null
     */
    private Db\Grammar|null $grammar = null;

    /**
     * @var Db\Server
     */
    protected function _server(): Db\Server
    {
        return $this->server ?: $this->server = new Db\Server($this, $this->utils);
    }

    /**
     * @var Db\Database
     */
    protected function _database(): Db\Database
    {
        return $this->database ?: $this->database = new Db\Database($this, $this->utils);
    }

    /**
     * @var Db\Table
     */
    protected function _table(): Db\Table
    {
        return $this->table ?: $this->table = new Db\Table($this, $this->utils);
    }

    /**
     * @var Db\Grammar
     */
    protected function _grammar(): Db\Grammar
    {
        return $this->grammar ?: $this->grammar = new Db\Grammar($this, $this->utils);
    }

    /**
     * @var Db\Query
     */
    protected function _query(): Db\Query
    {
        return $this->query ?: $this->query = new Db\Query($this, $this->utils);
    }

    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return "MySQL";
    }

    /**
     * @inheritDoc
     */
    protected function beforeConnection(): void
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
            "char" => ["md5", "sha1", "password", "encrypt", "uuid"],
            "binary" => ["md5", "sha1"],
            "date|time" => ["now"],
        ];
        $this->config->editFunctions = [
            $this->numberRegex() => ["+", "-"],
            "date" => ["+ interval", "- interval"],
            "time" => ["addtime", "subtime"],
            "char|text" => ["concat"],
        ];
        // Features always available
        $this->config->features = ['comment', 'columns', 'copy', 'database', 'drop_col',
            'dump', 'indexes', 'kill', 'privileges', 'move_col', 'procedure', 'processlist',
            'routine', 'sql', 'status', 'table', 'trigger', 'variables', 'view'];
    }

    /**
     * @inheritDoc
     */
    protected function configConnection(): void
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
            $this->config->insertFunctions['uuid'] = ['uuid'];
        }
        if ($this->minVersion(9, '')) {
            $this->config->setTypes(['Numbers' => ["vecto" => 16383]]);
            $this->config->insertFunctions['vector'] = ['string_to_vector'];
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
    protected function connectionOpened(): void
    {
        $this->_server()->setConnection($this->connection);
    }

    /**
     * @inheritDoc
     * @throws AuthException
     */
    public function createConnection(array $options): Connection|null
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
