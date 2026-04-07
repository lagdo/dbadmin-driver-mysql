<?php

namespace Lagdo\DbAdmin\Support\MySql;

use Lagdo\DbAdmin\Support\AbstractDriver;
use Lagdo\DbAdmin\Support\Db\Engine\Driver\AbstractConnection;
use Lagdo\DbAdmin\Support\Exception\AuthException;

use function extension_loaded;

class Driver extends AbstractDriver
{
    /**
     * @var Grammar|null;
     */
    private Grammar|null $grammar = null;

    /**
     * @var Driver\Server|null
     */
    private Driver\Server|null $server = null;

    /**
     * @var Driver\Database|null
     */
    private Driver\Database|null $database = null;

    /**
     * @var Driver\Table|null
     */
    private Driver\Table|null $table = null;

    /**
     * @var Driver\Query|null
     */
    private Driver\Query|null $query = null;

    /**
     * @return Grammar
     */
    public function grammar(): Grammar
    {
        return $this->grammar ??= new Grammar($this, $this->utils);
    }

    /**
     * @return Driver\Server
     */
    protected function _server(): Driver\Server
    {
        return $this->server ??= new Driver\Server($this, $this->grammar(), $this->utils);
    }

    /**
     * @return Driver\Database
     */
    protected function _database(): Driver\Database
    {
        return $this->database ??= new Driver\Database($this, $this->grammar(), $this->utils);
    }

    /**
     * @return Driver\Table
     */
    protected function _table(): Driver\Table
    {
        return $this->table ??= new Driver\Table($this, $this->grammar(), $this->utils);
    }

    /**
     * @return Driver\Query
     */
    protected function _query(): Driver\Query
    {
        return $this->query ??= new Driver\Query($this, $this->grammar(), $this->utils);
    }

    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return $this->flavor() === 'maria' ? 'MariaDB' : 'MySQL';
    }

    /**
     * @inheritDoc
     */
    protected function beforeConnection(): void
    {
        $trans = $this->utils->trans;
        // Init config
        $this->config->jush = 'sql';
        $this->config->drivers = ["MySQLi", "PDO_MySQL"];
        $this->config->types = [
            $trans->lang('Numbers') => ["tinyint" => 3, "smallint" => 5, "mediumint" => 8, "int" => 10,
                "bigint" => 20, "decimal" => 66, "float" => 12, "double" => 21],
            $trans->lang('Date and time') => ["date" => 10, "datetime" => 19, "timestamp" => 19, "time" => 10, "year" => 4],
            $trans->lang('Strings') => ["char" => 255, "varchar" => 65535, "tinytext" => 255,
                "text" => 65535, "mediumtext" => 16777215, "longtext" => 4294967295],
            $trans->lang('Lists') => ["enum" => 65535, "set" => 64],
            $trans->lang('Binary') => ["bit" => 20, "binary" => 255, "varbinary" => 65535, "tinyblob" => 255,
                "blob" => 65535, "mediumblob" => 16777215, "longblob" => 4294967295],
            $trans->lang('Geometry') => ["geometry" => 0, "point" => 0, "linestring" => 0, "polygon" => 0,
                "multipoint" => 0, "multilinestring" => 0, "multipolygon" => 0, "geometrycollection" => 0],
        ];
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

        // Regex to parse SQL statements in a text
        $this->config->sqlStatementRegex = '\\s*|[\'"`#]|/\*|-- |$';
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

        $trans = $this->utils->trans;
        if ($this->minVersion('5.7.8', 10.2)) {
            $this->config->types[$trans->lang('Strings')]["json"] = 4294967295;
        }
        if ($this->minVersion('', 10.7)) {
            $this->config->types[$trans->lang('Strings')]["uuid"] = 128;
            $this->config->insertFunctions['uuid'] = ['uuid'];
        }
        if ($this->minVersion(9, '')) {
            $this->config->types[$trans->lang('Numbers')]["vector"] = 16383;
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
    public function createConnection(array $options): AbstractConnection|null
    {
        $preferPdo = $options['prefer_pdo'] ?? false;
        if (!$preferPdo && extension_loaded("mysqli")) {
            return new Connection\MySqli\Connection($this,
                $this->grammar(), $this->utils, $options, 'MySQLi');
        }
        if (extension_loaded("pdo_mysql")) {
            return new Connection\Pdo\Connection($this,
                $this->grammar(), $this->utils, $options, 'PDO_MySQL');
        }
        throw new AuthException($this->utils->trans
            ->lang('No package installed to connect to a MySQL server.'));
    }
}
