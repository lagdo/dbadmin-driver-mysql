<?php

namespace Lagdo\DbAdmin\Driver\MySql\Engine;

use Lagdo\DbAdmin\Driver\Sql\Specific\Connection\AbstractConnection;
use Lagdo\DbAdmin\Driver\Sql\Specific\Engine\AbstractServer;
use Lagdo\DbAdmin\Driver\Sql\Dto\UserDto;
use Lagdo\DbAdmin\Driver\Exception\AuthException;
use Lagdo\DbAdmin\Driver\MySql\Connection;

use function extension_loaded;
use function preg_match;
use function preg_match_all;

class Server extends AbstractServer
{
    /**
     * @inheritDoc
     */
    protected function starting(): void
    {
        $trans = $this->_utils()->trans;
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
            $this->_engine()->numberRegex() => ["+", "-"],
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
    protected function connected(): void
    {
        if ($this->_engine()->minVersion(5.1)) {
            $this->config->features[] = 'event';
        }
        if ($this->_engine()->minVersion(8)) {
            $this->config->features[] = 'descidx';
        }
        if ($this->_engine()->minVersion('8.0.16', '10.2.1')) {
            $this->config->features[] = 'check';
        }

        $trans = $this->_utils()->trans;
        if ($this->_engine()->minVersion('5.7.8', 10.2)) {
            $this->config->types[$trans->lang('Strings')]["json"] = 4294967295;
        }
        if ($this->_engine()->minVersion('', 10.7)) {
            $this->config->types[$trans->lang('Strings')]["uuid"] = 128;
            $this->config->insertFunctions['uuid'] = ['uuid'];
        }
        if ($this->_engine()->minVersion(9, '')) {
            $this->config->types[$trans->lang('Numbers')]["vector"] = 16383;
            $this->config->insertFunctions['vector'] = ['string_to_vector'];
        }
        if ($this->_engine()->minVersion(5.1, '')) {
            $this->config->partitionBy = ["HASH", "LINEAR HASH", "KEY", "LINEAR KEY", "RANGE", "LIST"];
        }
        if ($this->_engine()->minVersion(5.7, 10.2)) {
            $this->config->generated = ["STORED", "VIRTUAL"];
        }
    }

    /**
     * @inheritDoc
     * @throws AuthException
     */
    public function createConnection(array $options): AbstractConnection|null
    {
        $preferPdo = $options['prefer_pdo'] ?? false;
        if (!$preferPdo && extension_loaded("mysqli")) {
            return new Connection\MySqli\Connection($this->_engine(),
                $this->_statement(), $this->_utils(), $options, 'MySQLi');
        }
        if (extension_loaded("pdo_mysql")) {
            return new Connection\Pdo\Connection($this->_engine(),
                $this->_statement(), $this->_utils(), $options, 'PDO_MySQL');
        }

        throw new AuthException($this->_utils()->trans
            ->lang('No package installed to connect to a MySQL server.'));
    }

    /**
     * @inheritDoc
     */
    public function user(): string
    {
        return $this->_engine()->result('SELECT USER()');
    }

    /**
     * @inheritDoc
     */
    public function getUsers(string $database): array
    {
        // From privileges.inc.php
        $clause = ($database == '' ? 'user' : 'db WHERE ' .
            $this->connection->quote($database) . ' LIKE Db');
        $query = "SELECT User, Host FROM mysql.$clause ORDER BY Host, User";
        $statement = $this->connection->query($query);
        // $grant = $statement;
        if (!$statement) {
            // list logged user, information_schema.USER_PRIVILEGES lists just the current user too
            $statement = $this->connection->query("SELECT SUBSTRING_INDEX(CURRENT_USER, '@', 1) " .
                "AS User, SUBSTRING_INDEX(CURRENT_USER, '@', -1) AS Host");
        }
        $users = [];
        while ($user = $statement->fetchAssoc()) {
            $users[] = $user;
        }
        return $users;
    }

    /**
     * @param UserDto $user
     * @param array $grant
     *
     * @return void
     */
    private function addUserGrant(UserDto $user, array $grant)
    {
        if (preg_match('~GRANT (.*) ON (.*) TO ~', $grant[0], $match) &&
            preg_match_all('~ *([^(,]*[^ ,(])( *\([^)]+\))?~', $match[1], $matches, PREG_SET_ORDER)) {
            //! escape the part between ON and TO
            foreach ($matches as $val) {
                $match2 = $match[2] ?? '';
                $val2 = $val[2] ?? '';
                if ($val[1] != 'USAGE') {
                    $user->grants["$match2$val2"][$val[1]] = true;
                }
                if (preg_match('~ WITH GRANT OPTION~', $grant[0])) { //! don't check inside strings and identifiers
                    $user->grants["$match2$val2"]['GRANT OPTION'] = true;
                }
            }
        }
        if (preg_match("~ IDENTIFIED BY PASSWORD '([^']+)~", $grant[0], $match)) {
            $user->password = $match[1];
        }
    }

    /**
     * @inheritDoc
     */
    public function getUserGrants(string $user, string $host): UserDto
    {
        $entity = new UserDto($user, $host);

        // From user.inc.php
        //! use information_schema for MySQL 5 - column names in column privileges are not escaped
        $query = 'SHOW GRANTS FOR ' . $this->connection->quote($user) .
            '@' . $this->connection->quote($host);
        if (!($statement = $this->connection->query($query))) {
            return $entity;
        }

        while ($grant = $statement->fetchRow()) {
            $this->addUserGrant($entity, $grant);
        }
        return $entity;
    }

    /**
     * @inheritDoc
     */
    public function engines(): array
    {
        $engines = [];
        foreach ($this->_engine()->rows('SHOW ENGINES') as $row) {
            if (preg_match('~YES|DEFAULT~', $row['Support'])) {
                $engines[] = $row['Engine'];
            }
        }
        return $engines;
    }

    /**
     * @inheritDoc
     */
    public function collations(): array
    {
        $collations = [];
        foreach ($this->_engine()->rows('SHOW COLLATION') as $row) {
            if ($row['Default']) {
                $collations[$row['Charset']][-1] = $row['Collation'];
                continue;
            }
            // Else
            $collations[$row['Charset']][] = $row['Collation'];
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
    public function routineLanguages(): array
    {
        return []; // 'SQL' not required
    }

    /**
     * @inheritDoc
     */
    public function variables(): array
    {
        return $this->_engine()->keyValues('SHOW VARIABLES');
    }

    /**
     * @inheritDoc
     */
    public function processes(): array
    {
        return $this->_engine()->rows('SHOW FULL PROCESSLIST');
    }

    /**
     * @inheritDoc
     */
    public function statusVariables(): array
    {
        return $this->_engine()->keyValues('SHOW STATUS');
    }

    /**
     * @inheritDoc
     */
    // public function killProcess($val): bool
    // {
    //     return $this->_engine()->execute('KILL ' . $this->_utils()->str->number($val));
    // }

    /**
     * @inheritDoc
     */
    // public function maxConnections(): int
    // {
    //     return $this->_engine()->result('SELECT @@max_connections');
    // }
}
