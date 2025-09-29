<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db\MySqli;

use Lagdo\DbAdmin\Driver\Db\Connection as AbstractConnection;
use Lagdo\DbAdmin\Driver\Db\PreparedStatement;
use Lagdo\DbAdmin\Driver\Db\StatementInterface;
use Lagdo\DbAdmin\Driver\MySql\Db\ConnectionTrait;
use MySQLi;

use function explode;
use function ini_get;
use function intval;
use function is_numeric;
use function mysqli_report;

/**
 * MySQL driver to be used with the mysqli PHP extension.
 */
class Connection extends AbstractConnection
{
    use ConnectionTrait;

    /**
    * @inheritDoc
    */
    public function open(string $database, string $schema = ''): bool
    {
        $server = $this->options('server');
        $username = $this->options['username'];
        $password = $this->options['password'];
        $socket = null;

        // Create the MySQLi client
        $this->client = new MySQLi();

        mysqli_report(MYSQLI_REPORT_OFF); // stays between requests, not required since PHP 5.3.4
        [$host, $port] = explode(":", $server, 2); // part after : is used for port or socket
        $ssl = $this->options('ssl');
        if ($ssl) {
            $this->client->ssl_set($ssl['key'], $ssl['cert'], $ssl['ca'], '', '');
        }

        if (!@$this->client->real_connect(
            ($server != "" ? $host : ini_get("mysqli.default_host")),
            ($server . $username != "" ? $username : ini_get("mysqli.default_user")),
            ($server . $username . $password != "" ? $password : ini_get("mysqli.default_pw")),
            $database,
            (is_numeric($port) ? intval($port) : intval(ini_get("mysqli.default_port"))),
            (!is_numeric($port) ? $port : $socket),
            ($ssl ? MYSQLI_CLIENT_SSL_DONT_VERIFY_SERVER_CERT : 0) // (not available before PHP 5.6.16)
        )) {
            return false;
        }

        $this->client->options(MYSQLI_OPT_LOCAL_INFILE, false);
        if (($database)) {
            $this->client->select_db($database);
        }
        // Available in MySQLi since PHP 5.0.5
        $this->setCharset($this->driver->charset());
        $this->query("SET sql_quote_show_create = 1, autocommit = 1");
        return true;
    }

    /**
     * @inheritDoc
     */
    public function serverInfo()
    {
        return $this->client?->server_info ?? '';
    }

    /**
     * @inheritDoc
     */
    protected function setCharset(string $charset)
    {
        if ($this->client->set_charset($charset)) {
            return;
        }
        // the client library may not support utf8mb4
        $this->client->set_charset('utf8');
        return $this->client->query("SET NAMES $charset");
    }

    /**
     * @inheritDoc
     */
    public function query(string $query, bool $unbuffered = false)
    {
        $result = $this->client->query($query, $unbuffered);
        return !$result ? false : new Statement($result);
    }

    /**
     * @inheritDoc
     */
    public function quote(string $string)
    {
        return "'" . $this->client->escape_string($string) . "'";
    }

    /**
     * @inheritDoc
     */
    public function multiQuery(string $query)
    {
        return $this->client->multi_query($query);
    }

    /**
     * @inheritDoc
     */
    public function storedResult()
    {
        $result = $this->client->store_result();
        if (!$result) { // The resultset is empty
            // Error or no result
            $this->setError($this->client->error);
            $this->setAffectedRows($this->client->affected_rows);
            return $this->client->error === '';
        }
        return new Statement($result);
    }

    /**
     * @inheritDoc
     */
    public function nextResult()
    {
        $this->setError();
        $this->setAffectedRows(0);
        return $this->client->next_result();
    }

    /**
     * @inheritDoc
     */
    public function prepareStatement(string $query): PreparedStatement
    {
        // MySQLi uses the '?' char as placeholder for query params.
        [$params, $query] = $this->getPreparedParams($query, '?');
        $statement = $this->client->prepare($query);
        return new PreparedStatement($query, $statement, $params);
    }

    /**
     * @inheritDoc
     */
    public function executeStatement(PreparedStatement $statement,
        array $values): ?StatementInterface
    {
        if (!$statement->prepared()) {
            return null;
        }

        $values = $statement->paramValues($values, false);
        $result = $statement->statement()->execute($values);
        if (!$result) {
            $this->setError($this->client->error);
            return null;
        }

        return new Statement($statement->statement()->get_result());
    }
}
