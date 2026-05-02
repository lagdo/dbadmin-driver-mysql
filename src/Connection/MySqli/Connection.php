<?php

namespace Lagdo\DbAdmin\Driver\MySql\Connection\MySqli;

use Lagdo\DbAdmin\Driver\MySql\Connection\Traits\ConnectionTrait;
use Lagdo\DbAdmin\Driver\Sql\Connection\AbstractConnection;
use Lagdo\DbAdmin\Driver\Sql\Connection\PreparedStatement;
use Lagdo\DbAdmin\Driver\Sql\Connection\QueryResultInterface;
use mysqli;
use mysqli_stmt;

use function ini_get;
use function intval;
use function is_numeric;
use function mysqli_init;
use function mysqli_report;

/**
 * MySQL driver to be used with the mysqli PHP extension.
 */
class Connection extends AbstractConnection
{
    use ConnectionTrait;

    /**
     * The client object used to query the database driver
     *
     * @var mysqli|bool
     */
    protected mysqli|bool $client;

    /**
    * @inheritDoc
    */
    public function open(string $database, string $schema = ''): bool
    {
        $host = $this->options('host');
        $port = $this->options('port') ?: '';
        $username = $this->options['username'];
        $password = $this->options['password'];
        $socket = null;

        // Create the MySQLi client
        $this->client = mysqli_init();

        // Specify the connection timeout (both timeouts need to be set)
        // See https://stackoverflow.com/questions/64853050/why-mysqli-connect-doesnt-respect-mysqli-opt-connect-timeout
        $this->client->options(MYSQLI_OPT_CONNECT_TIMEOUT, 2);
        $this->client->options(MYSQLI_OPT_READ_TIMEOUT, 2);
        mysqli_report(MYSQLI_REPORT_OFF); // stays between requests, not required since PHP 5.3.4
        $ssl = $this->options('ssl');
        if ($ssl) {
            $this->client->ssl_set($ssl['key'], $ssl['cert'], $ssl['ca'], '', '');
        }

        $server = $port === '' ? $host : "$host:$port";
        if (!@$this->client->real_connect(
            ($server !== '' ? $host : ini_get('mysqli.default_host')),
            ($server . $username !== '' ? $username : ini_get('mysqli.default_user')),
            ($server . $username . $password !== '' ? $password : ini_get('mysqli.default_pw')),
            $database,
            (is_numeric($port) ? intval($port) : intval(ini_get('mysqli.default_port'))),
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
        $this->setCharset($this->_engine()->charset());
        $this->executeQuery('SET sql_quote_show_create = 1, autocommit = 1');
        return true;
    }

    /**
     * @inheritDoc
     */
    public function serverInfo(): string
    {
        return $this->client?->server_info ?? '';
    }

    /**
     * @inheritDoc
     */
    protected function setCharset(string $charset): void
    {
        if ($this->client->set_charset($charset)) {
            return;
        }

        // the client library may not support utf8mb4
        $this->client->set_charset('utf8');
        $this->client->query("SET NAMES $charset");
    }

    /**
     * @inheritDoc
     */
    public function executeQuery(string $query, bool $unbuffered = false): QueryResultInterface
    {
        $result = $this->client->query($query, $unbuffered);
        return new QueryResult($result);
    }

    /**
     * @inheritDoc
     */
    public function quote(string $string): string
    {
        return "'" . $this->client->escape_string($string) . "'";
    }

    /**
     * @inheritDoc
     */
    public function prepareStatement(string $query): PreparedStatement
    {
        // MySQLi uses the '?' char as placeholder for query params.
        [$params, $query] = $this->getPreparedParams($query, fn() => '?');
        $statement = $this->client->prepare($query);
        return new PreparedStatement($statement, $query, $params);
    }

    /**
     * @inheritDoc
     */
    public function executeStatement(PreparedStatement $preparedStatement,
        array $values): QueryResultInterface
    {
        /** @var mysqli_stmt|bool */
        $statement = $preparedStatement->statement();
        if (!$statement) {
            $this->setError($this->_utils()->lang($this->statementNotPrepared));
            return new QueryResult(false);
        }

        $values = $preparedStatement->paramValues($values, false);
        if (!$statement->execute($values)) {
            $this->setError($this->client->error);
            return new QueryResult(false);
        }

        return new QueryResult($statement->get_result());
    }

    /**
     * @inheritDoc
     */
    public function executeMultiQuery(string $query): QueryResultInterface
    {
        return new QueryResult($this->client->multi_query($query));
    }

    /**
     * @inheritDoc
     */
    public function readRowset(QueryResultInterface $_): QueryResultInterface
    {
        $result = $this->client->store_result();
        if (!$result) { // The resultset is empty
            // Error or no result
            $this->setError($this->client->error);
            $this->setAffectedRows($this->client->affected_rows);
        }

        return new QueryResult($result);
    }

    /**
     * @inheritDoc
     */
    public function nextRowset(QueryResultInterface $_): bool
    {
        $this->setError();
        $this->setAffectedRows(0);

        return $this->client->next_result();
    }
}
