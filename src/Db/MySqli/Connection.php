<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db\MySqli;

use Lagdo\DbAdmin\Driver\Db\Connection as AbstractConnection;

use MySQLi;

use function mysqli_report;
use function explode;
use function is_numeric;
use function intval;
use function ini_get;

/**
 * MySQL driver to be used with the mysqli PHP extension.
 */
class Connection extends AbstractConnection
{
    /**
    * @inheritDoc
    */
    public function open(string $database, string $schema = '')
    {
        $server = $this->driver->options('server');
        $options = $this->driver->options();
        $username = $options['username'];
        $password = $options['password'];
        $socket = null;

        // Create the MySQLi client
        $this->client = new MySQLi();
        $this->client->init();

        mysqli_report(MYSQLI_REPORT_OFF); // stays between requests, not required since PHP 5.3.4
        list($host, $port) = explode(":", $server, 2); // part after : is used for port or socket
        $ssl = $this->driver->options('ssl');
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
        return $this->client->server_info;
    }

    /**
     * @inheritDoc
     */
    public function setCharset(string $charset)
    {
        if ($this->client->set_charset($charset)) {
            return true;
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
        return ($result) ? new Statement($result) : false;
    }

    /**
     * @inheritDoc
     */
    public function result(string $query, int $field = -1)
    {
        if ($field < 0) {
            $field = $this->defaultField();
        }
        $result = $this->client->query($query);
        if (!$result) {
            return null;
        }
        $row = $result->fetch_array();
        return count($row) > $field ? $row[$field] : null;
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
            $this->driver->setError($this->client->error);
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
        $this->driver->setError();
        $this->setAffectedRows(0);
        return $this->client->next_result();
    }
}
