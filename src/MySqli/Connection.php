<?php

namespace Lagdo\DbAdmin\Driver\MySql\MySqli;

use Lagdo\DbAdmin\Driver\Db\Connection as AbstractConnection;

use MySQLi;

/**
 * MySQL driver to be used with the mysqli PHP extension.
 */
class Connection extends AbstractConnection
{
    /**
    * @inheritDoc
    */
    public function open($server, array $options)
    {
        $username = $options['username'];
        $password = $options['password'];
        $database = null;
        $port = null;
        $socket = null;

        // Create the MuSQLi client
        $this->client = new MySQLi();
        $this->client->init();

        mysqli_report(MYSQLI_REPORT_OFF); // stays between requests, not required since PHP 5.3.4
        list($host, $port) = explode(":", $server, 2); // part after : is used for port or socket
        $ssl = $this->db->sslOptions();
        if ($ssl) {
            $this->client->ssl_set($ssl['key'], $ssl['cert'], $ssl['ca'], '', '');
        }

        $return = @$this->client->real_connect(
            ($server != "" ? $host : ini_get("mysqli.default_host")),
            ($server . $username != "" ? $username : ini_get("mysqli.default_user")),
            ($server . $username . $password != "" ? $password : ini_get("mysqli.default_pw")),
            $database,
            (is_numeric($port) ? $port : ini_get("mysqli.default_port")),
            (!is_numeric($port) ? $port : $socket),
            ($ssl ? MYSQLI_CLIENT_SSL_DONT_VERIFY_SERVER_CERT : 0) // (not available before PHP 5.6.16)
        );
        $this->client->options(MYSQLI_OPT_LOCAL_INFILE, false);
        return $return;
    }

    /**
     * @inheritDoc
     */
    public function getServerInfo()
    {
        return $this->client->server_info;
    }

    /**
     * @inheritDoc
     */
    public function selectDatabase($database)
    {
        return $this->client->select_db($database);
    }

    /**
     * @inheritDoc
     */
    public function setCharset($charset)
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
    public function query($query, $unbuffered = false)
    {
        return $this->client->query($query, $unbuffered);
    }

    /**
     * @inheritDoc
     */
    public function result($query, $field = 0)
    {
        $result = $this->client->query($query);
        if (!$result) {
            return false;
        }
        $row = $result->fetch_array();
        return $row[$field];
    }

    /**
     * @inheritDoc
     */
    public function quote($string)
    {
        return "'" . $this->client->escape_string($string) . "'";
    }

    /**
     * @inheritDoc
     */
    public function nextResult()
    {
        return $this->client->next_result();
    }

    /**
     * @inheritDoc
     */
    public function multiQuery($query)
    {
        return $this->client->multi_query($query);
    }

    /**
     * @inheritDoc
     */
    public function storedResult($result = null)
    {
        return $this->client->store_result($result);
    }
}
