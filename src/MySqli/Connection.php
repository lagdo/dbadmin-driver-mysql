<?php

namespace Lagdo\DbAdmin\Driver\MySql\MySqli;

use Lagdo\DbAdmin\Driver\AdminerDbInterface;
use Lagdo\DbAdmin\Driver\AdminerUtilInterface;
use Lagdo\DbAdmin\Driver\Db\ServerInterface;
use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;
use Lagdo\DbAdmin\Driver\Db\ConnectionTrait;

use MySQLi;

/**
 * MySQL driver to be used with the mysqli PHP extension.
 */
class Connection extends MySQLi implements ConnectionInterface
{
    use ConnectionTrait;

    /**
     * The constructor
     *
     * @param AdminerDbInterface $db
     * @param AdminerUtilInterface $util
     * @param ServerInterface $server
     * @param string $extension
     */
    public function __construct(AdminerDbInterface $db, AdminerUtilInterface $util, ServerInterface $server, string $extension)
    {
        parent::init();
        $this->extension = 'MySQLi';

        $this->db = $db;
        $this->util = $util;
        $this->server = $server;
        $this->extension = $extension;
    }

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

        mysqli_report(MYSQLI_REPORT_OFF); // stays between requests, not required since PHP 5.3.4
        list($host, $port) = explode(":", $server, 2); // part after : is used for port or socket
        $ssl = $this->db->connectSsl();
        if ($ssl) {
            $this->ssl_set($ssl['key'], $ssl['cert'], $ssl['ca'], '', '');
        }
        $return = @$this->real_connect(
            ($server != "" ? $host : ini_get("mysqli.default_host")),
            ($server . $username != "" ? $username : ini_get("mysqli.default_user")),
            ($server . $username . $password != "" ? $password : ini_get("mysqli.default_pw")),
            $database,
            (is_numeric($port) ? $port : ini_get("mysqli.default_port")),
            (!is_numeric($port) ? $port : $socket),
            ($ssl ? MYSQLI_CLIENT_SSL_DONT_VERIFY_SERVER_CERT : 0) // (not available before PHP 5.6.16)
        );
        $this->options(MYSQLI_OPT_LOCAL_INFILE, false);
        return $return;
    }

    public function set_charset($charset)
    {
        if (parent::set_charset($charset)) {
            return true;
        }
        // the client library may not support utf8mb4
        parent::set_charset('utf8');
        return $this->query("SET NAMES $charset");
    }

    public function result($query, $field = 0)
    {
        $result = $this->query($query);
        if (!$result) {
            return false;
        }
        $row = $result->fetch_array();
        return $row[$field];
    }

    public function quote($string)
    {
        return "'" . $this->escape_string($string) . "'";
    }
}
