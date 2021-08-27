<?php

namespace Lagdo\Adminer\Driver\MySql\Pdo;

use Lagdo\Adminer\Driver\Db\Pdo\Connection as PdoConnection;

use PDO;

/**
 * MySQL driver to be used with the pdo_mysql PHP extension.
 */
class Connection extends PdoConnection
{
    /**
     * @inheritDoc
     */
    public function open($server, array $options)
    {
        $username = $options['username'];
        $password = $options['password'];

        $options = array(PDO::MYSQL_ATTR_LOCAL_INFILE => false);
        $ssl = $this->db->connectSsl();
        if ($ssl) {
            if (!empty($ssl['key'])) {
                $options[PDO::MYSQL_ATTR_SSL_KEY] = $ssl['key'];
            }
            if (!empty($ssl['cert'])) {
                $options[PDO::MYSQL_ATTR_SSL_CERT] = $ssl['cert'];
            }
            if (!empty($ssl['ca'])) {
                $options[PDO::MYSQL_ATTR_SSL_CA] = $ssl['ca'];
            }
        }
        $this->dsn(
            "mysql:charset=utf8;host=" . str_replace(":", ";unix_socket=", preg_replace('~:(\d)~', ';port=\1', $server)),
            $username,
            $password,
            $options
        );
        return true;
    }

    public function set_charset($charset)
    {
        $this->query("SET NAMES $charset"); // charset in DSN is ignored before PHP 5.3.6
    }

    public function select_db($database)
    {
        // database selection is separated from the connection so dbname in DSN can't be used
        return $this->query("USE " . $this->server->idf_escape($database));
    }

    public function query($query, $unbuffered = false)
    {
        $this->client->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, !$unbuffered);
        return parent::query($query, $unbuffered);
    }
}
