<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db\Pdo;

use Lagdo\DbAdmin\Driver\Db\Pdo\Connection as PdoConnection;

use PDO;

/**
 * MySQL driver to be used with the pdo_mysql PHP extension.
 */
class Connection extends PdoConnection
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
        if (!$password) {
            $password = '';
        }

        $options = [PDO::MYSQL_ATTR_LOCAL_INFILE => false];
        $ssl = $this->driver->options('');
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
        $this->dsn("mysql:charset=utf8;host=" . str_replace(":", ";unix_socket=",
            preg_replace('~:(\d)~', ';port=\1', $server)), $username, $password, $options);

        if (($database)) {
            $this->query("USE " . $this->driver->escapeId($database));
        }
        // Available in MySQLi since PHP 5.0.5
        $this->setCharset($this->driver->charset());
        $this->query("SET sql_quote_show_create = 1, autocommit = 1");
        return true;
    }

    /**
     * @inheritDoc
     */
    public function setCharset(string $charset)
    {
        $this->query("SET NAMES $charset"); // charset in DSN is ignored before PHP 5.3.6
    }

    /**
     * @inheritDoc
     */
    public function query(string $query, bool $unbuffered = false)
    {
        $this->client->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, !$unbuffered);
        return parent::query($query, $unbuffered);
    }
}
