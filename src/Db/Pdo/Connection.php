<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db\Pdo;

use Lagdo\DbAdmin\Driver\Db\Pdo\AbstractConnection;
use Lagdo\DbAdmin\Driver\Db\StatementInterface;
use Lagdo\DbAdmin\Driver\MySql\Db\Traits\ConnectionTrait;

use PDO;

/**
 * MySQL driver to be used with the pdo_mysql PHP extension.
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
        if (!$password) {
            $password = '';
        }

        $options = [PDO::MYSQL_ATTR_LOCAL_INFILE => false];
        $ssl = $this->options('ssl');
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

        $dsn = "mysql:charset=utf8;host=" . str_replace(":", ";unix_socket=",
            preg_replace('~:(\d)~', ';port=\1', $server));
        if (!$this->dsn($dsn, $username, $password, $options)) {
            return false;
        }


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
    protected function setCharset(string $charset): void
    {
        $this->query("SET NAMES $charset"); // charset in DSN is ignored before PHP 5.3.6
    }

    /**
     * @inheritDoc
     */
    public function query(string $query, bool $unbuffered = false): StatementInterface|bool
    {
        $this->client->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, !$unbuffered);
        return parent::query($query, $unbuffered);
    }
}
