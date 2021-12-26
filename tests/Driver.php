<?php

namespace Lagdo\DbAdmin\Driver\MySql\Tests;

use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;
use Lagdo\DbAdmin\Driver\MySql\Driver as MySqlDriver;
use Lagdo\DbAdmin\Driver\MySql\Db\Server;
use Lagdo\DbAdmin\Driver\MySql\Db\Database;
use Lagdo\DbAdmin\Driver\MySql\Db\Table;
use Lagdo\DbAdmin\Driver\MySql\Db\Query;
use Lagdo\DbAdmin\Driver\MySql\Db\Grammar;

class Driver extends MySqlDriver
{
    /**
     * @var Connection
     */
    protected $testConnection;

    /**
     * @inheritDoc
     */
    public function createConnection()
    {
        $this->testConnection = new Connection($this, $this->util, $this->trans, 'test');
        $this->connection = $this->testConnection;
        $this->server = new Server($this, $this->util, $this->trans, $this->connection);
        $this->database = new Database($this, $this->util, $this->trans, $this->connection);
        $this->table = new Table($this, $this->util, $this->trans, $this->connection);
        $this->query = new Query($this, $this->util, $this->trans, $this->connection);
        $this->grammar = new Grammar($this, $this->util, $this->trans, $this->connection);

        return $this->connection;
    }

    /**
     * @inheritDoc
     */
    public function connect(string $database, string $schema)
    {
        AbstractDriver::connect($database, $schema);
    }

    /**
     * @param string $version
     *
     * @return void
     */
    public function setVersion(string $version)
    {
        $this->testConnection->setServerInfo($version);
    }
}
