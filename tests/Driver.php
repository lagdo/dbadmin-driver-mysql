<?php

namespace Lagdo\DbAdmin\Driver\MySql\Tests;

use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;
use Lagdo\DbAdmin\Driver\Utils\Str;
use Lagdo\DbAdmin\Driver\Utils\Utils;
use Lagdo\DbAdmin\Driver\Utils\Input;
use Lagdo\DbAdmin\Driver\Fake\DriverTrait;
use Lagdo\DbAdmin\Driver\Fake\Translator;
use Lagdo\DbAdmin\Driver\Fake\Connection;
use Lagdo\DbAdmin\Driver\MySql\Db\Server;
use Lagdo\DbAdmin\Driver\MySql\Db\Database;
use Lagdo\DbAdmin\Driver\MySql\Db\Table;
use Lagdo\DbAdmin\Driver\MySql\Db\Query;
use Lagdo\DbAdmin\Driver\MySql\Db\Grammar;
use Lagdo\DbAdmin\Driver\MySql\Driver as MySqlDriver;

class Driver extends MySqlDriver
{
    use DriverTrait;

    /**
     * The constructor
     */
    public function __construct()
    {
        $utils = new Utils(new Translator(), new Input(), new Str());
        parent::__construct($utils, []);
    }

    /**
     * @inheritDoc
     */
    public function createConnection(array $options): ConnectionInterface|null
    {
        $this->testConnection = new Connection($this, $this->utils, $options, 'test');
        $this->connection = $this->testConnection;
        $this->server = new Server($this, $this->utils);
        $this->database = new Database($this, $this->utils);
        $this->table = new Table($this, $this->utils);
        $this->query = new Query($this, $this->utils);
        $this->grammar = new Grammar($this, $this->utils);

        return $this->connection;
    }
}
