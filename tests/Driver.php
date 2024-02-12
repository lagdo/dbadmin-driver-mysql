<?php

namespace Lagdo\DbAdmin\Driver\MySql\Tests;

use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;
use Lagdo\DbAdmin\Driver\Input;
use Lagdo\DbAdmin\Driver\Fake\DriverTrait;
use Lagdo\DbAdmin\Driver\Fake\Translator;
use Lagdo\DbAdmin\Driver\Fake\Util;
use Lagdo\DbAdmin\Driver\Fake\Connection;

use Lagdo\DbAdmin\Driver\MySql\Driver as MySqlDriver;
use Lagdo\DbAdmin\Driver\MySql\Db\Server;
use Lagdo\DbAdmin\Driver\MySql\Db\Database;
use Lagdo\DbAdmin\Driver\MySql\Db\Table;
use Lagdo\DbAdmin\Driver\MySql\Db\Query;
use Lagdo\DbAdmin\Driver\MySql\Db\Grammar;

class Driver extends MySqlDriver
{
    use DriverTrait;

    /**
     * The constructor
     */
    public function __construct()
    {
        $input = new Input();
        $trans = new Translator();
        $util = new Util($trans, $input);
        parent::__construct($util, $trans, []);
    }

    /**
     * @inheritDoc
     */
    protected function createConnection()
    {
        $this->testConnection = new Connection($this, $this->util, $this->trans, 'test');
        $this->connection = $this->testConnection;
        $this->server = new Server($this, $this->util, $this->trans);
        $this->database = new Database($this, $this->util, $this->trans);
        $this->table = new Table($this, $this->util, $this->trans);
        $this->query = new Query($this, $this->util, $this->trans);
        $this->grammar = new Grammar($this, $this->util, $this->trans);

        return $this->connection;
    }
}
