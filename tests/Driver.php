<?php

namespace Lagdo\DbAdmin\Support\MySql\Tests;

use Lagdo\DbAdmin\Support\Db\Engine\Driver\AbstractConnection;
use Lagdo\DbAdmin\Support\Db\Fake\DriverTrait;
use Lagdo\DbAdmin\Support\Db\Fake\Translator;
use Lagdo\DbAdmin\Support\Db\Fake\Connection;
use Lagdo\DbAdmin\Support\MySql\Driver as MySqlDriver;
use Lagdo\DbAdmin\Support\Utils\Str;
use Lagdo\DbAdmin\Support\Utils\Utils;
use Lagdo\DbAdmin\Support\Utils\Input;

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
    public function createConnection(array $options): AbstractConnection|null
    {
        $this->connection = new Connection($this, $this->grammar(), $this->utils, $options, 'test');
        $this->testConnection = $this->connection;

        return $this->connection;
    }
}
