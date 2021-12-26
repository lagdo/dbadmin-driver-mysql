<?php

namespace Lagdo\DbAdmin\Driver\MySql\Tests;

use Lagdo\DbAdmin\Driver\Db\Connection as AbstractConnection;

class Connection extends AbstractConnection
{
    /**
     * @var string
     */
    protected $serverInfo = '';

    /**
     * @param string $serverInfo
     *
     * @return void
     */
    public function setServerInfo(string $serverInfo)
    {
        $this->serverInfo = $serverInfo;
    }

    /**
     * @inheritDoc
     */
    public function serverInfo()
    {
        return $this->serverInfo;
    }

    /**
     * @inheritDoc
     */
    public function open(string $database, string $schema = '')
    {
        // TODO: Implement open() method.
    }

    /**
     * @inheritDoc
     */
    public function query(string $query, bool $unbuffered = false)
    {
        // TODO: Implement query() method.
    }

    /**
     * @inheritDoc
     */
    public function result(string $query, int $field = -1)
    {
        // TODO: Implement result() method.
    }

    /**
     * @inheritDoc
     */
    public function multiQuery(string $query)
    {
        // TODO: Implement multiQuery() method.
    }

    /**
     * @inheritDoc
     */
    public function storedResult()
    {
        // TODO: Implement storedResult() method.
    }

    /**
     * @inheritDoc
     */
    public function nextResult()
    {
        // TODO: Implement nextResult() method.
    }
}
