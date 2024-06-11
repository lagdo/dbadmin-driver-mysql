<?php

use Lagdo\DbAdmin\Driver\MySql\Tests\Driver;

use Behat\Behat\Context\Context;
use PHPUnit\Framework\Assert;

class ServerContext implements Context
{
    /**
     * @var Driver
     */
    protected $driver;

    /**
     * @var int
     */
    protected $dbSize;

    /**
     * The constructor
     */
    public function __construct()
    {
        $this->driver = new Driver();
    }

    /**
     * @Given The default server is connected
     */
    public function connectToTheDefaultServer()
    {
        // Nothing to do
    }

    /**
     * @Given The driver version is :version
     */
    public function setTheDriverVersion(string $version)
    {
        $this->driver->setVersion($version);
    }

    /**
     * @When I read the database list
     */
    public function getTheDatabaseList()
    {
        $this->driver->databases(true);
    }

    /**
     * @Then The select schema name query is executed
     */
    public function checkTheSelectSchemaNameQueryIsExecuted()
    {
        $queries = $this->driver->queries();
        Assert::assertGreaterThan(0, count($queries));
        Assert::assertEquals($queries[0]['query'], 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME');
    }

    /**
     * @Then The show databases query is executed
     */
    public function checkTheShowDatabasesQueryIsExecuted()
    {
        $queries = $this->driver->queries();
        Assert::assertGreaterThan(0, count($queries));
        Assert::assertEquals($queries[0]['query'], 'SHOW DATABASES');
    }

    /**
     * @Given The next request returns :status
     */
    public function setTheNextDatabaseRequestStatus(bool $status)
    {
        $this->driver->connection()->setNextResultStatus($status);
    }

    /**
     * @Given The next request returns database size of :size
     */
    public function setTheNextDatabaseRequestValueOfSize(int $size)
    {
        $this->driver->connection()->setNextResultValues([['size' => $size]]);
    }

    /**
     * @When I read the database :database size
     */
    public function getTheDatabaseSize(string $database)
    {
        $this->dbSize = $this->driver->databaseSize($database);
    }

    /**
     * @Then The size of the database is :size
     */
    public function checkTheDatabaseSize(int $size)
    {
        Assert::assertEquals($size, $this->dbSize);
    }

    /**
     * @Then The get database size query is executed on :database
     */
    public function checkIfTheGetDatabaseSizeQueryIsExecuted(string $database)
    {
        $queries = $this->driver->queries();
        $count = count($queries);
        Assert::assertGreaterThan(0, $count);
        $query = "SELECT SUM(data_length + index_length) " .
            "FROM information_schema.tables where table_schema=$database";
        Assert::assertEquals($queries[$count - 1]['query'], $query);
    }
}
