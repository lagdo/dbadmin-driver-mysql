<?php

use Lagdo\DbAdmin\Driver\Input;
use Lagdo\DbAdmin\Driver\MySql\Tests\Driver;
use Lagdo\DbAdmin\Driver\MySql\Tests\Translator;
use Lagdo\DbAdmin\Driver\MySql\Tests\Util;

use Behat\Behat\Context\Context;
use PHPUnit\Framework\Assert;

class FeatureContext implements Context
{
    /**
     * @var Driver
     */
    protected $driver;

    /**
     * The constructor
     */
    public function __construct()
    {
        $input = new Input();
        $trans = new Translator();
        $util = new Util($trans, $input);
        $this->driver = new Driver($util, $trans, []);
    }

    /**
     * @Given I am connected to the default server
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
     * @When I get the database list
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
        Assert::assertGreaterThan( 0, count($queries));
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
}
