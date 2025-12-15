<?php

use Lagdo\DbAdmin\Driver\AbstractDriver;
use Lagdo\DbAdmin\Driver\DriverInterface;
use Lagdo\DbAdmin\Driver\MySql\Driver;
use Lagdo\DbAdmin\Driver\Utils\Utils;

AbstractDriver::registerDriver('mysql', fn($di, array $options): DriverInterface =>
    new Driver($di->g(Utils::class), $options));
