<?php

use Lagdo\DbAdmin\Support\AbstractDriver;
use Lagdo\DbAdmin\Support\DriverInterface;
use Lagdo\DbAdmin\Support\MySql\Driver;
use Lagdo\DbAdmin\Support\Utils\Utils;

AbstractDriver::registerDriver('mysql', fn($di, array $options): DriverInterface =>
    new Driver($di->g(Utils::class), $options));
