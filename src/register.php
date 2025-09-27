<?php

use Lagdo\DbAdmin\Driver;

Driver\Driver::registerDriver('mysql',
    function($di, array $options): Driver\DriverInterface {
        $utils = $di->g(Driver\Utils\Utils::class);
        return new Driver\MySql\Driver($utils, $options);
    });
