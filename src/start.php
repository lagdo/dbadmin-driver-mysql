<?php

$di = \jaxon()->di();
// Register the database classes in the dependency container
$di->set(Lagdo\DbAdmin\Driver\MySql\Driver::class, function($di) {
    $util = $di->get(Lagdo\DbAdmin\Driver\UtilInterface::class);
    $options = $di->get('adminer_config_options');
    return new Lagdo\DbAdmin\Driver\MySql\Driver($util, $options);
});
$di->alias('adminer_driver_mysql', Lagdo\DbAdmin\Driver\MySql\Driver::class);
