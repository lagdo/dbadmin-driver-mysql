<?php

$di = \jaxon()->di();
// Register the database classes in the dependency container
$di->set(Lagdo\DbAdmin\Driver\MySql\Server::class, function($di) {
    return new Lagdo\DbAdmin\Driver\MySql\Server(
        $di->get(Lagdo\Adminer\Driver\DbInterface::class),
        $di->get(Lagdo\Adminer\Driver\UtilInterface::class));
});
$di->alias('adminer_server_mysql', Lagdo\DbAdmin\Driver\MySql\Server::class);
