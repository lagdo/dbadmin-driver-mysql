<?php

$di = \jaxon()->di();
// Register the database classes in the dependency container
$di->auto(Lagdo\DbAdmin\Driver\MySql\Server::class);
$di->alias('adminer_server_mysql', Lagdo\DbAdmin\Driver\MySql\Server::class);
