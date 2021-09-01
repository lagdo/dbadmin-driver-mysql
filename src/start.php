<?php

// Register the database classes in the dependency container
\jaxon()->di()->set('adminer_server_mysql', function($di) {
    return new Lagdo\DbAdmin\Driver\MySql\Server(
        $di->get(Lagdo\Adminer\Driver\DbInterface::class),
        $di->get(Lagdo\Adminer\Driver\UtilInterface::class));
});
