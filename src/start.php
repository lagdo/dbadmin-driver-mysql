<?php

if(class_exists(Lagdo\Adminer\DbAdmin::class))
{
    Lagdo\Adminer\DbAdmin::addServer("mysql", Lagdo\DbAdmin\Driver\MySql\Server::class);
}
