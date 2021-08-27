<?php

if(class_exists(Lagdo\Adminer\DbAdmin::class))
{
    Lagdo\Adminer\DbAdmin::addServer("mysql", Lagdo\Adminer\Driver\MySql\Server::class);
}
