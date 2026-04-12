<?php

use Lagdo\DbAdmin\Driver\Driver;
use Lagdo\DbAdmin\Driver\MySql\Engine;
use Lagdo\DbAdmin\Driver\MySql\Statement;
use Lagdo\DbAdmin\Driver\Utils\Utils;

Driver::registerBuilder('mysql', fn(Utils $utils, array $options) =>
    [new Engine($utils, $options), new Statement($utils)]);
