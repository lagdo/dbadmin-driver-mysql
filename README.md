[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/lagdo/dbadmin-driver-mysql/badges/quality-score.png?b=main)](https://scrutinizer-ci.com/g/lagdo/dbadmin-driver-mysql/?branch=main)
[![StyleCI](https://styleci.io/repos/400390067/shield?branch=main)](https://styleci.io/repos/400390067)
[![Build Status](https://api.travis-ci.com/lagdo/dbadmin-driver-mysql.svg?branch=main)](https://app.travis-ci.com/github/lagdo/dbadmin-driver-mysql)
[![Coverage Status](https://coveralls.io/repos/github/lagdo/dbadmin-driver-mysql/badge.svg?branch=main)](https://coveralls.io/github/lagdo/dbadmin-driver-mysql?branch=main)

[![Latest Stable Version](https://poser.pugx.org/lagdo/dbadmin-driver-mysql/v/stable)](https://packagist.org/packages/lagdo/dbadmin-driver-mysql)
[![Total Downloads](https://poser.pugx.org/lagdo/dbadmin-driver-mysql/downloads)](https://packagist.org/packages/lagdo/dbadmin-driver-mysql)
[![License](https://poser.pugx.org/lagdo/dbadmin-driver-mysql/license)](https://packagist.org/packages/lagdo/dbadmin-driver-mysql)

DbAdmin drivers for MySQL
=========================

This package is based on [Adminer](https://github.com/vrana/adminer).

It provides MySQL drivers for [Jaxon DbAdmin](https://github.com/lagdo/jaxon-dbadmin), and implements the interfaces defined in [https://github.com/lagdo/dbadmin-driver](https://github.com/lagdo/dbadmin-driver).

It requires either the `php-mysqli` or the `php-pdo_mysql` PHP extension to be installed, and uses the former by default.

**Installation**

Install with Composer.

```
composer require lagdo/dbadmin-driver-mysql
```

**Configuration**

Declare the MySQL servers in the `packages` section on the `Jaxon` config file. Set the `driver` option to `mysql`.

```php
    'app' => [
        'packages' => [
            Lagdo\DbAdmin\App\Package::class => [
                'servers' => [
                    'server_id' => [ // A unique identifier for this server
                        'driver' => 'mysql',
                        'name' => '',     // The name to be displayed in the dashboard UI.
                        'host' => '',     // The database host name or address.
                        'port' => 0,      // The database port. Optional.
                        'username' => '', // The database user credentials.
                        'password' => '', // The database user credentials.
                    ],
                ],
            ],
        ],
    ],
```

Check the [Jaxon DbAdmin](https://github.com/lagdo/jaxon-dbadmin) documentation for more information about the package usage.
