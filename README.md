DbAdmin drivers for MySQL
=========================

This package is based on [Adminer](https://github.com/vrana/adminer).

It provides MySQL drivers for [Jaxon Adminer](https://github.com/lagdo/jaxon-dbadmin), and implements the interfaces defined in [https://github.com/lagdo/dbadmin-driver](https://github.com/lagdo/dbadmin-driver).

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
            Lagdo\DbAdmin\Package::class => [
                'servers' => [
                    'server_id' => [
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

Check the [Jaxon Adminer](https://github.com/lagdo/jaxon-dbadmin) documentation for more information about the package usage.
