<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Db\Server as AbstractServer;
use Lagdo\DbAdmin\Driver\Db\StatementInterface;

use function array_key_exists;
use function is_a;
use function intval;
use function preg_match;

class Server extends AbstractServer
{
    /**
     * @inheritDoc
     */
    public function databases(bool $flush)
    {
        // !!! Caching and slow query handling are temporarily disabled !!!
        $query = $this->driver->minVersion(5) ?
            'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME' :
            'SHOW DATABASES';
        return $this->driver->values($query);

        // SHOW DATABASES can take a very long time so it is cached
        // $databases = get_session('dbs');
        // if ($databases === null) {
        //     $query = ($this->driver->minVersion(5)
        //         ? 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME'
        //         : 'SHOW DATABASES'
        //     ); // SHOW DATABASES can be disabled by skip_show_database
        //     $databases = ($flush ? slow_query($query) : $this->driver->values($query));
        //     restart_session();
        //     set_session('dbs', $databases);
        //     stop_session();
        // }
        // return $databases;
    }

    /**
     * @inheritDoc
     */
    public function databaseSize(string $database)
    {
        $statement = $this->driver->execute('SELECT SUM(data_length + index_length) ' .
            'FROM information_schema.tables where table_schema=' . $this->driver->quote($database));
        if (is_a($statement, StatementInterface::class) && ($row = $statement->fetchRow())) {
            return intval($row[0]);
        }
        return 0;
    }

    /**
     * @inheritDoc
     */
    public function databaseCollation(string $database, array $collations)
    {
        $collation = null;
        $create = $this->driver->result('SHOW CREATE DATABASE ' . $this->driver->escapeId($database), 1);
        if (preg_match('~ COLLATE ([^ ]+)~', $create, $match)) {
            $collation = $match[1];
        } elseif (preg_match('~ CHARACTER SET ([^ ]+)~', $create, $match)) {
            // default collation
            $collation = $collations[$match[1]][-1];
        }
        return $collation;
    }

    /**
     * @inheritDoc
     */
    public function engines()
    {
        $engines = [];
        foreach ($this->driver->rows('SHOW ENGINES') as $row) {
            if (preg_match('~YES|DEFAULT~', $row['Support'])) {
                $engines[] = $row['Engine'];
            }
        }
        return $engines;
    }

    /**
     * @inheritDoc
     */
    public function collations()
    {
        $collations = [];
        foreach ($this->driver->rows('SHOW COLLATION') as $row) {
            if ($row['Default']) {
                $collations[$row['Charset']][-1] = $row['Collation'];
            } else {
                $collations[$row['Charset']][] = $row['Collation'];
            }
        }
        ksort($collations);
        foreach ($collations as $key => $val) {
            asort($collations[$key]);
        }
        return $collations;
    }

    /**
     * @inheritDoc
     */
    public function isInformationSchema(string $database)
    {
        return ($this->driver->minVersion(5) && $database == 'information_schema') ||
            ($this->driver->minVersion(5.5) && $database == 'performance_schema');
    }

    /**
     * @inheritDoc
     */
    public function createDatabase(string $database, string $collation)
    {
        $result = $this->driver->execute('CREATE DATABASE ' . $this->driver->escapeId($database) .
            ($collation ? ' COLLATE ' . $this->driver->quote($collation) : ''));
        return $result !== false;
    }

    /**
     * @inheritDoc
     */
    public function dropDatabase(string $database)
    {
        $result = $this->driver->execute('DROP DATABASE ' . $this->driver->escapeId($database));
        return $result !== false;
    }

    /**
     * @inheritDoc
     */
    public function renameDatabase(string $name, string $collation)
    {
        // The feature is not natively provided by latest MySQL versions, thus it is disabled here.
        return false;
        /*$renamed = false;
        if ($this->createDatabase($name, $collation)) {
            $tables = [];
            $views = [];
            foreach ($this->driver->tables() as $table => $type) {
                if ($type == 'VIEW') {
                    $views[] = $table;
                } else {
                    $tables[] = $table;
                }
            }
            $renamed = (!$tables && !$views) || $this->driver->moveTables($tables, $views, $name);
            $this->dropDatabase($renamed ? $this->driver->database() : '');
        }
        return $renamed;*/
    }

    /**
     * @inheritDoc
     */
    public function routineLanguages()
    {
        return []; // 'SQL' not required
    }

    /**
     * @inheritDoc
     */
    public function variables()
    {
        return $this->driver->keyValues('SHOW VARIABLES');
    }

    /**
     * @inheritDoc
     */
    public function processes()
    {
        return $this->driver->rows('SHOW FULL PROCESSLIST');
    }

    /**
     * @inheritDoc
     */
    public function processAttr(array $process, string $key, string $val): string
    {
        $match = array_key_exists('Command', $process) && preg_match('~Query|Killed~', $process['Command']);
        if ($key == 'Info' && $match && $val != '') {
            return '<code>' . $this->util->shortenUtf8($val, 50) . '</code>' . $this->trans->lang('Clone');
        }
        return parent::processAttr($process, $key, $val);
    }

    /**
     * @inheritDoc
     */
    public function statusVariables()
    {
        return $this->driver->keyValues('SHOW STATUS');
    }

    /**
     * @inheritDoc
     */
    // public function killProcess($val)
    // {
    //     return $this->driver->execute('KILL ' . $this->util->number($val));
    // }

    /**
     * @inheritDoc
     */
    // public function maxConnections()
    // {
    //     return $this->driver->result('SELECT @@max_connections');
    // }
}
