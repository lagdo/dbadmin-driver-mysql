<?php

namespace Lagdo\DbAdmin\Support\MySql\Grammar;

use Lagdo\DbAdmin\Support\Db\Engine\Grammar\AbstractDatabase;

use function array_map;
use function implode;
use function in_array;
use function preg_match;

class Database extends AbstractDatabase
{
    /**
     * @param string $database
     * @param string $style
     *
     * @return string
     */
    private function getInitDatabaseQuery(string $database, string $style = ''): string
    {
        if (!preg_match('~CREATE~', $style)) {
            return '';
        }
        $create = $this->driver->result("SHOW CREATE DATABASE $database", 1);
        if (!$create) {
            return '';
        }

        $this->grammar->setUtf8mb4($create);
        $drop = $style !== 'DROP+CREATE' ? '' : "DROP DATABASE IF EXISTS $database;\n";
        return "{$drop}{$create};\n";
    }

    /**
     * @inheritDoc
     */
    public function getUseDatabaseQuery(string $database, string $style = ''): string
    {
        $name = $this->grammar->escapeId($database);
        return $this->getInitDatabaseQuery($name, $style) . "USE $name;";
    }

    /**
     * @inheritDoc
     */
    public function getCreateDatabaseQuery(string $database, string $collation): string
    {
        return 'CREATE DATABASE ' . $this->grammar->escapeId($database) .
            ($collation ? ' COLLATE ' . $this->driver->quote($collation) : '');
    }

    /**
     * @inheritDoc
     */
    public function getDropDatabaseQuery(string $database): string
    {
        return 'DROP DATABASE ' . $this->grammar->escapeId($database);
    }

    /**
     * @inheritDoc
     */
    public function getAutoIncrementModifier(): string
    {
        $autoIncrementIndex = " PRIMARY KEY";
        // don't overwrite primary key by auto increment
        $table = $this->utils->input->getTable();
        $fields = $this->utils->input->getFields();
        $autoIncrementField = $this->utils->input->getAutoIncrementField();
        if ($table != "" && $autoIncrementField) {
            foreach ($this->driver->indexes($table) as $index) {
                if (in_array($fields[$autoIncrementField]["orig"], $index->columns, true)) {
                    $autoIncrementIndex = "";
                    break;
                }
                if ($index->type == "PRIMARY") {
                    $autoIncrementIndex = " UNIQUE";
                }
            }
        }
        return " AUTO_INCREMENT$autoIncrementIndex";
    }

    /**
     * @inheritDoc
     */
    public function getDropViewsQueries(array $views): array
    {
        return [
            'DROP VIEW ' . implode(', ', array_map($this->grammar->escapeTableName(...), $views)),
        ];
    }

    /**
     * @inheritDoc
     */
    public function getDropTablesQueries(array $tables): array
    {
        return [
            'DROP TABLE ' . implode(', ', array_map($this->grammar->escapeTableName(...), $tables)),
        ];
    }

    /**
     * @inheritDoc
     */
    public function getTruncateTablesQueries(array $tables): array
    {
        return array_map(fn(string $table) =>
            'TRUNCATE TABLE ' . $this->grammar->escapeTableName($table), $tables);
    }
}
