<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractDatabase;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;

use function addcslashes;
use function array_map;
use function count;
use function implode;
use function preg_match;
use function preg_replace;

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

        $create = $this->_engine()->columnValue("SHOW CREATE DATABASE $database", 1);
        if (!$create) {
            return '';
        }

        $this->_statement()->setUtf8mb4($create);
        $drop = $style !== 'DROP+CREATE' ? '' : "DROP DATABASE IF EXISTS $database;\n";
        return "{$drop}{$create};\n";
    }

    /**
     * @inheritDoc
     */
    public function getUseDatabaseQuery(string $database, string $style = ''): string
    {
        $name = $this->_statement()->escapeId($database);
        return $this->getInitDatabaseQuery($name, $style) . "USE $name;";
    }

    /**
     * @inheritDoc
     */
    public function getCreateDatabaseQuery(string $database, string $collation): string
    {
        return 'CREATE DATABASE ' . $this->_statement()->escapeId($database) .
            ($collation ? ' COLLATE ' . $this->_engine()->quote($collation) : '');
    }

    /**
     * @inheritDoc
     */
    public function getDropDatabaseQuery(string $database): string
    {
        return 'DROP DATABASE ' . $this->_statement()->escapeId($database);
    }

    /**
     * @inheritDoc
     */
    public function getDropViewsQueries(array $views): array
    {
        return [
            'DROP VIEW ' . implode(', ',
                array_map($this->_statement()->escapeTableName(...), $views)),
        ];
    }

    /**
     * @inheritDoc
     */
    public function getDropTablesQueries(array $tables): array
    {
        return [
            'DROP TABLE ' . implode(', ',
                array_map($this->_statement()->escapeTableName(...), $tables)),
        ];
    }

    /**
     * @inheritDoc
     */
    public function getTruncateTablesQueries(array $tables): array
    {
        return array_map(fn(string $table) => 'TRUNCATE TABLE ' .
            $this->_statement()->escapeTableName($table), $tables);
    }

    /**
     * @inheritDoc
     */
    public function getExportTableQueries(string $table, bool $autoIncrement, string $style): string
    {
        $tableName = $this->_statement()->escapeTableName($table);
        $query = $this->_engine()->columnValue("SHOW CREATE TABLE $tableName", 1);
        if (!$autoIncrement) {
            //! skip comments
            $query = preg_replace('~ AUTO_INCREMENT=\d+~', '', $query);
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function getAlterIndexQueries(string $table, array $alter, array $drop): array
    {
        $dropClauses = array_map(fn(IndexDto $index) =>
            'DROP INDEX ' . $this->_statement()->escapeId($index->name), $drop);
        $alterClauses = array_map(function(IndexDto $index) {
            $indexType = $index->type === 'PRIMARY' ? 'PRIMARY KEY' :  $index->type;
            if ($index->name !== '') {
                $indexType .= ' ' . $this->_statement()->escapeId($index->name);
            }
            $columns = implode(', ', $index->columns);
            return "ADD $indexType ($columns)";
        }, $alter);
        $clauses = [...$dropClauses, ...$alterClauses];

        if (count($clauses) === 0) {
            return [];
        }

        $tableName = $this->_statement()->escapeTableName($table);
        return ["ALTER TABLE $tableName " . implode(', ', $clauses)];
    }

    /**
     * @inheritDoc
     */
    public function getTruncateTableQuery(string $table): string
    {
        return "TRUNCATE " . $this->_statement()->escapeTableName($table);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTriggerQuery(string $table): string
    {
        $tableName = $this->_engine()->quote(addcslashes($table, "%_\\"));
        $triggers = $this->_engine()->rows("SHOW TRIGGERS LIKE $tableName");
        $queries = array_map(function(array $row) {
            $trigger = $this->_statement()->escapeId($row['Trigger']);
            $triggerTable = $this->_statement()->escapeTableName($row['Table']);
            return "
CREATE TRIGGER $trigger {$row['Timing']} {$row['Event']} ON $triggerTable FOR EACH ROW
{$row['Statement']}";
        }, $triggers);

        return implode(";;\n", $queries);
    }
}
