<?php

namespace Lagdo\DbAdmin\Driver\MySql\Engine;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Engine\AbstractQuery;

use function array_keys;
use function count;
use function implode;
use function preg_match;
use function preg_replace;
use function strlen;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    // public function insertOrUpdate(string $table, array $rows, array $primary): bool
    // {
    //     $columns = array_keys(reset($rows));
    //     $prefix = 'INSERT INTO ' . $this->_statement()->escapeTableName($table) . ' (' . implode(', ', $columns) . ') VALUES ';
    //     $values = [];
    //     foreach ($columns as $key) {
    //         $values[$key] = "$key = VALUES($key)";
    //     }
    //     $suffix = ' ON DUPLICATE KEY UPDATE ' . implode(', ', $values);
    //     $values = [];
    //     $length = 0;
    //     foreach ($rows as $set) {
    //         $value = '(' . implode(', ', $set) . ')';
    //         if (!empty($values) && (strlen($prefix) + $length + strlen($value) + strlen($suffix) > 1e6)) {
    //             // 1e6 - default max_allowed_packet
    //             if (!$this->_engine()->execute($prefix . implode(",\n", $values) . $suffix)) {
    //                 return false;
    //             }
    //             $values = [];
    //             $length = 0;
    //         }
    //         $values[] = $value;
    //         $length += strlen($value) + 2; // 2 - strlen(",\n")
    //     }
    //     $result = $this->_engine()->execute($prefix . implode(",\n", $values) . $suffix);
    //     return $result !== false;
    // }

    /**
     * @inheritDoc
     */
    public function slowQuery(string $query, int $timeout): string|null
    {
        // $this->connection->timeout = $timeout;
        if ($this->_engine()->minVersion('5.7.8', '10.1.2')) {
            if (preg_match('~MariaDB~', $this->_engine()->serverInfo())) {
                return "SET STATEMENT max_statement_time=$timeout FOR $query";
            } elseif (preg_match('~^(SELECT\b)(.+)~is', $query, $match)) {
                return "$match[1] /*+ MAX_EXECUTION_TIME(" . ($timeout * 1000) . ") */ $match[2]";
            }
        }
        return null;
    }

    /**
     * @inheritDoc
     */
    public function convertSearch(string $idf, array $value, ColumnDto $column): string
    {
        return (preg_match('~char|text|enum|set~', $column->type) &&
            !preg_match('~^utf8~', $column->collation) &&
            preg_match('~[\x80-\xFF]~', $value['val']) ?
            "CONVERT($idf USING " . $this->_engine()->charset() . ')' : $idf
        );
    }

    /**
     * @inheritDoc
     */
    public function view(string $name): array
    {
        return [
            'name' => $name,
            'type' => 'VIEW',
            'materialized' => false,
            'select' => preg_replace('~^(?:[^`]|`[^`]*`)*\s+AS\s+~isU', '',
                $this->_engine()->columnValue('SHOW CREATE VIEW ' . $this->_statement()->escapeTableName($name), 1)),
        ];
    }

    /**
     * @inheritDoc
     */
    public function lastAutoIncrementId(): string
    {
        return $this->_engine()->columnValue('SELECT LAST_INSERT_ID()'); // mysql_insert_id() truncates bigint
    }

    /**
     * @inheritDoc
     */
    public function countRows(TableDto $tableStatus, array $where): int|null
    {
        return !empty($where) || $tableStatus->engine != 'InnoDB' ? null : $tableStatus->rowCount;
    }
}
