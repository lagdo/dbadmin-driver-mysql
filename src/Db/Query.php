<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;

use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;

use Lagdo\DbAdmin\Driver\Db\Query as AbstractQuery;

use function count;
use function array_keys;
use function implode;
use function strlen;
use function preg_match;
use function preg_replace;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    protected function limitToOne(string $table, string $query, string $where)
    {
        return $this->driver->limit($query, $where, 1, 0);
    }

    /**
     * @inheritDoc
     */
    public function insert(string $table, array $values)
    {
        if (!empty($values)) {
            return parent::insert($table, $values);
        }
        $result = $this->driver->execute('INSERT INTO ' . $this->driver->table($table) . ' () VALUES ()');
        return $result !== false;
    }

    /**
     * @inheritDoc
     */
    public function insertOrUpdate(string $table, array $rows, array $primary)
    {
        $columns = array_keys(reset($rows));
        $prefix = 'INSERT INTO ' . $this->driver->table($table) . ' (' . implode(', ', $columns) . ') VALUES ';
        $values = [];
        foreach ($columns as $key) {
            $values[$key] = "$key = VALUES($key)";
        }
        $suffix = ' ON DUPLICATE KEY UPDATE ' . implode(', ', $values);
        $values = [];
        $length = 0;
        foreach ($rows as $set) {
            $value = '(' . implode(', ', $set) . ')';
            if (!empty($values) && (strlen($prefix) + $length + strlen($value) + strlen($suffix) > 1e6)) {
                // 1e6 - default max_allowed_packet
                if (!$this->driver->execute($prefix . implode(",\n", $values) . $suffix)) {
                    return false;
                }
                $values = [];
                $length = 0;
            }
            $values[] = $value;
            $length += strlen($value) + 2; // 2 - strlen(",\n")
        }
        $result = $this->driver->execute($prefix . implode(",\n", $values) . $suffix);
        return $result !== false;
    }

    /**
     * @inheritDoc
     */
    public function slowQuery(string $query, int $timeout)
    {
        // $this->connection->timeout = $timeout;
        if ($this->driver->minVersion('5.7.8', '10.1.2')) {
            if (preg_match('~MariaDB~', $this->driver->serverInfo())) {
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
    public function convertSearch(string $idf, array $val, TableFieldEntity $field)
    {
        return (preg_match('~char|text|enum|set~', $field->type) &&
            !preg_match('~^utf8~', $field->collation) && preg_match('~[\x80-\xFF]~', $val['val']) ?
            "CONVERT($idf USING " . $this->driver->charset() . ')' : $idf
        );
    }

    /**
     * @inheritDoc
     */
    public function user()
    {
        return $this->driver->result('SELECT USER()');
    }

    /**
     * @inheritDoc
     */
    public function view(string $name)
    {
        return [
            'name' => $name,
            'type' => 'VIEW',
            'materialized' => false,
            'select' => preg_replace('~^(?:[^`]|`[^`]*`)*\s+AS\s+~isU', '',
                $this->driver->result('SHOW CREATE VIEW ' . $this->driver->table($name), 1)),
        ];
    }

    /**
     * @inheritDoc
     */
    public function lastAutoIncrementId()
    {
        return $this->driver->result('SELECT LAST_INSERT_ID()'); // mysql_insert_id() truncates bigint
    }

    /**
     * @inheritDoc
     */
    public function explain(ConnectionInterface $connection, string $query)
    {
        return $connection->query('EXPLAIN ' . ($this->driver->minVersion(5.1) &&
            !$this->driver->minVersion(5.7) ? 'PARTITIONS ' : '') . $query);
    }

    /**
     * @inheritDoc
     */
    public function countRows(TableEntity $tableStatus, array $where)
    {
        return (!empty($where) || $tableStatus->engine != 'InnoDB' ? null : count($tableStatus->rows));
    }
}
