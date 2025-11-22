<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db\Traits;

use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\PartitionEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;

use function array_pad;
use function array_map;
use function preg_match;
use function preg_match_all;

trait TableTrait
{
    abstract public function tableStatuses(bool $fast = false);
    abstract public function fields(string $table);

    /**
     * @inheritDoc
     */
    public function supportForeignKeys(TableEntity $tableStatus)
    {
        return preg_match('~InnoDB|IBMDB2I~i', $tableStatus->engine ?? '')
            || (preg_match('~NDB~i', $tableStatus->engine ?? '')
            && $this->driver->minVersion(5.6));
    }

    /**
     * @param string $tableName
     *
     * @return TableFieldEntity|null
     */
    private function getTablePrimaryKeyField(string $tableName)
    {
        $pkField = null;
        foreach ($this->fields($tableName) as $field) {
            if ($field->primary) {
                if ($pkField !== null) {
                    // No multi column primary key
                    return null;
                }
                $pkField = $field;
            }
        }
        return $pkField;
    }

    /**
     * @inheritDoc
     */
    public function referencableTables(string $table)
    {
        $fields = []; // table_name => [field]
        foreach ($this->tableStatuses(true) as $tableName => $tableStatus) {
            if ($tableName !== $table && $this->supportForeignKeys($tableStatus) &&
                ($field = $this->getTablePrimaryKeyField($tableName)) !== null) {
                $fields[$tableName] = $field;
            }
        }
        return $fields;
    }

    /**
     * @param array $match
     *
     * @return ForeignKeyEntity
     */
    private function makeTableForeignKey(array $match): ForeignKeyEntity
    {
        $match = array_pad($match, 8, '');

        $pattern = '(?:`(?:[^`]|``)+`|"(?:[^"]|"")+")';
        preg_match_all("~$pattern~", $match[2], $source);
        preg_match_all("~$pattern~", $match[5], $target);

        $foreignKey = new ForeignKeyEntity();

        $foreignKey->database = $this->driver->unescapeId($match[4] != "" ? $match[3] : $match[4]);
        $foreignKey->table = $this->driver->unescapeId($match[4] != "" ? $match[4] : $match[3]);
        $foreignKey->source = array_map(function ($idf) {
            return $this->driver->unescapeId($idf);
        }, $source[0]);
        $foreignKey->target = array_map(function ($idf) {
            return $this->driver->unescapeId($idf);
        }, $target[0]);
        $foreignKey->onDelete = $match[6] ?: "RESTRICT";
        $foreignKey->onUpdate = $match[7] ?: "RESTRICT";

        return $foreignKey;
    }

    /**
     * @inheritDoc
     */
    public function foreignKeys(string $table)
    {
        static $pattern = '(?:`(?:[^`]|``)+`|"(?:[^"]|"")+")';
        $foreignKeys = [];
        $onActions = $this->driver->actions();
        $createTable = $this->driver->result("SHOW CREATE TABLE " .
            $this->driver->escapeTableName($table), 1);
        if ($createTable) {
            preg_match_all("~CONSTRAINT ($pattern) FOREIGN KEY ?\\(((?:$pattern,? ?)+)\\) REFERENCES " .
                "($pattern)(?:\\.($pattern))? \\(((?:$pattern,? ?)+)\\)(?: ON DELETE ($onActions))" .
                "?(?: ON UPDATE ($onActions))?~", $createTable, $matches, PREG_SET_ORDER);

            foreach ($matches as $match) {
                $foreignKeys[$this->driver->unescapeId($match[1])] = $this->makeTableForeignKey($match);
            }
        }
        return $foreignKeys;
    }

    /**
     * @inheritDoc
     */
    public function checkConstraints(TableEntity $status): array
    {
        // From driver.inc.php
        $database = $this->driver->quote($this->driver->database());
        $table = $this->driver->quote($status->name);
        // MariaDB contains CHECK_CONSTRAINTS.TABLE_NAME, MySQL and PostrgreSQL not
        $query = "SELECT c.CONSTRAINT_NAME, CHECK_CLAUSE
FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS c JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
ON c.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA AND c.CONSTRAINT_NAME = t.CONSTRAINT_NAME
WHERE c.CONSTRAINT_SCHEMA = $database AND t.TABLE_NAME = $table
AND CHECK_CLAUSE NOT LIKE '% IS NOT NULL'";
        return $this->driver->keyValues($query); // ignore default IS NOT NULL checks in PostrgreSQL
    }

    /**
     * @inheritDoc
     */
    public function partitionsInfo(string $table): PartitionEntity|null
    {
        $database = $this->driver->quote($this->driver->database());
        $tableName = $this->driver->quote($table);
        $from = "FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = $database AND TABLE_NAME = $tableName";
        $query = "SELECT PARTITION_METHOD, PARTITION_EXPRESSION, PARTITION_ORDINAL_POSITION $from
ORDER BY PARTITION_ORDINAL_POSITION DESC LIMIT 1";
        $result = $this->driver->execute($query)?->fetchRow();
        if (!$result) {
            return null;
        }

        [$fields, $strategy, $partitions] = $result;
        $entity = new PartitionEntity($strategy, $fields);
        $entity->partitions = $partitions;

        $query = "SELECT PARTITION_NAME, PARTITION_DESCRIPTION $from
AND PARTITION_NAME != '' ORDER BY PARTITION_ORDINAL_POSITION";
        $partition = $this->driver->keyValues($query);
        $entity->names = array_keys($partition);
        $entity->values = array_values($partition);

        return $entity;
    }
}
