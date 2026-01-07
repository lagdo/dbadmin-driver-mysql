<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Db\AbstractTable;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\PartitionEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;

use function array_flip;
use function array_pad;
use function array_map;
use function explode;
use function ltrim;
use function pack;
use function preg_match;
use function preg_match_all;
use function preg_replace;
use function preg_replace_callback;
use function str_replace;
use function stripslashes;

class Table extends AbstractTable
{
    /**
     * @inheritDoc
     */
    public function supportForeignKeys(TableEntity $tableStatus): bool
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
    public function foreignKeys(string $table): array
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

    /**
     * @param array $row
     * @param array $matchType
     *
     * @return mixed
     */
    private function getRowDefaultValue(array $row, array $matchType): mixed
    {
        $isMaria = $this->driver->flavor() === 'maria';

        $default = $row["COLUMN_DEFAULT"] ?? '';
        if ($default === "") {
            return $default;
        }

        $isText = preg_match('~text|json~', $matchType[1]);
        if (!$isMaria && $isText) {
            // default value a'b of text column is stored as _utf8mb4\'a\\\'b\' in MySQL
            $default = preg_replace("~^(_\w+)?('.*')$~", '\2', stripslashes($default));
        }

        if ($isMaria || $isText) {
            $callback = fn($match) => stripslashes(str_replace("''", "'", $match[1]));
            $default = $default === "NULL" ? null :
                preg_replace_callback("~^'(.*)'$~", $callback, $default);
        }

        if (!$isMaria && preg_match('~binary~', $matchType[1]) &&
            preg_match('~^0x(\w*)$~', $default, $match)) {
            $default = pack("H*", $match[1]);
        }

        return $default;
    }

    /**
     * @param array $row
     *
     * @return TableFieldEntity
     */
    private function makeTableFieldEntity(array $row): TableFieldEntity
    {
        $field = new TableFieldEntity();

        $field->fullType = $row["COLUMN_TYPE"];
        $extra = $row["EXTRA"];

        // https://mariadb.com/kb/en/library/show-columns/
        // https://github.com/vrana/adminer/pull/359#pullrequestreview-276677186
        preg_match('~^(VIRTUAL|PERSISTENT|STORED)~', $extra, $generated);
        preg_match('~^([^( ]+)(?:\((.+)\))?( unsigned)?( zerofill)?$~',
            $field->fullType, $matchType);

        $field->name = $row["COLUMN_NAME"];
        $field->fullType = $row["COLUMN_TYPE"];
        $field->type = $matchType[1] ?? '';
        $field->length = $matchType[2] ?? '';
        $field->unsigned = ltrim(($matchType[3] ?? '') . ($matchType[4] ?? ''));
        $field->nullable = $row["IS_NULLABLE"] === "YES";
        $field->autoIncrement = $extra === "auto_increment";
        $field->collation = $row["COLLATION_NAME"] ?? '';
        $field->comment = $row["COLUMN_COMMENT"] ?? '';
        $field->primary = $row["COLUMN_KEY"] === "PRI";
 
        //! available since MySQL 5.1.23
        $field->onUpdate = preg_match('~\bon update (\w+)~i', $extra, $match) ? $match[1] : "";
        $privileges = $row["PRIVILEGES"] ?? '';
        $field->privileges = array_flip(explode(",", "$privileges,where,order"));

        $isMaria = $this->driver->flavor() === 'maria';
        $defaultValue = $this->getRowDefaultValue($row, $matchType);
        $generation = $row["GENERATION_EXPRESSION"] ?? '';
        $field->default = !$generated ? $defaultValue : ($isMaria ? $generation : stripslashes($generation));

        $generated = $generated[1] ?? '';
        $field->generated = $generated === "PERSISTENT" ? "STORED" : $generated;

        return $field;
    }

    /**
     * @inheritDoc
     */
    public function fields(string $table): array
    {
        $fields = [];
        $tableName = $this->driver->quote($table);
        $query = "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE()
AND TABLE_NAME = $tableName ORDER BY ORDINAL_POSITION";
        $rows = $this->driver->rows($query);
        foreach ($rows as $row) {
            $field = $this->makeTableFieldEntity($row);
            $fields[$field->name] = $field;
        }

        return $fields;
    }

    /**
     * @param bool $fast
     * @param string $table
     *
     * @return array
     */
    private function queryStatus(bool $fast, string $table = ''): array
    {
        // Todo: use match
        $query = ($fast && $this->driver->minVersion(5)) ?
            "SELECT TABLE_NAME AS Name, ENGINE AS Engine, TABLE_COMMENT AS Comment " .
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() " .
            ($table != "" ? "AND TABLE_NAME = " . $this->driver->quote($table) : "ORDER BY Name") :
            "SHOW TABLE STATUS" . ($table != "" ? " LIKE " . $this->driver->quote(addcslashes($table, "%_\\")) : "");
        return $this->driver->rows($query);
    }

    /**
     * @param array $row
     *
     * @return TableEntity
     */
    private function makeStatus(array $row): TableEntity
    {
        $status = new TableEntity($row['Name']);
        $status->engine = $row['Engine'];
        if ($row["Engine"] == "InnoDB") {
            // ignore internal comment, unnecessary since MySQL 5.1.21
            $status->comment = preg_replace('~(?:(.+); )?InnoDB free: .*~', '\1', $row["Comment"]);
        }
        // if (!isset($row["Engine"])) {
        //     $row["Comment"] = "";
        // }

        return $status;
    }

    /**
     * @inheritDoc
     */
    public function tableStatus(string $table, bool $fast = false): TableEntity|null
    {
        $rows = $this->queryStatus($fast, $table);
        if (!($row = reset($rows))) {
            return null;
        }
        return $this->makeStatus($row);
    }

    /**
     * @inheritDoc
     */
    public function tableStatuses(bool $fast = false): array
    {
        $tables = [];
        $rows = $this->queryStatus($fast);
        foreach ($rows as $row) {
            $tables[$row["Name"]] = $this->makeStatus($row);
        }
        return $tables;
    }

    /**
     * @inheritDoc
     */
    public function tableNames(): array
    {
        $tables = [];
        $rows = $this->queryStatus(true);
        foreach ($rows as $row) {
            $tables[] = $row["Name"];
        }
        return $tables;
    }

    /**
     * @inheritDoc
     */
    public function isView(TableEntity $tableStatus): bool
    {
        return $tableStatus->engine === null;
    }

    /**
     * @param array $row
     *
     * @return string
     */
    private function getTableIndexType(array $row): string
    {
        $name = $row['Key_name'];
        if ($name === 'PRIMARY') {
            return 'PRIMARY';
        }
        if ($row['Index_type'] === 'FULLTEXT') {
            return 'FULLTEXT';
        }
        if (!$row['Non_unique']) {
            return 'UNIQUE';
        }
        if ($row['Index_type'] === 'SPATIAL') {
            return 'SPATIAL';
        }
        return 'INDEX';
    }

    /**
     * @param array $row
     *
     * @return IndexEntity
     */
    private function makeTableIndex(array $row): IndexEntity
    {
        $index = new IndexEntity();

        $index->type = $this->getTableIndexType($row);
        $index->columns[] = $row['Column_name'];
        $index->lengths[] = ($row['Index_type'] == 'SPATIAL' ? null : $row['Sub_part']);
        $index->descs[] = null;

        return $index;
    }

    /**
     * @inheritDoc
     */
    public function indexes(string $table): array
    {
        $indexes = [];
        foreach ($this->driver->rows('SHOW INDEX FROM ' . $this->driver->escapeTableName($table)) as $row) {
            $indexes[$row['Key_name']] = $this->makeTableIndex($row);
        }
        return $indexes;
    }

    /**
     * @inheritDoc
     */
    public function trigger(string $name, string $table = ''): TriggerEntity|null
    {
        if ($name == "") {
            return null;
        }
        $rows = $this->driver->rows("SHOW TRIGGERS WHERE `Trigger` = " . $this->driver->quote($name));
        if (!($row = reset($rows))) {
            return null;
        }
        return new TriggerEntity($row["Timing"], $row["Event"], '', '', $row["Trigger"]);
    }

    /**
     * @inheritDoc
     */
    public function triggers(string $table): array
    {
        $triggers = [];
        foreach ($this->driver->rows("SHOW TRIGGERS LIKE " . $this->driver->quote(addcslashes($table, "%_\\"))) as $row) {
            $triggers[$row["Trigger"]] = new TriggerEntity($row["Timing"], $row["Event"], '', '', $row["Trigger"]);
        }
        return $triggers;
    }

    /**
     * @inheritDoc
     */
    public function triggerOptions(): array
    {
        return [
            "Timing" => ["BEFORE", "AFTER"],
            "Event" => ["INSERT", "UPDATE", "DELETE"],
            "Type" => ["FOR EACH ROW"],
        ];
    }

    /**
     * @inheritDoc
     */
    public function tableHelp(string $name): string
    {
        $isMaria = $this->driver->flavor() === 'maria';
        if ($this->driver->isInformationSchema($this->driver->database())) {
            return strtolower(($isMaria ? "information-schema-$name-table/" :
                    str_replace("_", "-", $name) . "-table.html"));
        }
        if ($this->driver->database() == "mysql") {
            return $isMaria ? "mysql$name-table/" : "system-database.html"; //! more precise link
        }
        return '';
    }
}
