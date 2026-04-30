<?php

namespace Lagdo\DbAdmin\Driver\MySql\Engine;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\ForeignKeyDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\PartitionDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TriggerDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Engine\AbstractTable;

use function addcslashes;
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
    public function supportForeignKeys(TableDto $tableStatus): bool
    {
        return preg_match('~InnoDB|IBMDB2I~i', $tableStatus->engine ?? '')
            || (preg_match('~NDB~i', $tableStatus->engine ?? '')
            && $this->_engine()->minVersion(5.6));
    }

    /**
     * @param string $tableName
     *
     * @return ColumnDto|null
     */
    private function getTablePrimaryKeyColumn(string $tableName): ColumnDto|null
    {
        $pkColumn = null;
        foreach ($this->columns($tableName) as $column) {
            if ($column->primary) {
                if ($pkColumn !== null) {
                    // No multi column primary key
                    return null;
                }
                $pkColumn = $column;
            }
        }
        return $pkColumn;
    }

    /**
     * @param array $match
     *
     * @return ForeignKeyDto
     */
    private function makeTableForeignKey(array $match): ForeignKeyDto
    {
        $match = array_pad($match, 8, '');

        $pattern = '(?:`(?:[^`]|``)+`|"(?:[^"]|"")+")';
        preg_match_all("~$pattern~", $match[2], $source);
        preg_match_all("~$pattern~", $match[5], $target);

        $foreignKey = new ForeignKeyDto();

        $foreignKey->database = $this->_statement()->unescapeId($match[4] != '' ? $match[3] : $match[4]);
        $foreignKey->table = $this->_statement()->unescapeId($match[4] != '' ? $match[4] : $match[3]);
        $foreignKey->source = array_map($this->_statement()->unescapeId(...), $source[0]);
        $foreignKey->target = array_map($this->_statement()->unescapeId(...), $target[0]);
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
        $onActions = $this->_engine()->actions();
        $createTable = $this->_engine()->result("SHOW CREATE TABLE " .
            $this->_statement()->escapeTableName($table), 1);
        if ($createTable) {
            preg_match_all("~CONSTRAINT ($pattern) FOREIGN KEY ?\\(((?:$pattern,? ?)+)\\) REFERENCES " .
                "($pattern)(?:\\.($pattern))? \\(((?:$pattern,? ?)+)\\)(?: ON DELETE ($onActions))" .
                "?(?: ON UPDATE ($onActions))?~", $createTable, $matches, PREG_SET_ORDER);

            foreach ($matches as $match) {
                $foreignKeys[$this->_statement()->unescapeId($match[1])] = $this->makeTableForeignKey($match);
            }
        }
        return $foreignKeys;
    }

    /**
     * @inheritDoc
     */
    public function checkConstraints(TableDto $status): array
    {
        // From driver.inc.php
        $database = $this->_engine()->quote($this->_engine()->database());
        $table = $this->_engine()->quote($status->name);
        // MariaDB contains CHECK_CONSTRAINTS.TABLE_NAME, MySQL and PostrgreSQL not
        $query = "SELECT c.CONSTRAINT_NAME, CHECK_CLAUSE
FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS c JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
ON c.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA AND c.CONSTRAINT_NAME = t.CONSTRAINT_NAME
WHERE c.CONSTRAINT_SCHEMA = $database AND t.TABLE_NAME = $table
AND CHECK_CLAUSE NOT LIKE '% IS NOT NULL'";
        return $this->_engine()->keyValues($query); // ignore default IS NOT NULL checks in PostrgreSQL
    }

    /**
     * @inheritDoc
     */
    public function partitionsInfo(string $table): PartitionDto|null
    {
        $database = $this->_engine()->quote($this->_engine()->database());
        $tableName = $this->_engine()->quote($table);
        $from = "FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = $database AND TABLE_NAME = $tableName";
        $query = "SELECT PARTITION_METHOD, PARTITION_EXPRESSION, PARTITION_ORDINAL_POSITION $from
ORDER BY PARTITION_ORDINAL_POSITION DESC LIMIT 1";
        $result = $this->_engine()->execute($query)?->fetchRow();
        if (!$result) {
            return null;
        }

        [$columns, $strategy, $partitions] = $result;
        $entity = new PartitionDto($strategy, $columns, $partitions);

        $query = "SELECT PARTITION_NAME, PARTITION_DESCRIPTION $from
AND PARTITION_NAME != '' ORDER BY PARTITION_ORDINAL_POSITION";
        $partition = $this->_engine()->keyValues($query);
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
        $isMaria = $this->_engine()->flavor() === 'maria';

        $default = $row["COLUMN_DEFAULT"] ?? null;
        if ($default === '' || $default === null) {
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
     * @return ColumnDto
     */
    private function makeColumnDto(array $row): ColumnDto
    {
        $column = new ColumnDto();
        $column->fullType = $row["COLUMN_TYPE"];

        $extra = $row["EXTRA"];
        // https://mariadb.com/kb/en/library/show-columns/
        // https://github.com/vrana/adminer/pull/359#pullrequestreview-276677186
        preg_match('~^(VIRTUAL|PERSISTENT|STORED)~', $extra, $generated);
        preg_match('~^([^( ]+)(?:\((.+)\))?( unsigned)?( zerofill)?$~',
            $column->fullType, $matchType);

        $column->name = $row["COLUMN_NAME"];
        $column->fullType = $row["COLUMN_TYPE"];
        $column->type = $matchType[1] ?? '';
        $column->length = $matchType[2] ?? '';
        $column->unsigned = ltrim(($matchType[3] ?? '') . ($matchType[4] ?? ''));
        $column->nullable = $row["IS_NULLABLE"] === "YES";
        $column->autoIncrement = $extra === "auto_increment";
        $column->collation = $row["COLLATION_NAME"] ?? '';
        $column->comment = $row["COLUMN_COMMENT"] ?? null;
        $column->primary = $row["COLUMN_KEY"] === "PRI";
 
        //! available since MySQL 5.1.23
        $column->onUpdate = preg_match('~\bon update (\w+)~i', $extra, $match) ? $match[1] : '';
        $privileges = explode(",", $row["PRIVILEGES"] ?? '');
        $column->privileges = array_flip([...$privileges, 'where', 'order']);

        $defaultValue = $this->getRowDefaultValue($row, $matchType);
        $generation = $row["GENERATION_EXPRESSION"] ?? '';
        $column->default = !$generated ? $defaultValue :
            ($this->_engine()->maria() ? $generation : stripslashes($generation));

        $generated = $generated[1] ?? '';
        $column->generated = $generated === "PERSISTENT" ? "STORED" : $generated;

        return $column;
    }

    /**
     * @inheritDoc
     */
    public function columns(string $table): array
    {
        $columns = [];
        $tableName = $this->_engine()->quote($table);
        $query = "SELECT * FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = $tableName
ORDER BY ORDINAL_POSITION";
        $rows = $this->_engine()->rows($query);
        foreach ($rows as $row) {
            $column = $this->makeColumnDto($row);
            $columns[$column->name] = $column;
        }

        return $columns;
    }

    /**
     * @param bool $fast
     * @param string $table
     *
     * @return array
     */
    private function queryStatus(bool $fast, string $table = ''): array
    {
        $tableName = $this->_engine()->quote($table);
        $tableNameLike = $this->_engine()->quote(addcslashes($table, "%_\\"));
        $query = $fast ? "SELECT TABLE_NAME AS Name, ENGINE AS Engine, TABLE_COMMENT " .
            "AS Comment FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()" .
            ($table !== '' ? " AND TABLE_NAME = $tableName" : " ORDER BY Name") :
            "SHOW TABLE STATUS" . ($table !== '' ? " LIKE $tableNameLike" : '');
        return $this->_engine()->rows($query);
    }

    /**
     * @param array $row
     * @param string $table
     *
     * @return TableDto
     */
    private function makeStatus(array $row, string $table = ''): TableDto
    {
        $status = new TableDto($row['Name']);
        $status->engine = $row['Engine'] ?? '';
        $status->collation = $row['Collation'] ?? '';
        $status->hasAutoIncrement = isset($row['Auto_increment']);
        $status->autoIncrement = $row['Auto_increment'] ?? 0;
        $status->dataLength = $row['Data_length'] ?? null;
        $status->indexLength = $row['Index_length'] ?? null;
        $status->dataFree = $row['Data_free'] ?? null;
        $status->rowCount = $row['Rows'] ?? null;
        $status->comment = $row['Comment'] ?? null;

        if (!isset($row["Engine"])) {
            $status->comment = null;
        }
        if ($status->comment !== null && $row["Engine"] === "InnoDB") {
            // ignore internal comment, unnecessary since MySQL 5.1.21
            $status->comment = preg_replace('~(?:(.+); )?InnoDB free: .*~', '\1', $status->comment);
        }
        if ($table !== '') {
            // MariaDB: Table name is returned as lowercase on macOS, so we fix it here.
            $status->name = $table;
        }

        return $status;
    }

    /**
     * @inheritDoc
     */
    public function tableStatus(string $table, bool $fast = false): TableDto|null
    {
        $rows = $this->queryStatus($fast, $table);
        $row = reset($rows);
        return !$row ? null : $this->makeStatus($row, $table);
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
    public function isView(TableDto $tableStatus): bool
    {
        return $tableStatus->engine === '';
    }

    /**
     * @param array $row
     *
     * @return string
     */
    private function getTableIndexType(array $row): string
    {
        return match(true) {
            $row['Key_name'] === 'PRIMARY' => 'PRIMARY',
            $row['Index_type'] === 'FULLTEXT' => 'FULLTEXT',
            !$row['Non_unique'] => 'UNIQUE',
            $row['Index_type'] === 'SPATIAL' => 'SPATIAL',
            default => 'INDEX',
        };
    }

    /**
     * @param IndexDto $index
     * @param array $row
     *
     * @return IndexDto
     */
    private function fillTableIndex(IndexDto $index, array $row): IndexDto
    {
        $type = $row["Index_type"];

        $index->name = $row['Key_name'];
        $index->type = $this->getTableIndexType($row);
        $index->columns[] = $row['Column_name'];
        $index->lengths[] = $type === 'SPATIAL' ? null : $row['Sub_part'];
        $index->descs[] = null;
        $index->algorithm = $type;

        return $index;
    }

    /**
     * @inheritDoc
     */
    public function indexes(string $table): array
    {
        $tableName = $this->_statement()->escapeTableName($table);
        $rows = $this->_engine()->rows("SHOW INDEX FROM $tableName");
        $indexes = [];
        foreach ($rows as $row) {
            $name = $row['Key_name'];
            $indexes[$name] ??= new IndexDto();
            $this->fillTableIndex($indexes[$name], $row);
        }
        return $indexes;
    }

    /**
     * @inheritDoc
     */
    public function trigger(string $name, string $table = ''): TriggerDto|null
    {
        if ($name == '') {
            return null;
        }
        $rows = $this->_engine()->rows("SHOW TRIGGERS WHERE `Trigger` = " . $this->_engine()->quote($name));
        if (!($row = reset($rows))) {
            return null;
        }
        return new TriggerDto($row["Timing"], $row["Event"], '', '', $row["Trigger"]);
    }

    /**
     * @inheritDoc
     */
    public function triggers(string $table): array
    {
        $triggers = [];
        foreach ($this->_engine()->rows("SHOW TRIGGERS LIKE " . $this->_engine()->quote(addcslashes($table, "%_\\"))) as $row) {
            $triggers[$row["Trigger"]] = new TriggerDto($row["Timing"], $row["Event"], '', '', $row["Trigger"]);
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
        $isMaria = $this->_engine()->flavor() === 'maria';
        if ($this->_engine()->isInformationSchema($this->_engine()->database())) {
            return strtolower(($isMaria ? "information-schema-$name-table/" :
                    str_replace("_", "-", $name) . "-table.html"));
        }
        if ($this->_engine()->database() == "mysql") {
            return $isMaria ? "mysql$name-table/" : "system-database.html"; //! more precise link
        }
        return '';
    }
}
