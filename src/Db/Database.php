<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use Lagdo\DbAdmin\Driver\Db\Database as AbstractDatabase;
use Lagdo\DbAdmin\Driver\Entity\FieldType;
use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;
use Lagdo\DbAdmin\Driver\Entity\RoutineInfoEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;

class Database extends AbstractDatabase
{
    /**
     * @param TableEntity $tableAttrs
     *
     * @return string
     */
    private function _tableStatus(TableEntity $tableAttrs)
    {
        return 'COMMENT=' . $this->driver->quote($tableAttrs->comment) .
            ($tableAttrs->engine ? ' ENGINE=' . $this->driver->quote($tableAttrs->engine) : '') .
            ($tableAttrs->collation ? ' COLLATE ' . $this->driver->quote($tableAttrs->collation) : '') .
            ($tableAttrs->autoIncrement !== 0 ? " AUTO_INCREMENT=$tableAttrs->autoIncrement" : '');
    }

    /**
     * @inheritDoc
     */
    public function createTable(TableEntity $tableAttrs): bool
    {
        $clauses = [];
        foreach ($tableAttrs->fields as $field) {
            $clauses[] = implode($field[1]);
        }

        $clauses = array_merge($clauses, $tableAttrs->foreign);
        $status = $this->_tableStatus($tableAttrs);

        $result = $this->driver->execute('CREATE TABLE ' . $this->driver->escapeTableName($tableAttrs->name) .
            ' (' . implode(', ', $clauses) . ") $status $tableAttrs->partitioning");
        return $result !== false;
    }

    /**
     * @inheritDoc
     */
    public function alterTable(string $table, TableEntity $tableAttrs): bool
    {
        $clauses = [];
        foreach ($tableAttrs->fields as $field) {
            $clauses[] = 'ADD ' . implode($field[1]) . $field[2];
        }
        foreach ($tableAttrs->edited as $field) {
            $clauses[] = 'CHANGE ' . $this->driver->escapeId($field[0]) . ' ' . implode($field[1]) . $field[2];
        }
        foreach ($tableAttrs->dropped as $column) {
            $clauses[] = 'DROP ' . $this->driver->escapeId($column);
        }

        $clauses = array_merge($clauses, $tableAttrs->foreign);
        if ($tableAttrs->name !== '' && $table !== $tableAttrs->name) {
            $clauses[] = 'RENAME TO ' . $this->driver->escapeTableName($tableAttrs->name);
        }
        $clauses[] = $this->_tableStatus($tableAttrs);

        $result = $this->driver->execute('ALTER TABLE ' . $this->driver->escapeTableName($table) . ' ' .
            implode(', ', $clauses) . ' ' . $tableAttrs->partitioning);
        return $result !== false;
    }

    /**
     * @inheritDoc
     */
    public function alterIndexes(string $table, array $alter, array $drop): bool
    {
        $clauses = [];
        foreach ($drop as $index) {
            $clauses[] = 'DROP INDEX ' . $this->driver->escapeId($index->name);
        }
        foreach ($alter as $index) {
            $clauses[] = 'ADD ' . ($index->type == 'PRIMARY' ? 'PRIMARY KEY ' :  $index->type . ' ') .
                ($index->name != '' ? $this->driver->escapeId($index->name) . ' ' : '') .
                '(' . implode(', ', $index->columns) . ')';
        }
        $result = $this->driver->execute('ALTER TABLE ' . $this->driver->escapeTableName($table) . ' ' . implode(', ', $clauses));
        return $result !== false;
    }

    /**
     * @inheritDoc
     */
    public function tables(): array
    {
        return $this->driver->keyValues($this->driver->minVersion(5) ?
            'SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME' :
            'SHOW TABLES');
    }

    /**
     * @inheritDoc
     */
    public function countTables(array $databases): array
    {
        $counts = [];
        foreach ($databases as $database) {
            $counts[$database] = count($this->driver->values('SHOW TABLES IN ' . $this->driver->escapeId($database)));
        }
        return $counts;
    }

    /**
     * @inheritDoc
     */
    public function dropViews(array $views): bool
    {
        $this->driver->execute('DROP VIEW ' . implode(', ', array_map(function ($view) {
            return $this->driver->escapeTableName($view);
        }, $views)));
        return true;
    }

    /**
     * @inheritDoc
     */
    public function dropTables(array $tables): bool
    {
        $this->driver->execute('DROP TABLE ' . implode(', ', array_map(function ($table) {
            return $this->driver->escapeTableName($table);
        }, $tables)));
        return true;
    }

    /**
     * @inheritDoc
     */
    public function truncateTables(array $tables): bool
    {
        return $this->driver->applyQueries('TRUNCATE TABLE', $tables);
    }

    /**
     * @inheritDoc
     */
    public function moveTables(array $tables, array $views, string $target): bool
    {
        // The feature is not natively provided by latest MySQL versions, thus it is disabled here.
        return false;
        /*$rename = [];
        foreach ($tables as $table) {
            $rename[] = $this->driver->escapeTableName($table) . ' TO ' . $this->driver->escapeId($target) . '.' . $this->driver->escapeTableName($table);
        }
        if (!$rename || $this->driver->execute('RENAME TABLE ' . implode(', ', $rename))) {
            $definitions = [];
            foreach ($views as $table) {
                $definitions[$this->driver->escapeTableName($table)] = $this->driver->view($table);
            }
            // $this->connection->open($target);
            $database = $this->driver->escapeId($this->driver->database());
            foreach ($definitions as $name => $view) {
                if (!$this->driver->execute("CREATE VIEW $name AS " . str_replace(" $database.", ' ', $view['select'])) ||
                    !$this->driver->execute("DROP VIEW $database.$name")) {
                    return false;
                }
            }
            return true;
        }
        //! move triggers
        return false;*/
    }

    /**
     * @inheritDoc
     */
    // public function copyTables(array $tables, array $views, string $target): bool
    // {
    //     $this->driver->execute("SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO'");
    //     $overwrite = $this->utils->input->getOverwrite();
    //     foreach ($tables as $table) {
    //         $name = ($target == $this->driver->database() ? $this->driver->escapeTableName("copy_$table") : $this->driver->escapeId($target) . '.' . $this->driver->escapeTableName($table));
    //         if (($overwrite && !$this->driver->execute("\nDROP TABLE IF EXISTS $name"))
    //             || !$this->driver->execute("CREATE TABLE $name LIKE " . $this->driver->escapeTableName($table))
    //             || !$this->driver->execute("INSERT INTO $name SELECT * FROM " . $this->driver->escapeTableName($table))
    //         ) {
    //             return false;
    //         }
    //         foreach ($this->driver->rows('SHOW TRIGGERS LIKE ' . $this->driver->quote(addcslashes($table, "%_\\"))) as $row) {
    //             $trigger = $row['Trigger'];
    //             if (!$this->driver->execute('CREATE TRIGGER ' .
    //                 ($target == $this->driver->database() ? $this->driver->escapeId("copy_$trigger") :
    //                 $this->driver->escapeId($target) . '.' . $this->driver->escapeId($trigger)) .
    //                 " $row[Timing] $row[Event] ON $name FOR EACH ROW\n$row[Statement];")) {
    //                 return false;
    //             }
    //         }
    //     }
    //     foreach ($views as $table) {
    //         $name = ($target == $this->driver->database() ? $this->driver->escapeTableName("copy_$table") :
    //             $this->driver->escapeId($target) . '.' . $this->driver->escapeTableName($table));
    //         $view = $this->driver->view($table);
    //         if (($overwrite && !$this->driver->execute("DROP VIEW IF EXISTS $name"))
    //             || !$this->driver->execute("CREATE VIEW $name AS $view[select]")) { //! USE to avoid db.table
    //             return false;
    //         }
    //     }
    //     return true;
    // }

    /**
     * @inheritDoc
     */
    public function events(): array
    {
        return $this->driver->rows('SHOW EVENTS');
    }

    /**
     * @inheritDoc
     */
    public function routine(string $name, string $type): RoutineInfoEntity|null
    {
        $enumLength = $this->driver->enumLength();
        $aliases = ['bool', 'boolean', 'integer', 'double precision', 'real',
            'dec', 'numeric', 'fixed', 'national char', 'national varchar'];
        $space = "(?:\\s|/\\*[\s\S]*?\\*/|(?:#|-- )[^\n]*\n?|--\r?\n)";
        $type_pattern = "((" . implode("|", array_merge(array_keys($this->driver->types()), $aliases)) .
            ")\\b(?:\\s*\\(((?:[^'\")]|$enumLength)++)\\))?\\s*(zerofill\\s*)?" .
            "(unsigned(?:\\s+zerofill)?)?)(?:\\s*(?:CHARSET|CHARACTER\\s+SET)" .
            "\\s*['\"]?([^'\"\\s,]+)['\"]?)?(?:\\s*COLLATE\\s*['\"]?[^'\"\\s,]+['\"]?)?"; //! store COLLATE
        $pattern = "$space*(" . ($type == 'FUNCTION' ? '' : $this->driver->inout()) .
            ")?\\s*(?:`((?:[^`]|``)*)`\\s*|\\b(\\S+)\\s+)$type_pattern";

        $create = $this->driver->result("SHOW CREATE $type " . $this->driver->escapeId($name), 2);
        if (!$create) {
            return null;
        }

        preg_match("~\\(((?:$pattern\\s*,?)*)\\)\\s*" . ($type == "FUNCTION" ?
            "RETURNS\\s+$type_pattern\\s+" : '') . "(.*)~is", $create, $match);
        $language = 'SQL'; // available in information_schema.ROUTINES.PARAMETER_STYLE;
        $query = "SELECT ROUTINE_COMMENT FROM information_schema.ROUTINES WHERE " .
            "ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = " . $this->driver->quote($name);
        $comment = $this->driver->result($query);

        preg_match_all("~$pattern\\s*,?~is", $match[1], $matches, PREG_SET_ORDER);
        $normalizeEnum = function(array $match): string {
            $val = $match[0];
            return "'" . str_replace("'", "''",
                addcslashes(stripcslashes(str_replace($val[0] . $val[0],
                $val[0], substr($val, 1, -1))), '\\')) . "'";
        };
        // All indexes greater than 5 can be missing.
        $params = array_map(function(array $param) use($enumLength, $normalizeEnum) {
            $name = str_replace("``", "`", $param[2]) . $param[3];
            $type = strtolower($param[5]);
            $length = preg_replace_callback("~$enumLength~s", $normalizeEnum, $param[6] ?? '');
            $inout = strtoupper($param[1]);
            $fullType = $param[4];
            $collation = strtolower($param[9] ?? '');
            $unsigned = strtolower(preg_replace('~\s+~', ' ',
                trim(($param[8] ?? '') . ' ' . ($param[7] ?? ''))));
            $null = 1;
            return new FieldType(name: $name, type: $type, length: $length, inout: $inout, fullType: $fullType,
                collation: $collation, unsigned: $unsigned, null: $null);
        }, $matches);

        return $type !== 'FUNCTION' ?
            new RoutineInfoEntity($match[11], '', $params, null, $comment ?: '') :
            new RoutineInfoEntity($match[17], $language, $params,
                new FieldType(type: $match[12], length: $match[13],
                    unsigned: $match[15], collation: $match[16]), $comment ?: '');
    }

    /**
     * @inheritDoc
     */
    public function routines(): array
    {
        $rows = $this->driver->rows('SELECT SPECIFIC_NAME, ROUTINE_NAME, ROUTINE_TYPE, ' .
            'DTD_IDENTIFIER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE()');
        return array_map(fn($row) =>
            new RoutineEntity($row['ROUTINE_NAME'], $row['SPECIFIC_NAME'],
                $row['ROUTINE_TYPE'], $row['DTD_IDENTIFIER'] ?: ''), $rows);
    }

    /**
     * @inheritDoc
     */
    public function routineId(string $name, array $row): string
    {
        return $this->driver->escapeId($name);
    }
}
