<?php

namespace Lagdo\DbAdmin\Driver\MySql\Engine;

use Lagdo\DbAdmin\Driver\Sql\Connection\StatementInterface;
use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnType;
use Lagdo\DbAdmin\Driver\Sql\Dto\RoutineDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\RoutineInfoDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Engine\AbstractDatabase;

use function addcslashes;
use function array_combine;
use function array_keys;
use function array_map;
use function count;
use function implode;
use function intval;
use function in_array;
use function is_a;
use function preg_match;
use function preg_match_all;
use function preg_replace;
use function preg_replace_callback;
use function str_replace;
use function stripcslashes;
use function strtolower;
use function strtoupper;
use function substr;
use function trim;

class Database extends AbstractDatabase
{
    /**
     * @inheritDoc
     */
    public function databases(bool $flush): array
    {
        // !!! Caching and slow query handling are temporarily disabled !!!
        $query = $this->_engine()->minVersion(5) ?
            'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME' :
            'SHOW DATABASES';
        return $this->_engine()->columnValues($query);

        // SHOW DATABASES can take a very long time so it is cached
        // $databases = get_session('dbs');
        // if ($databases === null) {
        //     $query = ($this->_engine()->minVersion(5)
        //         ? 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME'
        //         : 'SHOW DATABASES'
        //     ); // SHOW DATABASES can be disabled by skip_show_database
        //     $databases = ($flush ? slow_query($query) : $this->_engine()->values($query));
        //     restart_session();
        //     set_session('dbs', $databases);
        //     stop_session();
        // }
        // return $databases;
    }

    /**
     * @inheritDoc
     */
    public function databaseSize(string $database): int
    {
        $statement = $this->_engine()->execute("SELECT SUM(data_length + index_length)
FROM information_schema.tables where table_schema = " . $this->_engine()->quote($database));
        return is_a($statement, StatementInterface::class) && ($row = $statement->fetchRow()) ?
            intval($row[0]) : 0;
    }

    /**
     * @inheritDoc
     */
    public function databaseCollation(string $database, array $collations): string
    {
        $databaseName = $this->_statement()->escapeId($database);
        $create = $this->_engine()->result("SHOW CREATE DATABASE $databaseName", 1);

        return match(true) {
            preg_match('~ COLLATE ([^ ]+)~', $create, $match) => $match[1],
            preg_match('~ CHARACTER SET ([^ ]+)~', $create, $match) =>
                $collations[$match[1]][-1], // default collation
            default => null,
        };
    }

    /**
     * @inheritDoc
     */
    public function isInformationSchema(string $database): bool
    {
        return ($this->_engine()->minVersion(5) && $database == 'information_schema') ||
            ($this->_engine()->minVersion(5.5) && $database == 'performance_schema');
    }

    /**
     * @inheritDoc
     */
    public function isSystemSchema(string $database): bool
    {
        return in_array($database, ['sys', 'mysql', 'performance_schema', 'information_schema']);
    }

    /**
     * @inheritDoc
     */
    public function tables(): array
    {
        return $this->_engine()->keyValues("SELECT TABLE_NAME, TABLE_TYPE
FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME");
    }

    /**
     * @inheritDoc
     */
    public function countTables(array $databases): array
    {
        $counts = array_map(function(string $database) {
            $query = 'SHOW TABLES IN ' . $this->_statement()->escapeId($database);
            return count($this->_engine()->columnValues($query));
        }, $databases);

        return array_combine($databases, $counts);
    }

    /**
     * @inheritDoc
     */
    public function events(): array
    {
        return $this->_engine()->rows('SHOW EVENTS');
    }

    /**
     * @param array $match
     *
     * @return string
     */
    private function normalizeEnum(array $match): string
    {
        $enumValue = $match[0];
        $firstChar = $enumValue[0];
        $enumValue = str_replace("$firstChar$firstChar", $firstChar, substr($enumValue, 1, -1));

        return "'" . str_replace("'", "''", addcslashes(stripcslashes($enumValue), '\\')) . "'";
    }

    /**
     * @param array $param
     *
     * @return ColumnType
     */
    private function makeRoutineColumn(array $param): ColumnType
    {
        $enumLength = $this->_engine()->enumLengthRegex();
        $name = str_replace("``", "`", $param[2]) . $param[3];
        $type = strtolower($param[5]);
        $length = preg_replace_callback("~$enumLength~s",
            $this->normalizeEnum(...), $param[6] ?? '');
        $inout = strtoupper($param[1]);
        $fullType = $param[4];
        $collation = strtolower($param[9] ?? '');
        $unsigned = strtolower(preg_replace('~\s+~', ' ',
            trim(($param[8] ?? '') . ' ' . ($param[7] ?? ''))));
        $nullable = 1;

        return new ColumnType(name: $name, type: $type, length: $length,
            inout: $inout, fullType: $fullType, collation: $collation,
            unsigned: $unsigned, nullable: $nullable);
    }

    /**
     * @inheritDoc
     */
    public function routine(string $name, string $type): RoutineInfoDto|null
    {
        $types = array_keys($this->_engine()->types());
        $aliases = ['bool', 'boolean', 'integer', 'double precision', 'real',
            'dec', 'numeric', 'fixed', 'national char', 'national varchar'];
        $routineTypes = implode("|", [...$types, ...$aliases]);
        $enumLength = $this->_engine()->enumLengthRegex();
        $routineName = $this->_statement()->escapeId($name);
        $isFunction = $type === 'FUNCTION';
        $paramType = $isFunction ? '' : $this->_engine()->inout();

        $space = "(?:\\s|/\\*[\s\S]*?\\*/|(?:#|-- )[^\n]*\n?|--\r?\n)";
        $typePattern = "(($routineTypes)\\b(?:\\s*\\(((?:[^'\")]|$enumLength)++)\\))?\\s*" .
            "(zerofill\\s*)?(unsigned(?:\\s+zerofill)?)?)(?:\\s*(?:CHARSET|CHARACTER\\s+SET)" .
            "\\s*['\"]?([^'\"\\s,]+)['\"]?)?(?:\\s*COLLATE\\s*['\"]?[^'\"\\s,]+['\"]?)?"; //! store COLLATE
        $pattern = "$space*($paramType)?\\s*(?:`((?:[^`]|``)*)`\\s*|\\b(\\S+)\\s+)$typePattern";

        $create = $this->_engine()->result("SHOW CREATE $type $routineName", 2);
        if (!$create) {
            return null;
        }

        $returnCode = $isFunction ? "RETURNS\\s+$typePattern\\s+" : '';
        preg_match("~\\(((?:$pattern\\s*,?)*)\\)\\s*$returnCode(.*)~is", $create, $match);
        $language = 'SQL'; // available in information_schema.ROUTINES.PARAMETER_STYLE;
        $query = "SELECT ROUTINE_COMMENT FROM information_schema.ROUTINES
WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = " . $this->_engine()->quote($name);
        $comment = $this->_engine()->result($query);

        preg_match_all("~$pattern\\s*,?~is", $match[1], $matches, PREG_SET_ORDER);
        // All indexes greater than 5 can be missing.
        $params = array_map($this->makeRoutineColumn(...), $matches);

        return !$isFunction ?
            new RoutineInfoDto($match[11], '', $params, null, $comment ?: '') :
            new RoutineInfoDto($match[17], $language, $params,
                new ColumnType(type: $match[12], length: $match[13],
                    unsigned: $match[15], collation: $match[16]), $comment ?: '');
    }

    /**
     * @inheritDoc
     */
    public function routines(): array
    {
        $query = "SELECT SPECIFIC_NAME, ROUTINE_NAME, ROUTINE_TYPE, DTD_IDENTIFIER
FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE()";
        $callback = fn($row) => new RoutineDto($row['ROUTINE_NAME'],
            $row['SPECIFIC_NAME'], $row['ROUTINE_TYPE'], $row['DTD_IDENTIFIER'] ?: '');

        return array_map($callback, $this->_engine()->rows($query));
    }

    /**
     * @inheritDoc
     */
    public function routineId(string $name, array $row): string
    {
        return $this->_statement()->escapeId($name);
    }
}
