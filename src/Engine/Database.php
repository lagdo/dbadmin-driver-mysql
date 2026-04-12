<?php

namespace Lagdo\DbAdmin\Driver\MySql\Engine;

use Lagdo\DbAdmin\Driver\Sql\Specific\Connection\StatementInterface;
use Lagdo\DbAdmin\Driver\Sql\Specific\Engine\AbstractDatabase;
use Lagdo\DbAdmin\Driver\Sql\Dto\FieldType;
use Lagdo\DbAdmin\Driver\Sql\Dto\RoutineDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\RoutineInfoDto;

use function addcslashes;
use function array_keys;
use function array_map;
use function array_merge;
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
        return $this->_engine()->values($query);

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
        $statement = $this->_engine()->execute('SELECT SUM(data_length + index_length) ' .
            'FROM information_schema.tables where table_schema=' . $this->_engine()->quote($database));
        if (is_a($statement, StatementInterface::class) && ($row = $statement->fetchRow())) {
            return intval($row[0]);
        }
        return 0;
    }

    /**
     * @inheritDoc
     */
    public function databaseCollation(string $database, array $collations): string
    {
        $collation = null;
        $create = $this->_engine()->result('SHOW CREATE DATABASE ' . $this->_statement()->escapeId($database), 1);
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
        return in_array($database, ['sys', 'mysql',
            'performance_schema', 'information_schema']);
    }

    /**
     * @inheritDoc
     */
    public function tables(): array
    {
        return $this->_engine()->keyValues($this->_engine()->minVersion(5) ?
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
            $counts[$database] = count($this->_engine()->values('SHOW TABLES IN ' . $this->_statement()->escapeId($database)));
        }
        return $counts;
    }

    /**
     * @inheritDoc
     */
    public function events(): array
    {
        return $this->_engine()->rows('SHOW EVENTS');
    }

    /**
     * @inheritDoc
     */
    public function routine(string $name, string $type): RoutineInfoDto|null
    {
        $enumLength = $this->_engine()->enumLengthRegex();
        $aliases = ['bool', 'boolean', 'integer', 'double precision', 'real',
            'dec', 'numeric', 'fixed', 'national char', 'national varchar'];
        $space = "(?:\\s|/\\*[\s\S]*?\\*/|(?:#|-- )[^\n]*\n?|--\r?\n)";
        $typePattern = "((" . implode("|", array_merge(array_keys($this->_engine()->types()), $aliases)) .
            ")\\b(?:\\s*\\(((?:[^'\")]|$enumLength)++)\\))?\\s*(zerofill\\s*)?" .
            "(unsigned(?:\\s+zerofill)?)?)(?:\\s*(?:CHARSET|CHARACTER\\s+SET)" .
            "\\s*['\"]?([^'\"\\s,]+)['\"]?)?(?:\\s*COLLATE\\s*['\"]?[^'\"\\s,]+['\"]?)?"; //! store COLLATE
        $pattern = "$space*(" . ($type == 'FUNCTION' ? '' : $this->_engine()->inout()) .
            ")?\\s*(?:`((?:[^`]|``)*)`\\s*|\\b(\\S+)\\s+)$typePattern";

        $create = $this->_engine()->result("SHOW CREATE $type " . $this->_statement()->escapeId($name), 2);
        if (!$create) {
            return null;
        }

        preg_match("~\\(((?:$pattern\\s*,?)*)\\)\\s*" . ($type == "FUNCTION" ?
            "RETURNS\\s+$typePattern\\s+" : '') . "(.*)~is", $create, $match);
        $language = 'SQL'; // available in information_schema.ROUTINES.PARAMETER_STYLE;
        $query = "SELECT ROUTINE_COMMENT FROM information_schema.ROUTINES WHERE " .
            "ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = " . $this->_engine()->quote($name);
        $comment = $this->_engine()->result($query);

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
            $nullable = 1;
            return new FieldType(name: $name, type: $type, length: $length, inout: $inout, fullType: $fullType,
                collation: $collation, unsigned: $unsigned, nullable: $nullable);
        }, $matches);

        return $type !== 'FUNCTION' ?
            new RoutineInfoDto($match[11], '', $params, null, $comment ?: '') :
            new RoutineInfoDto($match[17], $language, $params,
                new FieldType(type: $match[12], length: $match[13],
                    unsigned: $match[15], collation: $match[16]), $comment ?: '');
    }

    /**
     * @inheritDoc
     */
    public function routines(): array
    {
        $rows = $this->_engine()->rows('SELECT SPECIFIC_NAME, ROUTINE_NAME, ROUTINE_TYPE, ' .
            'DTD_IDENTIFIER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE()');
        return array_map(fn($row) =>
            new RoutineDto($row['ROUTINE_NAME'], $row['SPECIFIC_NAME'],
                $row['ROUTINE_TYPE'], $row['DTD_IDENTIFIER'] ?: ''), $rows);
    }

    /**
     * @inheritDoc
     */
    public function routineId(string $name, array $row): string
    {
        return $this->_statement()->escapeId($name);
    }
}
