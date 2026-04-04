<?php

namespace Lagdo\DbAdmin\Support\MySql\Driver;

use Lagdo\DbAdmin\Support\Db\Engine\Driver\AbstractDatabase;
use Lagdo\DbAdmin\Support\Dto\FieldType;
use Lagdo\DbAdmin\Support\Dto\RoutineDto;
use Lagdo\DbAdmin\Support\Dto\RoutineInfoDto;

use function addcslashes;
use function array_keys;
use function array_map;
use function array_merge;
use function count;
use function implode;
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
            $counts[$database] = count($this->driver->values('SHOW TABLES IN ' . $this->grammar->escapeId($database)));
        }
        return $counts;
    }

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
    public function routine(string $name, string $type): RoutineInfoDto|null
    {
        $enumLength = $this->driver->enumLengthRegex();
        $aliases = ['bool', 'boolean', 'integer', 'double precision', 'real',
            'dec', 'numeric', 'fixed', 'national char', 'national varchar'];
        $space = "(?:\\s|/\\*[\s\S]*?\\*/|(?:#|-- )[^\n]*\n?|--\r?\n)";
        $typePattern = "((" . implode("|", array_merge(array_keys($this->driver->types()), $aliases)) .
            ")\\b(?:\\s*\\(((?:[^'\")]|$enumLength)++)\\))?\\s*(zerofill\\s*)?" .
            "(unsigned(?:\\s+zerofill)?)?)(?:\\s*(?:CHARSET|CHARACTER\\s+SET)" .
            "\\s*['\"]?([^'\"\\s,]+)['\"]?)?(?:\\s*COLLATE\\s*['\"]?[^'\"\\s,]+['\"]?)?"; //! store COLLATE
        $pattern = "$space*(" . ($type == 'FUNCTION' ? '' : $this->driver->inout()) .
            ")?\\s*(?:`((?:[^`]|``)*)`\\s*|\\b(\\S+)\\s+)$typePattern";

        $create = $this->driver->result("SHOW CREATE $type " . $this->grammar->escapeId($name), 2);
        if (!$create) {
            return null;
        }

        preg_match("~\\(((?:$pattern\\s*,?)*)\\)\\s*" . ($type == "FUNCTION" ?
            "RETURNS\\s+$typePattern\\s+" : '') . "(.*)~is", $create, $match);
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
        $rows = $this->driver->rows('SELECT SPECIFIC_NAME, ROUTINE_NAME, ROUTINE_TYPE, ' .
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
        return $this->grammar->escapeId($name);
    }
}
