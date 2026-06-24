<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\UpsertDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractQuery;
use stdClass;

use function array_keys;
use function array_map;
use function array_slice;
use function implode;
use function preg_match;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    protected function limitToOne(string $table, string $query, string $where): string
    {
        return $this->addLimitClause("$query$where", 1, 0);
    }

    /**
     * @inheritDoc
     */
    public function getTableUpsertQueries(UpsertDto $input): array
    {
        $tableName = $this->_statement()->escapeTableName($input->table);
        $columns = new stdClass();
        $columns->wheres = array_keys($input->keys);
        $columns->values = array_keys($input->values);
        $columns->insert = implode(', ', [...$columns->wheres, ...$columns->values]);

        return array_map(function(...$row) use($tableName, $input, $columns) {
            $updateValues = array_slice($row, $input->keyCount());
            $updateValues = $this->getUpdateClause(', ', $updateValues, $columns->values);

            $insertValues = implode(', ', $row);
            return "INSERT INTO $tableName({$columns->insert}) VALUES ($insertValues)
ON DUPLICATE KEY UPDATE $updateValues";
        }, ...$input->rows());
    }

    /**
     * @inheritDoc
     */
    public function convertColumn(ColumnDto $column): string
    {
        if (preg_match("~binary~", $column->type)) {
            return "HEX(" . $this->_statement()->escapeId($column->name) . ")";
        }
        if ($column->type == "bit") {
            // + 0 is required outside MySQLnd
            return "BIN(" . $this->_statement()->escapeId($column->name) . " + 0)";
        }
        if (preg_match("~geometry|point|linestring|polygon~", $column->type)) {
            return ($this->_engine()->minVersion(8) ? "ST_" : "") .
                "AsWKT(" . $this->_statement()->escapeId($column->name) . ")";
        }
        return '';
    }

    /**
     * @inheritDoc
     */
    public function unconvertColumn(ColumnDto $column, string $value): string
    {
        if (preg_match("~binary~", $column->type)) {
            $value = "UNHEX($value)";
        }
        if ($column->type == "bit") {
            $value = "CONV($value, 2, 10) + 0";
        }
        if (preg_match("~geometry|point|linestring|polygon~", $column->type)) {
            $value = ($this->_engine()->minVersion(8) ? "ST_" : "") .
                "GeomFromText($value, SRID({$column->name}))";
        }
        return $value;
    }
}
