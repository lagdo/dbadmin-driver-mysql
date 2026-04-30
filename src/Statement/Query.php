<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\SelectInputDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractQuery;

use function count;
use function preg_match;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    public function limitToOne(string $table, string $query, string $where): string
    {
        return $this->getLimitClause($query, $where, 1, 0);
    }

    /**
     * @inheritDoc
     */
    public function getTableSelectQuery(SelectInputDto $input): string
    {
        $prefix = '';
        if (($input->page) && ($input->limit) && !empty($input->group) &&
            count($input->group) < count($input->columns)) {
            $prefix = 'SQL_CALC_FOUND_ROWS ';
        }
        return $prefix . parent::getTableSelectQuery($input);
    }

    /**
     * @inheritDoc
     */
    public function convertValue(ColumnDto $column): string
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
    public function unconvertValue(ColumnDto $column, string $value): string
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
