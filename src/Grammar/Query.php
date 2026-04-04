<?php

namespace Lagdo\DbAdmin\Support\MySql\Grammar;

use Lagdo\DbAdmin\Support\Db\Engine\Grammar\AbstractQuery;
use Lagdo\DbAdmin\Support\Dto\TableFieldDto;
use Lagdo\DbAdmin\Support\Dto\TableSelectDto;

use function count;
use function preg_match;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    protected function limitToOne(string $table, string $query, string $where): string
    {
        return $this->getLimitClause($query, $where, 1, 0);
    }

    /**
     * @inheritDoc
     */
    public function getTableSelectQuery(TableSelectDto $select): string
    {
        $prefix = '';
        if (($select->page) && ($select->limit) && !empty($select->group) &&
            count($select->group) < count($select->fields)) {
            $prefix = 'SQL_CALC_FOUND_ROWS ';
        }

        return $prefix . parent::getTableSelectQuery($select);
    }

    /**
     * @inheritDoc
     */
    public function convertField(TableFieldDto $field): string
    {
        if (preg_match("~binary~", $field->type)) {
            return "HEX(" . $this->grammar->escapeId($field->name) . ")";
        }
        if ($field->type == "bit") {
            // + 0 is required outside MySQLnd
            return "BIN(" . $this->grammar->escapeId($field->name) . " + 0)";
        }
        if (preg_match("~geometry|point|linestring|polygon~", $field->type)) {
            return ($this->driver->minVersion(8) ? "ST_" : "") .
                "AsWKT(" . $this->grammar->escapeId($field->name) . ")";
        }
        return '';
    }

    /**
     * @inheritDoc
     */
    public function unconvertField(TableFieldDto $field, string $value): string
    {
        if (preg_match("~binary~", $field->type)) {
            $value = "UNHEX($value)";
        }
        if ($field->type == "bit") {
            $value = "CONV($value, 2, 10) + 0";
        }
        if (preg_match("~geometry|point|linestring|polygon~", $field->type)) {
            $value = ($this->driver->minVersion(8) ? "ST_" : "") .
                "GeomFromText($value, SRID({$field->name}))";
        }
        return $value;
    }
}
