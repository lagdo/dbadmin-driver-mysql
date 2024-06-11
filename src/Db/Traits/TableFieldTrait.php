<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db\Traits;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;

use function array_pad;
use function preg_match;
use function intval;
use function ltrim;
use function preg_replace;
use function preg_split;
use function stripslashes;
use function array_flip;

trait TableFieldTrait
{
    /**
     * @param array $row
     * @param string $rowType
     *
     * @return mixed|string|null
     */
    private function getRowDefaultValue(array $row, string $rowType)
    {
        if ($row["Default"] === '' && preg_match('~char|set~', $rowType) === false) {
            return null;
        }
        if (preg_match('~text~', $rowType)) {
            return stripslashes(preg_replace("~^'(.*)'\$~", '\1', $row["Default"]));
        }
        return $row["Default"];
    }

    /**
     * @param array $row
     *
     * @return string
     */
    private function getRowUpdateFunction(array $row)
    {
        if (preg_match('~^on update (.+)~i', $row["Extra"], $match) === false) {
            return '';
        }
        $match = array_pad($match, 2, '');
        return $match[1]; //! available since MySQL 5.1.23
    }

    /**
     * @param array $row
     *
     * @return TableFieldEntity
     */
    private function makeTableFieldEntity(array $row): TableFieldEntity
    {
        preg_match('~^([^( ]+)(?:\((.+)\))?( unsigned)?( zerofill)?$~', $row["Type"], $match);
        $field = new TableFieldEntity();
        $match = array_pad($match, 5, '');

        $field->name = $row["Field"];
        $field->fullType = $row["Type"];
        $field->type = $match[1];
        $field->length = intval($match[2]);
        $field->unsigned = ltrim($match[3] . $match[4]);
        $field->default = $this->getRowDefaultValue($row, $match[1]);
        $field->null = ($row["Null"] == "YES");
        $field->autoIncrement = ($row["Extra"] == "auto_increment");
        $field->onUpdate = $this->getRowUpdateFunction($row);
        $field->collation = $row["Collation"];
        $field->privileges = array_flip(preg_split('~, *~', $row["Privileges"]));
        $field->comment = $row["Comment"];
        $field->primary = ($row["Key"] == "PRI");
        // https://mariadb.com/kb/en/library/show-columns/
        // https://github.com/vrana/adminer/pull/359#pullrequestreview-276677186
        $field->generated = preg_match('~^(VIRTUAL|PERSISTENT|STORED)~', $row["Extra"]) > 0;

        return $field;
    }

    /**
     * @inheritDoc
     */
    public function fields(string $table)
    {
        $fields = [];
        $rows = $this->driver->rows("SHOW FULL COLUMNS FROM " . $this->driver->table($table));
        foreach ($rows as $row) {
            $fields[$row["Field"]] = $this->makeTableFieldEntity($row);
        }
        return $fields;
    }
}
