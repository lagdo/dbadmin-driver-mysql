<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db\Traits;

use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;

trait TableIndexTrait
{
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
    public function indexes(string $table)
    {
        $indexes = [];
        foreach ($this->driver->rows('SHOW INDEX FROM ' . $this->driver->table($table)) as $row) {
            $indexes[$row['Key_name']] = $this->makeTableIndex($row);
        }
        return $indexes;
    }
}
