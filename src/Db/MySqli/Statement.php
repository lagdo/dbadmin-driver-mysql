<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db\MySqli;

use Lagdo\DbAdmin\Driver\Db\StatementInterface;
use Lagdo\DbAdmin\Driver\Entity\StatementFieldEntity;

use mysqli_result;

use function is_a;

class Statement implements StatementInterface
{
    /**
     * The query result
     *
     * @var mysqli_result
     */
    protected $result = null;

    /**
     * The constructor
     *
     * @param mysqli_result|bool $result
     */
    public function __construct($result)
    {
        if (is_a($result, mysqli_result::class)) {
            $this->result = $result;
        }
    }

    /**
     * @inheritDoc
     */
    public function rowCount()
    {
        return $this->result ? $this->result->num_rows : 0;
    }

    /**
     * @inheritDoc
     */
    public function fetchAssoc()
    {
        return ($this->result) ? $this->result->fetch_assoc() : null;
    }

    /**
     * @inheritDoc
     */
    public function fetchRow()
    {
        return ($this->result) ? $this->result->fetch_row() : null;
    }

    /**
     * @inheritDoc
     */
    public function fetchField()
    {
        if (!$this->result || !($field = $this->result->fetch_field())) {
            return null;
        }
        return new StatementFieldEntity($field->type, $field->type === 63, // 63 - binary
            $field->name, $field->orgname, $field->table, $field->orgtable);
    }

    /**
     * The destructor
     */
    public function __destruct()
    {
        if (($this->result)) {
            $this->result->free();
        }
    }
}
