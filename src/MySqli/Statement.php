<?php

namespace Lagdo\DbAdmin\Driver\MySql\MySqli;

use Lagdo\DbAdmin\Driver\Db\StatementInterface;
use Lagdo\DbAdmin\Driver\Entity\StatementField;

use mysqli_result;

class Statement implements StatementInterface
{
    /**
     * The query result
     *
     * @var mysqli_result
     */
    public $result = null;

    /**
     * Undocumented variable
     *
     * @var int
     */
    public $numRows = 0;

    /**
     * The constructor
     *
     * @param mysqli_result|bool $result
     */
    public function __construct($result)
    {
        if (is_a($result, mysqli_result::class)) {
            $this->result = $result;
            $this->numRows = $result->num_rows;
        }
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
        return new StatementField($field->type, $field->type === 63, // 63 - binary
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
