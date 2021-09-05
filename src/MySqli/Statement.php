<?php

namespace Lagdo\DbAdmin\Driver\MySql\MySqli;

use Lagdo\DbAdmin\Driver\Db\StatementInterface;

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

    public function __construct($result)
    {
        if (is_a($result, mysqli_result::class)) {
            $this->result = $result;
            $this->numRows = $result->num_rows;
        }
    }

    public function fetchAssoc()
    {
        return ($this->result) ? $this->result->fetch_assoc() : null;
    }

    public function fetchRow()
    {
        return ($this->result) ? $this->result->fetch_row() : null;
    }

    public function fetchField()
    {
        return ($this->result) ? $this->result->fetch_field() : null;
    }

    public function __destruct()
    {
        if (($this->result)) {
            $this->result->free();
        }
    }
}
