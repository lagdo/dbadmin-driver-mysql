<?php

namespace Lagdo\DbAdmin\Driver\MySql\Connection\MySqli;

use Lagdo\DbAdmin\Driver\Sql\Connection\QueryResultInterface;
use Lagdo\DbAdmin\Driver\Sql\Dto\ResultColumnDto;
use mysqli_result;

use function is_bool;

class QueryResult implements QueryResultInterface
{
    /**
     * The query result
     *
     * @var mysqli_result|null
     */
    private mysqli_result|null $result = null;

    /**
     * @var bool
     */
    private bool $hasError;

    /**
     * @param mysqli_result|bool $result
     */
    public function __construct(mysqli_result|bool $result)
    {
        $isBool = is_bool($result);
        $this->hasError = $isBool ? !$result : false;
        if (!$isBool) {
            $this->result = $result;
        }
    }

    /**
     * @inheritDoc
     */
    public function hasError(): bool
    {
        return $this->hasError;
    }

    /**
     * @inheritDoc
     */
    public function hasRowset(): bool
    {
        return $this->result !== null;
    }

    /**
     * @inheritDoc
     */
    public function rowCount(): int
    {
        return $this->result?->num_rows ?? 0;
    }

    /**
     * @inheritDoc
     */
    public function fetchAssoc(): array|null
    {
        return $this->result?->fetch_assoc() ?? null;
    }

    /**
     * @inheritDoc
     */
    public function fetchRow(): array|null
    {
        return $this->result?->fetch_row() ?? null;
    }

    /**
     * @inheritDoc
     */
    public function fetchColumn(): ResultColumnDto|null
    {
        $field = $this->result?->fetch_field() ?? false;
        return !$field ? null :
            new ResultColumnDto($field->type, $field->type === 63, // 63 - binary
                $field->name, $field->orgname, $field->table, $field->orgtable);
    }

    /**
     * The destructor
     */
    public function __destruct()
    {
        if ($this->result !== null) {
            $this->result->free();
            $this->result = null;
        }
    }
}
