<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db\Traits;

use Lagdo\DbAdmin\Driver\Db\StatementInterface;

use function preg_match;
use function preg_replace;

trait ConnectionTrait
{
    /**
     * @inheritDoc
     */
    public function flavor(): string
    {
        $serverInfo = $this->serverInfo();
        return !$serverInfo ? '' : (preg_match('~MariaDB~', $serverInfo) ? 'maria' : 'mysql');
    }

    /**
     * @return string
     */
    public function error(): string
    {
        $error = preg_replace('~^You have an error.*syntax to use~U', 'Syntax error', parent::error());
        // windows-1250 - most common Windows encoding
        // if (function_exists('iconv') && !$this->utils->str->isUtf8($error) &&
        //     strlen($s = iconv("windows-1250", "utf-8", $error)) > strlen($error)) {
        //     $error = $s;
        // }
        return $this->utils->str->html($error);
    }

    /**
     * @inheritDoc
     */
    public function explain(string $query): StatementInterface|bool
    {
        return $this->query('EXPLAIN ' . ($this->driver->minVersion(5.1) &&
            !$this->driver->minVersion(5.7) ? 'PARTITIONS ' : '') . $query);
    }
}
