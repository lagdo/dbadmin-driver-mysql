<?php

namespace Lagdo\DbAdmin\Driver\MySql\Connection\Traits;

use Lagdo\DbAdmin\Driver\Sql\Specific\Connection\StatementInterface;

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
        // if (function_exists('iconv') && !$this->_utils()->str->isUtf8($error) &&
        //     strlen($s = iconv("windows-1250", "utf-8", $error)) > strlen($error)) {
        //     $error = $s;
        // }
        return $this->_utils()->str->html($error);
    }

    /**
     * @inheritDoc
     */
    public function explain(string $query): StatementInterface|bool
    {
        return $this->query('EXPLAIN ' . ($this->_engine()->minVersion(5.1) &&
            !$this->_engine()->minVersion(5.7) ? 'PARTITIONS ' : '') . $query);
    }
}
