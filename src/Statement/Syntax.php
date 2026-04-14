<?php

namespace Lagdo\DbAdmin\Driver\MySql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractSyntax;

use function array_key_exists;
use function preg_match;
use function str_replace;

class Syntax extends AbstractSyntax
{
    /**
     * @inheritDoc
     */
    public function escapeId(string $idf): string
    {
        return "`" . str_replace("`", "``", $idf) . "`";
    }

    /**
     * @inheritDoc
     */
    public function processAttr(array $process, string $key, string $val): string
    {
        $match = array_key_exists('Command', $process) &&
            preg_match('~Query|Killed~', $process['Command']);
        if ($key === 'Info' && $match && $val !== '') {
            return '<code>' . $this->_utils()->str->shortenUtf8($val, 50) .
                '</code>' . $this->_utils()->lang('Clone');
        }
        return parent::processAttr($process, $key, $val);
    }
}
