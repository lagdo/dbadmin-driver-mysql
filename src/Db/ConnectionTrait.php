<?php

namespace Lagdo\DbAdmin\Driver\MySql\Db;

use function preg_replace;

trait ConnectionTrait
{
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
}
