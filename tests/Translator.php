<?php

namespace Lagdo\DbAdmin\Driver\MySql\Tests;

use Lagdo\DbAdmin\Driver\TranslatorInterface;

class Translator implements TranslatorInterface
{
    /**
     * @inheritDoc
     */
    public function lang(string $idf, $number = null): string
    {
        return $idf;
    }
}
