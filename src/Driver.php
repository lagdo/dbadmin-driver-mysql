<?php

namespace Lagdo\DbAdmin\Driver\MySql;

use Lagdo\DbAdmin\Driver\Db\Driver as AbstractDriver;

class Driver extends AbstractDriver
{
    public function insert($table, $set)
    {
        return ($set ? parent::insert($table, $set) : $this->db->queries("INSERT INTO " .
            $this->server->table($table) . " ()\nVALUES ()"));
    }

    public function insertUpdate($table, $rows, $primary)
    {
        $columns = array_keys(reset($rows));
        $prefix = "INSERT INTO " . $this->server->table($table) . " (" . implode(", ", $columns) . ") VALUES\n";
        $values = [];
        foreach ($columns as $key) {
            $values[$key] = "$key = VALUES($key)";
        }
        $suffix = "\nON DUPLICATE KEY UPDATE " . implode(", ", $values);
        $values = [];
        $length = 0;
        foreach ($rows as $set) {
            $value = "(" . implode(", ", $set) . ")";
            if ($values && (strlen($prefix) + $length + strlen($value) + strlen($suffix) > 1e6)) { // 1e6 - default max_allowed_packet
                if (!$this->db->queries($prefix . implode(",\n", $values) . $suffix)) {
                    return false;
                }
                $values = [];
                $length = 0;
            }
            $values[] = $value;
            $length += strlen($value) + 2; // 2 - strlen(",\n")
        }
        return $this->db->queries($prefix . implode(",\n", $values) . $suffix);
    }

    public function slowQuery($query, $timeout)
    {
        if ($this->server->min_version('5.7.8', '10.1.2')) {
            if (preg_match('~MariaDB~', $this->connection->server_info)) {
                return "SET STATEMENT max_statement_time=$timeout FOR $query";
            } elseif (preg_match('~^(SELECT\b)(.+)~is', $query, $match)) {
                return "$match[1] /*+ MAX_EXECUTION_TIME(" . ($timeout * 1000) . ") */ $match[2]";
            }
        }
    }

    public function convertSearch($idf, $val, $field)
    {
        return (preg_match('~char|text|enum|set~', $field["type"]) &&
            !preg_match("~^utf8~", $field["collation"]) && preg_match('~[\x80-\xFF]~', $val['val']) ?
            "CONVERT($idf USING " . $this->server->charset() . ")" : $idf
        );
    }

    // public function warnings() {
    //     $result = $this->connection->query("SHOW WARNINGS");
    //     if ($result && $result->num_rows) {
    //         ob_start();
    //         select($result); // select() usually needs to print a big table progressively
    //         return ob_get_clean();
    //     }
    // }

    public function tableHelp($name)
    {
        $maria = preg_match('~MariaDB~', $this->connection->server_info);
        if ($this->server->information_schema($this->server->current_db())) {
            return strtolower(($maria ? "information-schema-$name-table/" : str_replace("_", "-", $name) . "-table.html"));
        }
        if ($this->server->current_db() == "mysql") {
            return ($maria ? "mysql$name-table/" : "system-database.html"); //! more precise link
        }
    }
}
