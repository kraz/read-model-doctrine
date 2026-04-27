<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tools;

class SqlFormatter
{
    public const string PART_WHERE = 'where';
    public const string PART_GROUP_BY = 'group_by';
    public const string PART_HAVING = 'having';
    public const string PART_ORDER_BY = 'order_by';

    private array $options;

    public function __construct(array $options = [])
    {
        $this->setOptions($options);
    }

    protected function getDefaultOptions(): array
    {
        return [
            'section_prefix' => '/*#',
            'section_sufix' => '#*/',
            'section_begin_prefix' => '',
            'section_begin_sufix' => '_B',
            'section_end_prefix' => '',
            'section_end_sufix' => '_E',
            'parts' => [
                self::PART_WHERE => 'WHERE',
                self::PART_GROUP_BY => 'GROUPBY',
                self::PART_HAVING => 'HAVING',
                self::PART_ORDER_BY => 'ORDERBY',
            ],
        ];
    }

    public function setOptions(array $options): static
    {
        $this->options = array_replace_recursive($this->getDefaultOptions(), $options);

        return $this;
    }

    public function getOptions(): array
    {
        return $this->options;
    }

    protected function getSQLPartLabel(string $name): string
    {
        $opt = $this->getOptions();
        if (!\array_key_exists($name, $opt['parts'])) {
            throw new \RuntimeException(\sprintf('Unknown SQL part identifier: "%s". Expected one of the following: "%s"', $name, implode('", "', $opt['parts'])));
        }

        return $opt['parts'][$name];
    }

    protected function createSection(string $name): array
    {
        $opt = $this->getOptions();
        $label = $this->getSQLPartLabel($name);

        return [
            // Begin
            $opt['section_prefix'].
            $opt['section_begin_prefix'].
            $label.
            $opt['section_begin_sufix'].
            $opt['section_sufix'],
            // End
            $opt['section_prefix'].
            $opt['section_end_prefix'].
            $label.
            $opt['section_end_sufix'].
            $opt['section_sufix'],
        ];
    }

    protected function createPlaceholder(string $name): string
    {
        $opt = $this->getOptions();
        $label = $this->getSQLPartLabel($name);

        return $opt['section_prefix'].$label.$opt['section_sufix'];
    }

    /**
     * Returns FALSE when not found, array(position, length) when found and -1 when there is an error.
     */
    protected function findSection(string $name, string $str): false|array|int
    {
        list($secBegin, $secEnd) = $this->createSection($name);
        $bPos = mb_stripos($str, $secBegin);
        $ePos = mb_stripos($str, $secEnd);
        if (false === $bPos && false === $ePos) {
            return false;
        }
        if (false === $bPos || false === $ePos || $ePos < $bPos) {
            return -1;
        }

        return [$bPos, $ePos - $bPos + mb_strlen($secEnd), mb_strlen($secBegin), mb_strlen($secEnd)];
    }

    /**
     * Returns FALSE when not found, array(position, length) when found.
     */
    protected function findPlaceholder(string $name, string $str): false|array
    {
        $placeholder = $this->createPlaceholder($name);
        $pos = mb_stripos($str, $placeholder);
        if (false === $pos) {
            return false;
        }

        return [$pos, mb_strlen($placeholder)];
    }

    protected function hasPlaceholderOrSection(string $name, string $str): bool|string
    {
        return \is_array($this->findPlaceholder($name, $str)) || \is_array($this->findSection($name, $str));
    }

    protected function replaceSection(string $str, string $val, string $nam): string
    {
        $sec = $this->findSection($nam, $str);
        if (false === $sec) {
            return $str;
        }
        if (!\is_array($sec)) {
            list($secBegin, $secEnd) = $this->createSection($nam);
            throw new \RuntimeException(\sprintf('Invalid SQL section definition. The section must start with "%s" and end with "%s"', $secBegin, $secEnd));
        }
        $begin = array_shift($sec);
        $length = array_shift($sec);
        if ('' === $val) {
            $beginLength = array_shift($sec);
            $endLength = array_shift($sec);
            $str = substr_replace($str, '', $begin, $beginLength);

            return substr_replace($str, '', $begin + $length - $endLength - $beginLength, $endLength);
        }

        $str = substr_replace($str, '', $begin, $length);

        return substr_replace($str, $val, $begin, 0);
    }

    protected function replacePlaceholder(string $str, string $val, string $nam): string
    {
        $placeholder = $this->findPlaceholder($nam, $str);
        if (false === $placeholder) {
            return $str;
        }
        $begin = array_shift($placeholder);
        $length = array_shift($placeholder);
        $str = substr_replace($str, '', $begin, $length);

        return substr_replace($str, empty($val) ? '' : $val, $begin, 0);
    }

    protected function replaceSqlPart(string $sql, string $partValue, string $partName, array $options = []): string
    {
        $options = array_replace_recursive([
            'placeholder' => [
                'prefix' => '',
                'sufix' => '',
            ],
            'section' => [
                'prefix' => '',
                'sufix' => '',
            ],
        ], $options);

        $val = empty($partValue) ? '' : $options['section']['prefix'].$partValue.$options['section']['sufix'];
        $sql = $this->replaceSection($sql, $val, $partName);

        $val = empty($partValue) ? '' : $options['placeholder']['prefix'].$partValue.$options['placeholder']['sufix'];

        return $this->replacePlaceholder($sql, $val, $partName);
    }

    protected function replaceWherePart(string $sql, string $where): string
    {
        return $this->replaceSqlPart($sql, mb_trim($where), self::PART_WHERE, [
            'placeholder' => [
                'prefix' => ' WHERE ',
            ],
        ]);
    }

    protected function replaceGroupByPart(string $sql, string $groupBy): string
    {
        return $this->replaceSqlPart($sql, mb_trim($groupBy), self::PART_GROUP_BY, [
            'placeholder' => [
                'prefix' => ' GROUP BY ',
            ],
        ]);
    }

    protected function replaceHavingPart(string $sql, string $having): string
    {
        return $this->replaceSqlPart($sql, $having, self::PART_HAVING, [
            'placeholder' => [
                'prefix' => ' HAVING ',
            ],
        ]);
    }

    protected function replaceOrderByPart(string $sql, string $orderBy): string
    {
        return $this->replaceSqlPart($sql, $orderBy, self::PART_ORDER_BY, [
            'placeholder' => [
                'prefix' => ' ORDER BY ',
            ],
        ]);
    }

    public function formatSqlWhere(string $sql, QueryParts|string $where): string
    {
        if ($where instanceof QueryParts) {
            $where = $where->getWhereSqlReduced();
        }
        $newSql = $this->replaceWherePart($sql, $where);
        if (0 === strcmp($newSql, $sql) && mb_trim($where)) {
            $sql = $this->replaceWherePart(
                'SELECT * FROM ('.\PHP_EOL.$sql.\PHP_EOL.')'.
                $this->createPlaceholder(self::PART_WHERE).\PHP_EOL, $where
            );
        } else {
            $sql = $newSql;
        }

        return $sql;
    }

    public function formatSqlGroupBy(string $sql, QueryParts|string $groupBy): string
    {
        if ($groupBy instanceof QueryParts) {
            $groupBy = $groupBy->getGroupBySqlReduced();
        }
        $newSql = $this->replaceGroupByPart($sql, $groupBy);
        if (0 === strcmp($newSql, $sql) && mb_trim($groupBy)) {
            $sql = $this->replaceGroupByPart(
                'SELECT * FROM ('.\PHP_EOL.$sql.\PHP_EOL.')'.
                $this->createPlaceholder(self::PART_GROUP_BY).\PHP_EOL, $groupBy
            );
        } else {
            $sql = $newSql;
        }

        return $sql;
    }

    public function formatSqlHaving(string $sql, QueryParts|string $having): string
    {
        if ($having instanceof QueryParts) {
            $having = $having->getHavingSqlReduced();
        }
        $newSql = $this->replaceHavingPart($sql, $having);
        if (0 === strcmp($newSql, $sql) && mb_trim($having)) {
            $sql = $this->replaceHavingPart(
                'SELECT * FROM ('.\PHP_EOL.$sql.\PHP_EOL.')'.
                $this->createPlaceholder(self::PART_HAVING).\PHP_EOL, $having
            );
        } else {
            $sql = $newSql;
        }

        return $sql;
    }

    public function formatSqlOrderBy(string $sql, QueryParts|string $orderBy): string
    {
        if ($orderBy instanceof QueryParts) {
            $orderBy = $orderBy->getOrderBySqlReduced();
        }
        $newSql = $this->replaceOrderByPart($sql, $orderBy);
        if (0 === strcmp($newSql, $sql) && mb_trim($orderBy)) {
            $sql = $this->replaceOrderByPart(
                'SELECT * FROM ('.\PHP_EOL.$sql.\PHP_EOL.')'.$this->createPlaceholder(self::PART_ORDER_BY).\PHP_EOL, $orderBy
            );
        } else {
            $sql = $newSql;
        }

        return $sql;
    }

    public function formatSqlParts(string $sql, QueryParts $parts): string
    {
        $sWhere = $this->hasPlaceholderOrSection(self::PART_WHERE, $sql);
        $sGroupBy = $this->hasPlaceholderOrSection(self::PART_GROUP_BY, $sql);
        $sHaving = $this->hasPlaceholderOrSection(self::PART_HAVING, $sql);
        $sOrderBy = $this->hasPlaceholderOrSection(self::PART_ORDER_BY, $sql);

        $pWhere = $parts->hasWhere();
        $pGroupby = $parts->hasGroupBy();
        $pHaving = $parts->hasHaving();
        $pOrderBy = $parts->hasOrderBy();

        $wsql = 'SELECT * FROM ('.\PHP_EOL.$sql.\PHP_EOL.')';
        $wrapped = false;

        if (!$sWhere && $pWhere) {
            $sql = ($wrapped ? $sql : $wsql).\PHP_EOL.$this->createPlaceholder(self::PART_WHERE);
            $wrapped = true;
        }
        if (!$sGroupBy && $pGroupby) {
            $sql = ($wrapped ? $sql : $wsql).\PHP_EOL.$this->createPlaceholder(self::PART_GROUP_BY);
            $wrapped = true;
        }
        if (!$sHaving && $pHaving) {
            $sql = ($wrapped ? $sql : $wsql).\PHP_EOL.$this->createPlaceholder(self::PART_HAVING);
            $wrapped = true;
        }
        if (!$sOrderBy && $pOrderBy) {
            $sql = ($wrapped ? $sql : $wsql).\PHP_EOL.$this->createPlaceholder(self::PART_ORDER_BY);
            $wrapped = true;
        }

        $sql = $this->formatSqlWhere($sql, $parts);
        $sql = $this->formatSqlGroupBy($sql, $parts);
        $sql = $this->formatSqlHaving($sql, $parts);
        $sql = $this->formatSqlOrderBy($sql, $parts);

        return $sql;
    }
}
