<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tools;

use function array_filter;
use function ctype_space;
use function implode;
use function in_array;
use function preg_match;
use function rtrim;
use function strcasecmp;
use function strlen;
use function strpos;
use function strtoupper;
use function substr;
use function trim;

use const PHP_EOL;

/**
 * Removes the top-level ORDER BY clause from a SELECT statement.
 *
 * Sorting does not change the number of rows a query yields, but it often forces the database to sort on columns
 * without a suitable index. Counting the rows through a sub-select therefore performs better without it.
 *
 * ORDER BY clauses nested in parentheses (sub-selects, window functions, aggregates) are left untouched.
 * The top-level clause is kept as well when:
 *  - it contains query parameters ("?" or ":name"), since removing it would break the parameter binding;
 *  - it is followed by OFFSET or FETCH, since some platforms (SQL Server) require ORDER BY for those.
 */
final class SqlOrderByStripper
{
    /** Keywords which end the top-level ORDER BY clause. */
    private const array CLAUSE_TERMINATORS = ['LIMIT', 'OFFSET', 'FETCH', 'FOR', 'OPTION', 'UNION', 'EXCEPT', 'INTERSECT'];

    /** Clause terminators which require an ORDER BY clause on some platforms. */
    private const array ORDER_BY_DEPENDENT_TERMINATORS = ['OFFSET', 'FETCH'];

    private const string WORD_PATTERN = '/[A-Za-z0-9_$@#]+/A';

    /** @param bool $backslashEscapes Whether a backslash escapes the next character inside string literals (MySQL). */
    public function __construct(private bool $backslashEscapes = false)
    {
    }

    public function strip(string $sql): string
    {
        $length        = strlen($sql);
        $depth         = 0;
        $start         = null; // Offset of the top-level ORDER keyword
        $end           = 0;    // Offset right after the last code token of the clause (comments excluded)
        $terminator    = null;
        $hasParameters = false;

        $i = 0;
        while ($i < $length) {
            $commentEnd = self::skipComment($sql, $i);
            if ($commentEnd !== null) {
                $i = $commentEnd;
                continue;
            }

            if (ctype_space($sql[$i])) {
                $i++;
                continue;
            }

            $literalEnd = $this->skipLiteral($sql, $i);
            if ($literalEnd !== null) {
                $i = $end = $literalEnd;
                continue;
            }

            $char = $sql[$i];

            if ($char === '(') {
                $depth++;
                $i = $end = $i + 1;
                continue;
            }

            if ($char === ')') {
                $depth = $depth > 0 ? $depth - 1 : 0;
                $i     = $end = $i + 1;
                continue;
            }

            if ($char === ':') {
                // "::" is a cast (PostgreSQL), a single colon followed by a name is a named parameter
                $colons = self::countRepeated($sql, $i, ':');
                $name   = self::readWord($sql, $i + $colons);
                if ($colons === 1 && $name !== '' && $depth === 0 && $start !== null) {
                    $hasParameters = true;
                }

                $i = $end = $i + $colons + strlen($name);
                continue;
            }

            if ($char === '?') {
                // "??" is an operator (PostgreSQL), a single question mark is a positional parameter
                $marks = self::countRepeated($sql, $i, '?');
                if ($marks === 1 && $depth === 0 && $start !== null) {
                    $hasParameters = true;
                }

                $i = $end = $i + $marks;
                continue;
            }

            $word = self::readWord($sql, $i);
            if ($word === '') {
                $i = $end = $i + 1;
                continue;
            }

            $wordEnd = $i + strlen($word);

            if ($depth === 0) {
                if ($start === null) {
                    if (strcasecmp($word, 'ORDER') === 0 && ($i === 0 || $sql[$i - 1] !== '.')) {
                        $byEnd = $this->matchBy($sql, $wordEnd);
                        if ($byEnd !== null) {
                            $start = $i;
                            $i     = $end = $byEnd;
                            continue;
                        }
                    }
                } elseif (in_array(strtoupper($word), self::CLAUSE_TERMINATORS, true)) {
                    $terminator = strtoupper($word);
                    break;
                }
            }

            $i = $end = $wordEnd;
        }

        if ($start === null || $hasParameters || in_array($terminator, self::ORDER_BY_DEPENDENT_TERMINATORS, true)) {
            return $sql;
        }

        // Comments inside the clause are kept: they may be section markers of the SQL formatter.
        $pieces = [rtrim(substr($sql, 0, $start)), ...$this->extractComments($sql, $start, $end), trim(substr($sql, $end))];

        return implode(PHP_EOL, array_filter($pieces, static fn (string $piece): bool => $piece !== ''));
    }

    /**
     * Collects the comments found in the given range.
     *
     * @return list<string>
     */
    private function extractComments(string $sql, int $from, int $to): array
    {
        $comments = [];
        $i        = $from;
        while ($i < $to) {
            $commentEnd = self::skipComment($sql, $i);
            if ($commentEnd !== null) {
                $comments[] = rtrim(substr($sql, $i, $commentEnd - $i));
                $i          = $commentEnd;
                continue;
            }

            $i = $this->skipLiteral($sql, $i) ?? $i + 1;
        }

        return $comments;
    }

    /**
     * Matches the "BY" (or "SIBLINGS BY") keyword following an "ORDER" keyword which ends at the given offset.
     * Returns the offset right after "BY" or NULL when there is no match.
     */
    private function matchBy(string $sql, int $offset): int|null
    {
        $pos = $this->skipBlank($sql, $offset);
        if ($pos === $offset) {
            return null;
        }

        $word = self::readWord($sql, $pos);
        if (strcasecmp($word, 'SIBLINGS') === 0) {
            $afterSiblings = $pos + strlen($word);
            $pos           = $this->skipBlank($sql, $afterSiblings);
            if ($pos === $afterSiblings) {
                return null;
            }

            $word = self::readWord($sql, $pos);
        }

        if (strcasecmp($word, 'BY') !== 0) {
            return null;
        }

        return $pos + strlen($word);
    }

    /**
     * Skips whitespace and comments starting at the given offset.
     */
    private function skipBlank(string $sql, int $offset): int
    {
        $length = strlen($sql);
        while ($offset < $length) {
            if (ctype_space($sql[$offset])) {
                $offset++;
                continue;
            }

            $commentEnd = self::skipComment($sql, $offset);
            if ($commentEnd === null) {
                break;
            }

            $offset = $commentEnd;
        }

        return $offset;
    }

    /**
     * When a comment starts at the given offset, returns the offset right after it. Returns NULL otherwise.
     */
    private static function skipComment(string $sql, int $offset): int|null
    {
        $pair = substr($sql, $offset, 2);
        if ($pair === '--') {
            $eol = strpos($sql, "\n", $offset + 2);

            return $eol === false ? strlen($sql) : $eol + 1;
        }

        if ($pair === '/*') {
            $close = strpos($sql, '*/', $offset + 2);

            return $close === false ? strlen($sql) : $close + 2;
        }

        return null;
    }

    /**
     * When a string literal or a quoted identifier starts at the given offset, returns the offset right after it.
     * Returns NULL otherwise.
     */
    private function skipLiteral(string $sql, int $offset): int|null
    {
        $char = $sql[$offset];

        if ($char === "'" || $char === '"' || $char === '`') {
            return $this->skipQuoted($sql, $offset, $char, $char !== '`' && $this->backslashEscapes);
        }

        if ($char === '[' && ! self::isPrecededByWord($sql, $offset, 'ARRAY')) {
            $close = strpos($sql, ']', $offset + 1);

            return $close === false ? strlen($sql) : $close + 1;
        }

        return null;
    }

    /**
     * Skips a quoted region starting at the given offset. Doubled quotes (and optionally backslashes) escape the quote.
     */
    private function skipQuoted(string $sql, int $offset, string $quote, bool $backslashEscapes): int
    {
        $length = strlen($sql);
        $i      = $offset + 1;
        while ($i < $length) {
            $char = $sql[$i];
            if ($backslashEscapes && $char === '\\') {
                $i += 2;
                continue;
            }

            if ($char === $quote) {
                if (($sql[$i + 1] ?? '') === $quote) {
                    $i += 2;
                    continue;
                }

                return $i + 1;
            }

            $i++;
        }

        return $length;
    }

    private static function isPrecededByWord(string $sql, int $offset, string $word): bool
    {
        $wordLength = strlen($word);
        if ($offset < $wordLength) {
            return false;
        }

        $previous = substr($sql, $offset - $wordLength, $wordLength);
        if (strcasecmp($previous, $word) !== 0) {
            return false;
        }

        return $offset === $wordLength || self::readWord($sql, $offset - $wordLength - 1) === '';
    }

    /**
     * Reads the identifier/keyword/number starting at the given offset. Returns an empty string when there is none.
     */
    private static function readWord(string $sql, int $offset): string
    {
        if (preg_match(self::WORD_PATTERN, $sql, $matches, 0, $offset) !== 1) {
            return '';
        }

        return $matches[0];
    }

    private static function countRepeated(string $sql, int $offset, string $char): int
    {
        $count  = 0;
        $length = strlen($sql);
        while ($offset + $count < $length && $sql[$offset + $count] === $char) {
            $count++;
        }

        return $count;
    }
}
