<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Tools;

use Kraz\ReadModelDoctrine\Tools\SqlOrderByStripper;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(SqlOrderByStripper::class)]
final class SqlOrderByStripperTest extends TestCase
{
    /** @return iterable<string, array{string, string}> */
    public static function provideStrippedSql(): iterable
    {
        yield 'simple clause' => [
            'SELECT * FROM t ORDER BY a',
            'SELECT * FROM t',
        ];

        yield 'case insensitive, multiline, multiple columns' => [
            "SELECT *\nFROM t\norder  BY a DESC, b ASC\n",
            "SELECT *\nFROM t",
        ];

        yield 'clause with expressions and functions' => [
            'SELECT * FROM t ORDER BY COALESCE(a, b) DESC NULLS LAST, LOWER(c)',
            'SELECT * FROM t',
        ];

        yield 'limit is kept' => [
            'SELECT * FROM t ORDER BY a LIMIT 10',
            "SELECT * FROM t\nLIMIT 10",
        ];

        yield 'for update is kept' => [
            'SELECT * FROM t ORDER BY a FOR UPDATE',
            "SELECT * FROM t\nFOR UPDATE",
        ];

        yield 'nested clause is kept' => [
            'SELECT * FROM (SELECT * FROM t ORDER BY a) x ORDER BY b',
            'SELECT * FROM (SELECT * FROM t ORDER BY a) x',
        ];

        yield 'union' => [
            'SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a',
            'SELECT a FROM t1 UNION SELECT a FROM t2',
        ];

        yield 'string literal containing the keywords' => [
            "SELECT 'ORDER BY x' AS s FROM t ORDER BY a",
            "SELECT 'ORDER BY x' AS s FROM t",
        ];

        yield 'string literal with doubled quote' => [
            "SELECT * FROM t WHERE a = 'it''s' ORDER BY a",
            "SELECT * FROM t WHERE a = 'it''s'",
        ];

        yield 'quoted identifiers containing the keywords' => [
            'SELECT "order by", `order by`, [order by] FROM t ORDER BY 1',
            'SELECT "order by", `order by`, [order by] FROM t',
        ];

        yield 'quoted column names in the clause' => [
            'SELECT * FROM "t" ORDER BY "t"."created at" DESC, `name` ASC, [dbo].[order] NULLS FIRST',
            'SELECT * FROM "t"',
        ];

        yield 'quoted column names before the clause' => [
            'SELECT "t"."id", `t`.`name` FROM "t" WHERE "t"."order" = 1 AND `group` IS NULL ORDER BY "t"."id"',
            'SELECT "t"."id", `t`.`name` FROM "t" WHERE "t"."order" = 1 AND `group` IS NULL',
        ];

        yield 'quoted column names with escaped quotes' => [
            'SELECT "say ""hi""", `back``tick`, [bracket]]] FROM t ORDER BY "say ""hi""" DESC, `back``tick`',
            'SELECT "say ""hi""", `back``tick`, [bracket]]] FROM t',
        ];

        yield 'quoted column names in a nested clause' => [
            'SELECT * FROM (SELECT "id" FROM "t" ORDER BY "t"."name") x ORDER BY x."id"',
            'SELECT * FROM (SELECT "id" FROM "t" ORDER BY "t"."name") x',
        ];

        yield 'comment between order and by is kept' => [
            'SELECT * FROM t ORDER /* c */ BY a',
            "SELECT * FROM t\n/* c */",
        ];

        yield 'comments inside the clause are kept' => [
            "SELECT * FROM t ORDER BY a, -- first\n b /* second */, c",
            "SELECT * FROM t\n-- first\n/* second */",
        ];

        yield 'trailing comment is kept' => [
            'SELECT * FROM t ORDER BY a /* keep me */',
            "SELECT * FROM t\n/* keep me */",
        ];

        yield 'where section is untouched, order by section markers are kept' => [
            "SELECT r.* FROM (SELECT * FROM t) r\nWHERE /*#WHERE_B#*/r.\"active\" = 1 AND r.\"name\" <> 'ORDER BY'/*#WHERE_E#*/\nORDER BY /*#ORDERBY_B#*/r.\"name\" DESC/*#ORDERBY_E#*/",
            "SELECT r.* FROM (SELECT * FROM t) r\nWHERE /*#WHERE_B#*/r.\"active\" = 1 AND r.\"name\" <> 'ORDER BY'/*#WHERE_E#*/\n/*#ORDERBY_B#*/\n/*#ORDERBY_E#*/",
        ];

        yield 'section markers inside the clause are kept' => [
            "SELECT r.* FROM (SELECT * FROM t) r\n/*#WHERE#*/\norder by /*#ORDERBY_B#*/r.\"storedAt\", r.\"uid\"/*#ORDERBY_E#*/\n",
            "SELECT r.* FROM (SELECT * FROM t) r\n/*#WHERE#*/\n/*#ORDERBY_B#*/\n/*#ORDERBY_E#*/",
        ];

        yield 'comment before the terminator is kept' => [
            "SELECT * FROM t ORDER BY a -- sort\nLIMIT 5",
            "SELECT * FROM t\n-- sort\nLIMIT 5",
        ];

        yield 'one line comment before the clause is kept' => [
            "SELECT * FROM t -- comment\nORDER BY a LIMIT 5",
            "SELECT * FROM t -- comment\nLIMIT 5",
        ];

        yield 'order siblings by' => [
            'SELECT * FROM t CONNECT BY PRIOR id = parent_id ORDER SIBLINGS BY name',
            'SELECT * FROM t CONNECT BY PRIOR id = parent_id',
        ];

        yield 'postgres cast is not a parameter' => [
            'SELECT * FROM t ORDER BY a::text',
            'SELECT * FROM t',
        ];

        yield 'postgres double question mark is not a parameter' => [
            "SELECT * FROM t ORDER BY data ?? 'key'",
            'SELECT * FROM t',
        ];

        yield 'parameters before the clause' => [
            'SELECT * FROM t WHERE a = :a AND b = ? ORDER BY a',
            'SELECT * FROM t WHERE a = :a AND b = ?',
        ];
    }

    #[DataProvider('provideStrippedSql')]
    public function testStripsTopLevelOrderBy(string $sql, string $expected): void
    {
        self::assertSame($expected, new SqlOrderByStripper()->strip($sql));
    }

    /** @return iterable<string, array{string}> */
    public static function provideUntouchedSql(): iterable
    {
        yield 'no clause' => ['SELECT * FROM t WHERE a = 1'];
        yield 'empty' => [''];
        yield 'column names containing the keyword' => ['SELECT order_id, t.order FROM t GROUP BY order_id'];
        yield 'window function' => ['SELECT ROW_NUMBER() OVER (ORDER BY a) AS rn FROM t'];
        yield 'sub-select' => ['SELECT * FROM (SELECT * FROM t ORDER BY a LIMIT 1) x'];
        yield 'keywords in one line comment' => ["SELECT * FROM t -- ORDER BY a\n"];
        yield 'keywords in multi line comment' => ['SELECT * FROM t /* ORDER BY a */'];
        yield 'keywords in string literal' => ["SELECT 'ORDER BY a' FROM t"];
        yield 'named parameter in the clause' => ['SELECT * FROM t ORDER BY CASE WHEN :sort = 1 THEN a ELSE b END'];
        yield 'positional parameter in the clause' => ['SELECT * FROM t ORDER BY CASE WHEN ? = 1 THEN a ELSE b END'];
        yield 'offset requires the clause' => ['SELECT * FROM t ORDER BY a OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY'];
        yield 'fetch first requires the clause' => ['SELECT * FROM t ORDER BY a FETCH FIRST 5 ROWS ONLY'];
    }

    #[DataProvider('provideUntouchedSql')]
    public function testKeepsSqlWithoutTopLevelOrderBy(string $sql): void
    {
        self::assertSame($sql, new SqlOrderByStripper()->strip($sql));
    }

    public function testBackslashEscapingInStringLiterals(): void
    {
        $sql = "SELECT * FROM t WHERE a = 'it\\'s' ORDER BY a";

        // MySQL: the backslash escapes the quote, so the literal ends before ORDER BY.
        self::assertSame("SELECT * FROM t WHERE a = 'it\\'s'", new SqlOrderByStripper(true)->strip($sql));

        // ANSI SQL: the backslash is a regular character, the second quote opens an unterminated literal.
        self::assertSame($sql, new SqlOrderByStripper(false)->strip($sql));
    }
}
