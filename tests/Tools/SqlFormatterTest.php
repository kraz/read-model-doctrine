<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Tools;

use Kraz\ReadModelDoctrine\Tools\QueryParts;
use Kraz\ReadModelDoctrine\Tools\SqlFormatter;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;

#[CoversClass(SqlFormatter::class)]
final class SqlFormatterTest extends TestCase
{
    public function testDefaultOptionsAreApplied(): void
    {
        $formatter = new SqlFormatter();
        $options   = $formatter->getOptions();

        self::assertSame('/*#', $options['section_prefix']);
        self::assertSame('#*/', $options['section_sufix']);
        self::assertSame('WHERE', $options['parts']['where']);
        self::assertSame('GROUPBY', $options['parts']['group_by']);
        self::assertSame('HAVING', $options['parts']['having']);
        self::assertSame('ORDERBY', $options['parts']['order_by']);
    }

    public function testReplaceWherePlaceholderInsertsClause(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users /*#WHERE#*/ ORDER BY id';

        $result = $formatter->formatSqlWhere($sql, 'name = :name');

        self::assertSame('SELECT * FROM users  WHERE name = :name ORDER BY id', $result);
    }

    public function testEmptyWhereStripsPlaceholder(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users /*#WHERE#*/ ORDER BY id';

        $result = $formatter->formatSqlWhere($sql, '');

        self::assertSame('SELECT * FROM users  ORDER BY id', $result);
    }

    public function testWhereSection(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users /*#WHERE_B#*/ WHERE deleted_at IS NULL /*#WHERE_E#*/';

        $result = $formatter->formatSqlWhere($sql, 'name = :name');

        // Section replaces the entire region (markers + inner) with the new value (no WHERE prefix added).
        self::assertStringContainsString('name = :name', $result);
        self::assertStringNotContainsString('deleted_at IS NULL', $result);
        self::assertStringNotContainsString('/*#WHERE', $result);
    }

    public function testWhereSectionRemovedWhenValueEmpty(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users /*#WHERE_B#*/ WHERE deleted_at IS NULL /*#WHERE_E#*/ ORDER BY id';

        $result = $formatter->formatSqlWhere($sql, '');

        // Section markers must be removed and the inner content preserved.
        self::assertStringContainsString('WHERE deleted_at IS NULL', $result);
        self::assertStringNotContainsString('/*#WHERE', $result);
    }

    public function testWhereWithoutPlaceholderWrapsQuery(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users';

        $result = $formatter->formatSqlWhere($sql, 'name = :name');

        self::assertStringContainsString('SELECT * FROM (', $result);
        self::assertStringContainsString($sql, $result);
        self::assertStringContainsString(' WHERE name = :name', $result);
    }

    public function testEmptyWhereDoesNotWrapQuery(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users';

        $result = $formatter->formatSqlWhere($sql, '');

        self::assertSame($sql, $result);
    }

    public function testGroupByPlaceholder(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT department, COUNT(*) FROM users /*#GROUPBY#*/';

        $result = $formatter->formatSqlGroupBy($sql, 'department');

        self::assertSame('SELECT department, COUNT(*) FROM users  GROUP BY department', $result);
    }

    public function testHavingPlaceholder(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT id FROM users GROUP BY id /*#HAVING#*/';

        $result = $formatter->formatSqlHaving($sql, 'COUNT(*) > 1');

        self::assertSame('SELECT id FROM users GROUP BY id  HAVING COUNT(*) > 1', $result);
    }

    public function testOrderByPlaceholder(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users /*#ORDERBY#*/';

        $result = $formatter->formatSqlOrderBy($sql, 'name ASC');

        self::assertSame('SELECT * FROM users  ORDER BY name ASC', $result);
    }

    public function testFormatSqlPartsAppliesAllParts(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT department, COUNT(*) AS c FROM users /*#WHERE#*/ /*#GROUPBY#*/ /*#HAVING#*/ /*#ORDERBY#*/';

        $parts = new QueryParts();
        $parts->andWhere('active = 1');
        $parts->addGroupBy('department');
        $parts->andHaving('COUNT(*) > 1');
        $parts->addOrderBy('department', 'ASC');

        $result = $formatter->formatSqlParts($sql, $parts);

        self::assertStringContainsString(' WHERE active = 1', $result);
        self::assertStringContainsString(' GROUP BY department', $result);
        self::assertStringContainsString(' HAVING COUNT(*) > 1', $result);
        self::assertStringContainsString(' ORDER BY department ASC', $result);
        self::assertStringNotContainsString('/*#', $result);
    }

    public function testFormatSqlPartsWrapsWhenNoPlaceholdersPresent(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users';

        $parts = new QueryParts();
        $parts->andWhere('active = 1');
        $parts->addOrderBy('id', 'DESC');

        $result = $formatter->formatSqlParts($sql, $parts);

        self::assertStringContainsString('SELECT * FROM (', $result);
        self::assertStringContainsString($sql, $result);
        self::assertStringContainsString(' WHERE active = 1', $result);
        self::assertStringContainsString(' ORDER BY id DESC', $result);
    }

    public function testFormatSqlPartsWithEmptyPartsDoesNothing(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users';

        $result = $formatter->formatSqlParts($sql, new QueryParts());

        self::assertSame($sql, $result);
    }

    public function testFormatSqlPartsWithQueryPartsArgumentForWhere(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users /*#WHERE#*/';

        $parts = new QueryParts();
        $parts->andWhere('active = 1');

        $result = $formatter->formatSqlWhere($sql, $parts);

        self::assertStringContainsString(' WHERE active = 1', $result);
    }

    public function testInvalidSectionDefinitionThrows(): void
    {
        $formatter = new SqlFormatter();
        // End marker before begin marker is invalid.
        $sql = 'SELECT * /*#WHERE_E#*/ FROM users /*#WHERE_B#*/';

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/Invalid SQL section definition/');

        $formatter->formatSqlWhere($sql, 'name = :name');
    }

    public function testCustomOptionsAreUsed(): void
    {
        $formatter = new SqlFormatter([
            'section_prefix' => '<<',
            'section_sufix' => '>>',
        ]);

        $sql    = 'SELECT * FROM users <<WHERE>>';
        $result = $formatter->formatSqlWhere($sql, 'id = 1');

        self::assertSame('SELECT * FROM users  WHERE id = 1', $result);
    }

    public function testSetOptionsMergesWithDefaults(): void
    {
        $formatter = new SqlFormatter();
        $formatter->setOptions(['section_prefix' => '#@']);

        $opt = $formatter->getOptions();
        self::assertSame('#@', $opt['section_prefix']);
        // Other defaults preserved.
        self::assertSame('#*/', $opt['section_sufix']);
        self::assertSame('WHERE', $opt['parts']['where']);
    }

    public function testSecondInvocationIsIdempotent(): void
    {
        $formatter = new SqlFormatter();
        $sql       = 'SELECT * FROM users /*#WHERE#*/';

        $first  = $formatter->formatSqlWhere($sql, 'a = 1');
        $second = $formatter->formatSqlWhere($first, 'b = 2');

        // After the placeholder is consumed, a subsequent call wraps the result.
        self::assertStringContainsString('a = 1', $second);
        self::assertStringContainsString('SELECT * FROM (', $second);
        self::assertStringContainsString('WHERE b = 2', $second);
    }
}
