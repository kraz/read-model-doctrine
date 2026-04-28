<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Tools;

use BadMethodCallException;
use Doctrine\ORM\Query\Expr;
use InvalidArgumentException;
use Kraz\ReadModelDoctrine\Tests\Fixtures;
use Kraz\ReadModelDoctrine\Tools\QueryParts;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function array_keys;

#[CoversClass(QueryParts::class)]
final class QueryPartsTest extends TestCase
{
    public function testNewInstanceHasNoActiveParts(): void
    {
        $parts = new QueryParts();

        self::assertFalse($parts->hasWhere());
        self::assertFalse($parts->hasGroupBy());
        self::assertFalse($parts->hasHaving());
        self::assertFalse($parts->hasOrderBy());
        self::assertSame('', $parts->getSql());
        self::assertSame('', (string) $parts);
    }

    public function testExprIsLazyAndShared(): void
    {
        $parts = new QueryParts();
        $a     = $parts->expr();
        $b     = $parts->expr();

        self::assertInstanceOf(Expr::class, $a);
        self::assertSame($a, $b);
    }

    public function testWhereSetsAndAndWhereAppendsWithAnd(): void
    {
        $parts = new QueryParts();
        $parts->where('a = 1');
        $parts->andWhere('b = 2');
        $parts->andWhere('c = 3');

        self::assertTrue($parts->hasWhere());
        self::assertSame(' WHERE a = 1 AND b = 2 AND c = 3', $parts->getWhereSql());
        self::assertSame('a = 1 AND b = 2 AND c = 3', $parts->getWhereSqlReduced());
    }

    public function testOrWhereCreatesOrComposite(): void
    {
        $parts = new QueryParts();
        $parts->where('a = 1');
        $parts->orWhere('b = 2');

        self::assertSame('a = 1 OR b = 2', $parts->getWhereSqlReduced());
    }

    public function testWhereCompositeSinglePredicateIsKeptAsIs(): void
    {
        $parts = new QueryParts();
        $orx   = new Expr\Orx(['a = 1', 'b = 2']);
        $parts->where($orx);

        self::assertSame('a = 1 OR b = 2', $parts->getWhereSqlReduced());
    }

    public function testGroupByAndAddGroupBy(): void
    {
        $parts = new QueryParts();
        $parts->groupBy('a');
        $parts->addGroupBy('b', 'c');

        self::assertTrue($parts->hasGroupBy());
        self::assertSame(' GROUP BY a, b, c', $parts->getGroupBySql());
    }

    public function testHavingAndAndHaving(): void
    {
        $parts = new QueryParts();
        $parts->having('COUNT(*) > 1');
        $parts->andHaving('SUM(x) < 10');

        self::assertTrue($parts->hasHaving());
        self::assertSame('COUNT(*) > 1 AND SUM(x) < 10', $parts->getHavingSqlReduced());
    }

    public function testOrHaving(): void
    {
        $parts = new QueryParts();
        $parts->having('a = 1');
        $parts->orHaving('b = 2');

        self::assertSame('a = 1 OR b = 2', $parts->getHavingSqlReduced());
    }

    public function testOrderByAndAddOrderBy(): void
    {
        $parts = new QueryParts();
        $parts->orderBy('a', 'ASC');
        $parts->addOrderBy('b', 'DESC');

        self::assertTrue($parts->hasOrderBy());
        self::assertSame(' ORDER BY a ASC, b DESC', $parts->getOrderBySql());
    }

    public function testOrderByOverridesPreviousOrderBy(): void
    {
        $parts = new QueryParts();
        $parts->orderBy('a', 'ASC');
        $parts->orderBy('b', 'DESC');

        self::assertSame('b DESC', $parts->getOrderBySqlReduced());
    }

    public function testCombinedSqlOutput(): void
    {
        $parts = new QueryParts();
        $parts->andWhere('a = 1');
        $parts->groupBy('a');
        $parts->andHaving('COUNT(*) > 1');
        $parts->addOrderBy('a', 'ASC');

        self::assertSame(
            ' WHERE a = 1 GROUP BY a HAVING COUNT(*) > 1 ORDER BY a ASC',
            (string) $parts,
        );
    }

    public function testResetQueryPart(): void
    {
        $parts = new QueryParts();
        $parts->andWhere('a = 1');
        $parts->addOrderBy('a', 'ASC');

        $parts->resetQueryPart('where');

        self::assertFalse($parts->hasWhere());
        self::assertTrue($parts->hasOrderBy());
    }

    public function testResetQueryPartsAll(): void
    {
        $parts = new QueryParts();
        $parts->andWhere('a = 1');
        $parts->addGroupBy('a');
        $parts->andHaving('x > 0');
        $parts->addOrderBy('a', 'ASC');

        $parts->resetQueryParts();

        self::assertFalse($parts->hasWhere());
        self::assertFalse($parts->hasGroupBy());
        self::assertFalse($parts->hasHaving());
        self::assertFalse($parts->hasOrderBy());
        self::assertSame('', $parts->getSql());
    }

    public function testAppendOnWhereThrows(): void
    {
        $parts = new QueryParts();

        $this->expectException(InvalidArgumentException::class);

        $parts->add('where', 'a = 1', true);
    }

    public function testAppendOnHavingThrows(): void
    {
        $parts = new QueryParts();

        $this->expectException(InvalidArgumentException::class);

        $parts->add('having', 'x > 0', true);
    }

    public function testNamedArgumentsThrow(): void
    {
        $parts = new QueryParts();

        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessageMatches('/unknown named arguments/');

        $parts->andWhere(predicate: 'a = 1');
    }

    public function testCloneDeepCopiesParts(): void
    {
        $parts = new QueryParts();
        $parts->andWhere('a = 1');
        $parts->addOrderBy('a', 'ASC');

        $clone = clone $parts;
        $clone->andWhere('b = 2');
        $clone->addOrderBy('b', 'DESC');

        self::assertSame('a = 1', $parts->getWhereSqlReduced());
        self::assertSame('a = 1 AND b = 2', $clone->getWhereSqlReduced());
        self::assertSame('a ASC', $parts->getOrderBySqlReduced());
        self::assertSame('a ASC, b DESC', $clone->getOrderBySqlReduced());
    }

    public function testAddToCopiesWhereHavingAndOrderByIntoAnotherQueryParts(): void
    {
        $source = new QueryParts();
        $source->andWhere('a = 1');
        $source->andHaving('SUM(x) > 0');
        $source->addOrderBy('a', 'ASC');

        $target = new QueryParts();
        $source->addTo($target);

        self::assertSame('a = 1', $target->getWhereSqlReduced());
        self::assertSame('SUM(x) > 0', $target->getHavingSqlReduced());
        self::assertSame('a ASC', $target->getOrderBySqlReduced());
    }

    public function testAddToDoesNotMutateSource(): void
    {
        $source = new QueryParts();
        $source->andWhere('a = 1');
        $source->addOrderBy('a', 'ASC');

        $target = new QueryParts();
        $source->addTo($target);

        $target->andWhere('b = 2');

        self::assertSame('a = 1', $source->getWhereSqlReduced());
    }

    public function testAddToMergesIntoQueryBuilder(): void
    {
        $em = ORMTestKit::createEntityManager();

        $qb = $em->createQueryBuilder()
            ->select('u')
            ->from(Fixtures\TestEntity::class, 'u');

        $parts = new QueryParts();
        $parts->andWhere('u.name = :name');
        $parts->addOrderBy('u.id', 'DESC');
        $parts->addTo($qb);

        $dql = $qb->getDQL();
        self::assertStringContainsString('WHERE u.name = :name', $dql);
        self::assertStringContainsString('ORDER BY u.id DESC', $dql);
    }

    public function testGetQueryPartReturnsRawValue(): void
    {
        $parts = new QueryParts();
        $parts->andWhere('a = 1');

        $where = $parts->getQueryPart('where');
        self::assertNotNull($where);
        self::assertSame('a = 1', (string) $where);
    }

    public function testGetQueryPartsReturnsAllSlots(): void
    {
        $parts    = new QueryParts();
        $allParts = $parts->getQueryParts();

        self::assertSame(['where', 'groupBy', 'having', 'orderBy'], array_keys($allParts));
        self::assertNull($allParts['where']);
        self::assertSame([], $allParts['groupBy']);
        self::assertNull($allParts['having']);
        self::assertSame([], $allParts['orderBy']);
    }

    public function testHavingWithSinglePredicateIsKeptAsIs(): void
    {
        $parts = new QueryParts();
        $orx   = new Expr\Orx(['a = 1', 'b = 2']);
        $parts->having($orx);

        self::assertSame('a = 1 OR b = 2', $parts->getHavingSqlReduced());
    }
}
