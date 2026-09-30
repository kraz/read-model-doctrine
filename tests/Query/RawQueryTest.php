<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Query;

use ArrayObject;
use Doctrine\DBAL\Connection;
use Doctrine\ORM\EntityManagerInterface;
use Kraz\ReadModelDoctrine\Exception\NonUniqueResultException;
use Kraz\ReadModelDoctrine\Exception\NoResultException;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use stdClass;

use function array_column;
use function array_map;
use function iterator_to_array;

#[CoversClass(RawQuery::class)]
final class RawQueryTest extends TestCase
{
    private EntityManagerInterface $em;
    private Connection $connection;

    protected function setUp(): void
    {
        $this->em         = ORMTestKit::createEntityManager();
        $this->connection = $this->em->getConnection();
        $this->seed();
    }

    private function seed(): void
    {
        $rows = [
            [1, 'Alice', 'alice@example.com', 'eng', 30, 1],
            [2, 'Bob', 'bob@example.com', 'eng', 25, 1],
            [3, 'Charlie', 'charlie@example.com', 'sales', 35, 0],
            [4, 'Dave', '', 'sales', 40, 1],
            [5, 'Eve', null, 'support', 28, 1],
        ];
        foreach ($rows as [$id, $name, $email, $dept, $age, $active]) {
            $this->connection->executeStatement(
                'INSERT INTO test_entity (id, name, email, department, age, active) VALUES (?, ?, ?, ?, ?, ?)',
                [$id, $name, $email, $dept, $age, $active],
            );
        }
    }

    /** @phpstan-return RawQuery<array<string, mixed>|object> */
    private function makeQuery(string $sql = 'SELECT * FROM test_entity ORDER BY id ASC'): RawQuery
    {
        $query = new RawQuery($this->connection);
        $query->setSql($sql);

        return $query;
    }

    // -------------------------------------------------------------------------
    // toIterable
    // -------------------------------------------------------------------------

    public function testToIterableReturnsAllRows(): void
    {
        $query = $this->makeQuery();
        $rows  = iterator_to_array($query->toIterable());

        self::assertCount(5, $rows);
        self::assertIsArray($rows[0]);
        self::assertIsArray($rows[4]);
        self::assertSame('1', (string) $rows[0]['id']);
        self::assertSame('5', (string) $rows[4]['id']);
    }

    public function testToIterableAppliesItemNormalizer(): void
    {
        $query = new RawQuery($this->connection, [
            'item_normalizer' => static fn (array $row): string => $row['name'],
        ]);
        $query->setSql('SELECT * FROM test_entity ORDER BY id ASC');

        $names = iterator_to_array($query->toIterable());

        self::assertSame(['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'], $names);
    }

    public function testToIterableWithMaxResults(): void
    {
        $query = $this->makeQuery();
        $query->setMaxResults(2);

        $rows = iterator_to_array($query->toIterable());

        self::assertCount(2, $rows);
        self::assertIsArray($rows[0]);
        self::assertIsArray($rows[1]);
        self::assertSame('1', (string) $rows[0]['id']);
        self::assertSame('2', (string) $rows[1]['id']);
    }

    public function testToIterableWithFirstResult(): void
    {
        $query = $this->makeQuery();
        $query->setMaxResults(2)->setFirstResult(3);

        $rows = iterator_to_array($query->toIterable());

        self::assertCount(2, $rows);
        self::assertIsArray($rows[0]);
        self::assertIsArray($rows[1]);
        self::assertSame('4', (string) $rows[0]['id']);
        self::assertSame('5', (string) $rows[1]['id']);
    }

    public function testToIterableCanBeCalledMultipleTimes(): void
    {
        $query = $this->makeQuery();

        $first  = iterator_to_array($query->toIterable());
        $second = iterator_to_array($query->toIterable());

        self::assertCount(5, $first);
        self::assertCount(5, $second);
    }

    public function testToIterableWithBoundParameters(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity WHERE department = :dept ORDER BY id ASC');
        $query->setParameter('dept', 'eng');

        $rows = iterator_to_array($query->toIterable());

        self::assertCount(2, $rows);
        self::assertSame([1, 2], array_column($rows, 'id'));
    }

    // -------------------------------------------------------------------------
    // getResult / getArrayResult
    // -------------------------------------------------------------------------

    public function testGetResultReturnsArray(): void
    {
        $rows = $this->makeQuery()->getResult();

        self::assertCount(5, $rows);
        self::assertIsArray($rows[0]);
    }

    public function testGetResultAppliesNormalizer(): void
    {
        $query = new RawQuery($this->connection, [
            'item_normalizer' => static fn (array $row): int => (int) $row['id'],
        ]);
        $query->setSql('SELECT * FROM test_entity ORDER BY id ASC');

        self::assertSame([1, 2, 3, 4, 5], $query->getResult());
    }

    public function testGetArrayResultCallsNormalizerWhenItReturnsArray(): void
    {
        $called = false;
        $query  = new RawQuery($this->connection, [
            'item_normalizer' => static function (array $row) use (&$called): array {
                $called = true;

                return $row;
            },
        ]);
        $query->setSql('SELECT * FROM test_entity ORDER BY id ASC');

        $rows = $query->getArrayResult();

        self::assertCount(5, $rows);
        self::assertTrue($called, 'getArrayResult() must invoke normalizer when it returns array');
    }

    public function testGetArrayResultSkipsNormalizerWhenItReturnsObject(): void
    {
        $called = false;
        $query  = new RawQuery($this->connection, [
            'item_normalizer' => static function (array $row) use (&$called): stdClass {
                $called = true;
                $obj    = new stdClass();
                foreach ($row as $k => $v) {
                    $obj->$k = $v;
                }

                return $obj;
            },
        ]);
        $query->setSql('SELECT * FROM test_entity ORDER BY id ASC');

        $rows = $query->getArrayResult();

        self::assertCount(5, $rows);
        self::assertFalse($called, 'getArrayResult() must not invoke normalizer when it returns object');
    }

    public function testGetArrayResultCallsNormalizerWithUnionArrayReturnType(): void
    {
        $called = false;
        $query  = new RawQuery($this->connection, [
            /** @phpstan-ignore return.unusedType */
            'item_normalizer' => static function (array $row) use (&$called): array|stdClass {
                $called = true;

                return $row;
            },
        ]);
        $query->setSql('SELECT * FROM test_entity ORDER BY id ASC');

        $rows = $query->getArrayResult();

        self::assertCount(5, $rows);
        self::assertTrue($called, 'getArrayResult() must invoke normalizer when union return type includes array');
    }

    public function testGetArrayResultSkipsNormalizerWithNonArrayUnionReturnType(): void
    {
        $called = false;
        $query  = new RawQuery($this->connection, [
            /** @phpstan-ignore return.unusedType */
            'item_normalizer' => static function (array $row) use (&$called): stdClass|ArrayObject {
                $called = true;

                return new stdClass();
            },
        ]);
        $query->setSql('SELECT * FROM test_entity ORDER BY id ASC');

        $rows = $query->getArrayResult();

        self::assertCount(5, $rows);
        self::assertFalse($called, 'getArrayResult() must not invoke normalizer when union return type excludes array');
    }

    public function testGetArrayResultSkipsNormalizerWithNoReturnType(): void
    {
        $called = false;
        $query  = new RawQuery($this->connection, [
            'item_normalizer' => static function (array $row) use (&$called) {
                $called = true;

                return $row;
            },
        ]);
        $query->setSql('SELECT * FROM test_entity ORDER BY id ASC');

        $rows = $query->getArrayResult();

        self::assertCount(5, $rows);
        self::assertFalse($called, 'getArrayResult() must not invoke normalizer when return type is absent');
    }

    // -------------------------------------------------------------------------
    // getSingleResult
    // -------------------------------------------------------------------------

    public function testGetSingleResultReturnsOneRow(): void
    {
        $query = $this->makeQuery('SELECT * FROM test_entity WHERE id = 1');
        $row   = $query->getSingleResult();

        self::assertIsArray($row);
        self::assertSame('1', (string) $row['id']);
    }

    public function testGetSingleResultThrowsNoResultException(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity WHERE id = 999');

        $this->expectException(NoResultException::class);
        $query->getSingleResult();
    }

    public function testGetSingleResultThrowsNonUniqueResultException(): void
    {
        $this->expectException(NonUniqueResultException::class);
        $this->makeQuery()->getSingleResult();
    }

    // -------------------------------------------------------------------------
    // getOneOrNullResult
    // -------------------------------------------------------------------------

    public function testGetOneOrNullResultReturnsRow(): void
    {
        $query = $this->makeQuery('SELECT * FROM test_entity WHERE id = 2');
        $row   = $query->getOneOrNullResult();

        self::assertNotNull($row);
        self::assertIsArray($row);
        self::assertSame('2', (string) $row['id']);
    }

    public function testGetOneOrNullResultReturnsNullWhenEmpty(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity WHERE id = 999');

        self::assertNull($query->getOneOrNullResult());
    }

    public function testGetOneOrNullResultThrowsNonUniqueResultException(): void
    {
        $this->expectException(NonUniqueResultException::class);
        $this->makeQuery()->getOneOrNullResult();
    }

    // -------------------------------------------------------------------------
    // getCount
    // -------------------------------------------------------------------------

    public function testGetCountReturnsTotal(): void
    {
        self::assertSame(5, $this->makeQuery()->getCount());
    }

    public function testGetCountIgnoresPagination(): void
    {
        $query = $this->makeQuery();
        $query->setMaxResults(2)->setFirstResult(0);

        self::assertSame(5, $query->getCount());
    }

    public function testGetCountReturnsCachedValue(): void
    {
        $query = $this->makeQuery();

        $first  = $query->getCount();
        $second = $query->getCount();

        self::assertSame($first, $second);
    }

    public function testGetCountReturnsZeroForEmptyResultSet(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity WHERE 1=0');

        self::assertSame(0, $query->getCount());
    }

    // -------------------------------------------------------------------------
    // close
    // -------------------------------------------------------------------------

    public function testCloseClosesStatementAndAllowsRequery(): void
    {
        $query = $this->makeQuery();

        $query->close();

        // Should be executable again after close.
        $rows = iterator_to_array($query->toIterable());
        self::assertCount(5, $rows);
    }

    // -------------------------------------------------------------------------
    // setParameter / setParameters
    // -------------------------------------------------------------------------

    public function testSetParameterFiltersRows(): void
    {
        $query = $this->makeQuery('SELECT * FROM test_entity WHERE id = :id');
        $query->setParameter('id', 3);

        $rows = $query->getResult();

        self::assertCount(1, $rows);
        self::assertIsArray($rows[0]);
        self::assertSame('3', (string) $rows[0]['id']);
    }

    public function testSetParametersReplacesAll(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity WHERE department = :dept ORDER BY id ASC');
        $query->setParameters(['dept' => 'sales']);

        $rows = $query->getResult();

        self::assertCount(2, $rows);
        self::assertSame([3, 4], array_column($rows, 'id'));
    }

    // -------------------------------------------------------------------------
    // getConnection
    // -------------------------------------------------------------------------

    public function testGetConnectionReturnsInjectedConnection(): void
    {
        $query = new RawQuery($this->connection);
        self::assertSame($this->connection, $query->getConnection());
    }

    // -------------------------------------------------------------------------
    // setCountSql
    // -------------------------------------------------------------------------

    public function testCustomCountSqlIsUsed(): void
    {
        $query = $this->makeQuery();
        $query->setCountSql('SELECT 42');

        self::assertSame(42, $query->getCount());
    }

    // -------------------------------------------------------------------------
    // getCountSql
    // -------------------------------------------------------------------------

    public function testGetCountSqlStripsTopLevelOrderBy(): void
    {
        $query = $this->makeQuery('SELECT * FROM test_entity ORDER BY name DESC');

        $countSql = $query->getCountSql();

        self::assertNotNull($countSql);
        self::assertStringContainsString('SELECT COUNT(*) FROM (', $countSql);
        self::assertStringContainsString('SELECT * FROM test_entity', $countSql);
        self::assertStringNotContainsStringIgnoringCase('ORDER BY', $countSql);
        self::assertSame(5, $query->getCount());
    }

    public function testGetCountSqlStripsOrderByFromQueryParts(): void
    {
        $query = $this->makeQuery('SELECT * FROM test_entity');
        $query->sql()->orderBy('name', 'DESC');

        self::assertStringContainsString('ORDER BY name DESC', $query->getExtendedSql());

        $countSql = $query->getCountSql();

        self::assertNotNull($countSql);
        self::assertStringNotContainsStringIgnoringCase('ORDER BY', $countSql);
        self::assertSame(5, $query->getCount());
    }

    public function testGetCountSqlStripsOrderBySectionWithDefaultContent(): void
    {
        $query = $this->makeQuery('SELECT * FROM test_entity ORDER BY /*#ORDERBY_B#*/id/*#ORDERBY_E#*/');

        self::assertStringContainsString('ORDER BY id', $query->getExtendedSql());

        $countSql = $query->getCountSql();

        self::assertNotNull($countSql);
        self::assertStringNotContainsStringIgnoringCase('ORDER BY', $countSql);
        self::assertSame(5, $query->getCount());
    }

    public function testGetCountSqlStripsOrderBySectionInsideTheClause(): void
    {
        $sql = <<<'SQL'
            select r.* from (
                select t.id as "id", t.name as "name", t.active as "active" from test_entity t
            ) r
            /*#WHERE#*/
            order by /*#ORDERBY_B#*/r."name", r."id"/*#ORDERBY_E#*/
            SQL;

        $query = $this->makeQuery($sql);
        $query->sql()->where('r."active" = 1');

        self::assertStringContainsString('order by r."name", r."id"', $query->getExtendedSql());

        $countSql = $query->getCountSql();

        self::assertNotNull($countSql);
        self::assertStringNotContainsStringIgnoringCase('ORDER BY', $countSql);
        self::assertStringNotContainsString('/*#', $countSql);
        self::assertStringContainsString('WHERE r."active" = 1', $countSql);
        self::assertSame(4, $query->getCount());

        // The order by part replaces the section content and must not leak into the count SQL either.
        $query->sql()->orderBy('r."id"', 'DESC');

        self::assertStringContainsString('order by r."id" DESC', $query->getExtendedSql());
        self::assertStringNotContainsStringIgnoringCase('ORDER BY', (string) $query->getCountSql());
        self::assertSame(4, $query->getCount());

        $rows = $query->getResult();
        self::assertIsArray($rows[0]);
        self::assertSame('5', (string) $rows[0]['id']);
    }

    public function testGetCountSqlWithWhereAndOrderBySections(): void
    {
        $sql = <<<'SQL'
            select r.* from (
                select t.id as "id", t.name as "name", t.department as "department", t.active as "active" from test_entity t
            ) r
            where /*#WHERE_B#*/r."active" = 1/*#WHERE_E#*/
            order by /*#ORDERBY_B#*/r."name" desc/*#ORDERBY_E#*/
            SQL;

        // Default section content: the where is applied, the order by is dropped.
        $query    = $this->makeQuery($sql);
        $countSql = $query->getCountSql();

        self::assertNotNull($countSql);
        self::assertStringContainsString('where r."active" = 1', $countSql);
        self::assertStringNotContainsStringIgnoringCase('ORDER BY', $countSql);
        self::assertStringNotContainsString('/*#', $countSql);
        self::assertSame(4, $query->getCount());

        // The where part replaces the section content in both the main and the count SQL.
        $query->sql()->where('r."department" = \'sales\'');
        $countSql = $query->getCountSql();

        self::assertNotNull($countSql);
        self::assertStringContainsString('where r."department" = \'sales\'', $countSql);
        self::assertStringNotContainsString('r."active" = 1', $countSql);
        self::assertStringNotContainsStringIgnoringCase('ORDER BY', $countSql);
        self::assertStringNotContainsString('/*#', $countSql);
        self::assertSame(2, $query->getCount());

        // An order by part on top only affects the main SQL.
        $query->sql()->orderBy('r."id"', 'ASC');
        $extendedSql = $query->getExtendedSql();

        self::assertStringContainsString('where r."department" = \'sales\'', $extendedSql);
        self::assertStringContainsString('order by r."id" ASC', $extendedSql);
        self::assertStringNotContainsStringIgnoringCase('ORDER BY', (string) $query->getCountSql());
        self::assertSame(2, $query->getCount());
        self::assertSame(['3', '4'], array_map(static fn (array $row): string => (string) $row['id'], $query->getArrayResult()));
    }

    public function testGetCountSqlKeepsNestedOrderByAndWhere(): void
    {
        $query = $this->makeQuery('SELECT * FROM (SELECT * FROM test_entity ORDER BY id DESC) t ORDER BY name');
        $query->sql()->where('active = 1');

        $countSql = $query->getCountSql();

        self::assertNotNull($countSql);
        self::assertStringContainsString('ORDER BY id DESC', $countSql);
        self::assertStringContainsString('active = 1', $countSql);
        self::assertStringNotContainsString('ORDER BY name', $countSql);
        self::assertSame(4, $query->getCount());
    }

    public function testGetCountSqlKeepsOrderByWhenStrippingIsDisabled(): void
    {
        $query = new RawQuery($this->connection, ['strip_count_order_by' => false]);
        $query->setSql('SELECT * FROM test_entity ORDER BY name DESC');
        $query->sql()->addOrderBy('id', 'ASC');

        $countSql = $query->getCountSql();

        self::assertNotNull($countSql);
        self::assertStringContainsString('ORDER BY name DESC', $countSql);
        self::assertStringContainsString('ORDER BY id ASC', $countSql);
        self::assertSame(5, $query->getCount());
    }
}
