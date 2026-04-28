<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Query;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\ORM\EntityManagerInterface;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;
use function str_contains;
use function strtoupper;

#[CoversClass(RawQueryBuilder::class)]
final class RawQueryBuilderTest extends TestCase
{
    private EntityManagerInterface $em;
    private Connection $connection;

    protected function setUp(): void
    {
        $this->em         = ORMTestKit::createEntityManager();
        $this->connection = $this->em->getConnection();
        $this->seed();
    }

    /** @phpstan-return RawQueryBuilder<array<string, mixed>|object> */
    private function makeBuilder(): RawQueryBuilder
    {
        return new RawQueryBuilder($this->connection);
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

    // -------------------------------------------------------------------------
    // getQuery basics
    // -------------------------------------------------------------------------

    public function testGetQueryReturnsRawQueryInstance(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')->from('test_entity');

        self::assertInstanceOf(RawQuery::class, $qb->getQuery());
    }

    public function testGetQuerySqlDoesNotContainLimit(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')->from('test_entity')->setMaxResults(2)->setFirstResult(1);

        $query = $qb->getQuery();

        // The base SQL stored on the RawQuery should not have LIMIT baked in.
        self::assertFalse(str_contains(strtoupper($query->getSql()), 'LIMIT'));
        self::assertFalse(str_contains(strtoupper($query->getSql()), 'OFFSET'));
    }

    public function testGetQueryTransfersMaxResultsAndFirstResult(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')->from('test_entity')->setMaxResults(3)->setFirstResult(1);

        $query = $qb->getQuery();

        self::assertSame(3, $query->getMaxResults());
        self::assertSame(1, $query->getFirstResult());
    }

    public function testGetQueryRestoresBuilderLimitsAfterwards(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')->from('test_entity')->setMaxResults(3)->setFirstResult(2);

        $qb->getQuery();

        // The builder must be left unchanged so callers can call getQuery() again.
        self::assertSame(3, $qb->getMaxResults());
        self::assertSame(2, $qb->getFirstResult());
    }

    // -------------------------------------------------------------------------
    // Parameter handling
    // -------------------------------------------------------------------------

    public function testGetQueryTransfersNamedParameters(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')
            ->from('test_entity')
            ->where('department = :dept')
            ->setParameter('dept', 'eng');

        $query = $qb->getQuery();

        self::assertSame('eng', $query->getParameter('dept'));
    }

    public function testGetQueryTransfersPositionalParameters(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')
            ->from('test_entity')
            ->where('id = ?')
            ->setParameter(0, 1);

        $query = $qb->getQuery();

        self::assertSame(1, $query->getParameter(0));
    }

    public function testGetQueryTransfersParameterTypes(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')
            ->from('test_entity')
            ->where('id = :id')
            ->setParameter('id', 1, ParameterType::INTEGER);

        $query = $qb->getQuery();

        self::assertSame(ParameterType::INTEGER, $query->getParameterType('id'));
    }

    // -------------------------------------------------------------------------
    // getParameterTypes passthrough
    // -------------------------------------------------------------------------

    public function testGetParameterTypesReturnsTypesFromBuilder(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')
            ->from('test_entity')
            ->where('id = :id')
            ->setParameter('id', 1, ParameterType::INTEGER);

        $types = $qb->getParameterTypes();

        self::assertArrayHasKey('id', $types);
        self::assertSame(ParameterType::INTEGER, $types['id']);
    }

    // -------------------------------------------------------------------------
    // End-to-end execution
    // -------------------------------------------------------------------------

    public function testGetQueryExecutesAndReturnsRows(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')->from('test_entity')->orderBy('id', 'ASC');

        $rows = $qb->getQuery()->getResult();

        self::assertCount(5, $rows);
    }

    public function testGetQueryRespectsPagination(): void
    {
        $qb = $this->makeBuilder();
        $qb->select('*')->from('test_entity')->orderBy('id', 'ASC')
            ->setFirstResult(2)->setMaxResults(2);

        $rows = $qb->getQuery()->getResult();

        self::assertCount(2, $rows);
        self::assertIsArray($rows[0]);
        self::assertIsArray($rows[1]);
        self::assertSame('3', (string) $rows[0]['id']);
        self::assertSame('4', (string) $rows[1]['id']);
    }

    public function testGetQueryWithFilterParameter(): void
    {
        $qb = $this->makeBuilder();
        $qb->select('*')
            ->from('test_entity')
            ->where('department = :dept')
            ->orderBy('id', 'ASC')
            ->setParameter('dept', 'eng');

        $rows = $qb->getQuery()->getResult();

        self::assertCount(2, $rows);
        self::assertIsArray($rows[0]);
        self::assertIsArray($rows[1]);
        self::assertSame('1', (string) $rows[0]['id']);
        self::assertSame('2', (string) $rows[1]['id']);
    }

    public function testGetQueryCanBeCalledMultipleTimes(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')->from('test_entity');

        $first  = iterator_to_array($qb->getQuery()->toIterable());
        $second = iterator_to_array($qb->getQuery()->toIterable());

        self::assertCount(5, $first);
        self::assertCount(5, $second);
    }
}
