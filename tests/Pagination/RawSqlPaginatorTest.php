<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Pagination;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\EntityManagerInterface;
use InvalidArgumentException;
use Kraz\ReadModelDoctrine\Pagination\RawSqlPaginator;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

#[CoversClass(RawSqlPaginator::class)]
final class RawSqlPaginatorTest extends TestCase
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
    private function makeQuery(int $firstResult = 0, int $maxResults = 10): RawQuery
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity ORDER BY id ASC');
        $query->setFirstResult($firstResult);
        $query->setMaxResults($maxResults);

        return $query;
    }

    public function testConstructorThrowsWhenFirstResultIsNull(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity');
        $query->setMaxResults(10);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/firstResult/');
        new RawSqlPaginator($query);
    }

    public function testConstructorThrowsWhenMaxResultsIsNull(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity');
        $query->setFirstResult(0);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/maxResults/');
        new RawSqlPaginator($query);
    }

    public function testConstructorThrowsWhenMaxResultsIsNegative(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity');
        $query->setFirstResult(0);
        $query->setMaxResults(-1);

        $this->expectException(InvalidArgumentException::class);
        new RawSqlPaginator($query);
    }

    public function testConstructorAcceptsRawQueryBuilder(): void
    {
        $qb = new RawQueryBuilder($this->connection);
        $qb->select('*')->from('test_entity');
        $qb->setFirstResult(0);
        $qb->setMaxResults(10);

        $paginator = new RawSqlPaginator($qb);
        self::assertSame(10, $paginator->getItemsPerPage());
    }

    public function testGetItemsPerPage(): void
    {
        $paginator = new RawSqlPaginator($this->makeQuery(0, 3));
        self::assertSame(3, $paginator->getItemsPerPage());
    }

    public function testGetCurrentPageFirstPage(): void
    {
        $paginator = new RawSqlPaginator($this->makeQuery(0, 3));
        self::assertSame(1, $paginator->getCurrentPage());
    }

    public function testGetCurrentPageSecondPage(): void
    {
        $paginator = new RawSqlPaginator($this->makeQuery(3, 3));
        self::assertSame(2, $paginator->getCurrentPage());
    }

    public function testGetCurrentPageThirdPage(): void
    {
        $paginator = new RawSqlPaginator($this->makeQuery(6, 3));
        self::assertSame(3, $paginator->getCurrentPage());
    }

    public function testGetCurrentPageWhenMaxResultsIsZero(): void
    {
        // When maxResults is 0, getCurrentPage() returns 1 (guard branch).
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity');
        $query->setFirstResult(10);
        $query->setMaxResults(0);

        $paginator = new RawSqlPaginator($query);
        self::assertSame(1, $paginator->getCurrentPage());
    }

    public function testGetLastPageWhenItemsFitExactlyOnOnePage(): void
    {
        // 5 items, 5 per page => 1 page.
        $paginator = new RawSqlPaginator($this->makeQuery(0, 5));
        self::assertSame(1, $paginator->getLastPage());
    }

    public function testGetLastPageWithRemainder(): void
    {
        // 5 items, 3 per page => ceil(5/3) = 2 pages.
        $paginator = new RawSqlPaginator($this->makeQuery(0, 3));
        self::assertSame(2, $paginator->getLastPage());
    }

    public function testGetLastPageWhenNoItemsIsAtLeastOne(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity WHERE 1 = 0');
        $query->setFirstResult(0);
        $query->setMaxResults(10);

        $paginator = new RawSqlPaginator($query);
        // Zero items: ceil(0/10) = 0, but `?: 1` coalesces to 1.
        self::assertSame(1, $paginator->getLastPage());
    }

    public function testGetLastPageWhenMaxResultsIsZero(): void
    {
        $query = new RawQuery($this->connection);
        $query->setSql('SELECT * FROM test_entity');
        $query->setFirstResult(0);
        $query->setMaxResults(0);

        $paginator = new RawSqlPaginator($query);
        // Guard branch: maxResults <= 0 returns 1.
        self::assertSame(1, $paginator->getLastPage());
    }

    public function testGetTotalItemsCountsAllRowsIgnoringPagination(): void
    {
        // Even with a small page size, total should reflect all 5 rows.
        $paginator = new RawSqlPaginator($this->makeQuery(0, 2));
        self::assertSame(5, $paginator->getTotalItems());
    }

    public function testGetTotalItemsOnSecondPageStillCountsAll(): void
    {
        $paginator = new RawSqlPaginator($this->makeQuery(2, 2));
        self::assertSame(5, $paginator->getTotalItems());
    }

    public function testCountReturnsNumberOfItemsOnCurrentPage(): void
    {
        // First page of 2: 2 items.
        $paginator = new RawSqlPaginator($this->makeQuery(0, 2));
        self::assertSame(2, $paginator->count());
    }

    public function testCountReturnsRemainingItemsOnLastPage(): void
    {
        // Offset 4, page size 3: only 1 row remains.
        $paginator = new RawSqlPaginator($this->makeQuery(4, 3));
        self::assertSame(1, $paginator->count());
    }

    public function testGetIteratorYieldsCurrentPageRows(): void
    {
        $paginator = new RawSqlPaginator($this->makeQuery(0, 2));
        $rows      = iterator_to_array($paginator->getIterator());
        self::assertCount(2, $rows);
        self::assertIsArray($rows[0]);
        self::assertIsArray($rows[1]);
        self::assertSame('1', (string) $rows[0]['id']);
        self::assertSame('2', (string) $rows[1]['id']);
    }

    public function testGetIteratorYieldsSecondPage(): void
    {
        $paginator = new RawSqlPaginator($this->makeQuery(2, 2));
        $rows      = iterator_to_array($paginator->getIterator());
        self::assertCount(2, $rows);
        self::assertIsArray($rows[0]);
        self::assertIsArray($rows[1]);
        self::assertSame('3', (string) $rows[0]['id']);
        self::assertSame('4', (string) $rows[1]['id']);
    }
}
