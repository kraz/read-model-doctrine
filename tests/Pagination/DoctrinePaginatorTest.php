<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Pagination;

use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\Tools\Pagination\Paginator;
use InvalidArgumentException;
use Kraz\ReadModelDoctrine\Pagination\DoctrinePaginator;
use Kraz\ReadModelDoctrine\Tests\Fixtures\TestEntity;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

#[CoversClass(DoctrinePaginator::class)]
final class DoctrinePaginatorTest extends TestCase
{
    private EntityManagerInterface $em;

    protected function setUp(): void
    {
        $this->em = ORMTestKit::createEntityManager();
        $this->seed();
    }

    private function seed(): void
    {
        $connection = $this->em->getConnection();
        $rows       = [
            [1, 'Alice', 'alice@example.com', 'eng', 30, 1],
            [2, 'Bob', 'bob@example.com', 'eng', 25, 1],
            [3, 'Charlie', 'charlie@example.com', 'sales', 35, 0],
            [4, 'Dave', '', 'sales', 40, 1],
            [5, 'Eve', null, 'support', 28, 1],
        ];
        foreach ($rows as [$id, $name, $email, $dept, $age, $active]) {
            $connection->executeStatement(
                'INSERT INTO test_entity (id, name, email, department, age, active) VALUES (?, ?, ?, ?, ?, ?)',
                [$id, $name, $email, $dept, $age, $active],
            );
        }
    }

    /** @return Paginator<TestEntity> */
    private function makePaginator(int $firstResult = 0, int $maxResults = 10): Paginator
    {
        $query = $this->em->createQueryBuilder()
            ->select('u')
            ->from(TestEntity::class, 'u')
            ->orderBy('u.id', 'ASC')
            ->getQuery()
            ->setFirstResult($firstResult)
            ->setMaxResults($maxResults);

        return new Paginator($query);
    }

    public function testConstructorThrowsWhenMaxResultsIsNull(): void
    {
        $query = $this->em->createQueryBuilder()
            ->select('u')
            ->from(TestEntity::class, 'u')
            ->getQuery()
            ->setFirstResult(0);
        // maxResults not set — Doctrine returns null.

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/maxResults/');
        new DoctrinePaginator(new Paginator($query));
    }

    public function testConstructorThrowsWhenMaxResultsIsNegative(): void
    {
        $query = $this->em->createQueryBuilder()
            ->select('u')
            ->from(TestEntity::class, 'u')
            ->getQuery()
            ->setFirstResult(0)
            ->setMaxResults(-1);

        $this->expectException(InvalidArgumentException::class);
        new DoctrinePaginator(new Paginator($query));
    }

    public function testGetItemsPerPage(): void
    {
        $paginator = new DoctrinePaginator($this->makePaginator(0, 3));
        self::assertSame(3, $paginator->getItemsPerPage());
    }

    public function testGetCurrentPageFirstPage(): void
    {
        $paginator = new DoctrinePaginator($this->makePaginator(0, 3));
        self::assertSame(1, $paginator->getCurrentPage());
    }

    public function testGetCurrentPageSecondPage(): void
    {
        $paginator = new DoctrinePaginator($this->makePaginator(3, 3));
        self::assertSame(2, $paginator->getCurrentPage());
    }

    public function testGetCurrentPageThirdPage(): void
    {
        $paginator = new DoctrinePaginator($this->makePaginator(6, 3));
        self::assertSame(3, $paginator->getCurrentPage());
    }

    public function testGetCurrentPageWhenMaxResultsIsZero(): void
    {
        // Guard branch: maxResults <= 0 returns 1.
        $query = $this->em->createQueryBuilder()
            ->select('u')
            ->from(TestEntity::class, 'u')
            ->getQuery()
            ->setFirstResult(10)
            ->setMaxResults(0);

        $paginator = new DoctrinePaginator(new Paginator($query));
        self::assertSame(1, $paginator->getCurrentPage());
    }

    public function testGetLastPageWithExactDivision(): void
    {
        // 5 items, 5 per page => 1 page.
        $paginator = new DoctrinePaginator($this->makePaginator(0, 5));
        self::assertSame(1, $paginator->getLastPage());
    }

    public function testGetLastPageWithRemainder(): void
    {
        // 5 items, 3 per page => ceil(5/3) = 2 pages.
        $paginator = new DoctrinePaginator($this->makePaginator(0, 3));
        self::assertSame(2, $paginator->getLastPage());
    }

    public function testGetLastPageWhenNoItemsIsAtLeastOne(): void
    {
        $query = $this->em->createQueryBuilder()
            ->select('u')
            ->from(TestEntity::class, 'u')
            ->where('u.id = -1')
            ->getQuery()
            ->setFirstResult(0)
            ->setMaxResults(10);

        $paginator = new DoctrinePaginator(new Paginator($query));
        // Zero items: ceil(0/10) = 0, but `?: 1` coalesces to 1.
        self::assertSame(1, $paginator->getLastPage());
    }

    public function testGetLastPageWhenMaxResultsIsZero(): void
    {
        $query = $this->em->createQueryBuilder()
            ->select('u')
            ->from(TestEntity::class, 'u')
            ->getQuery()
            ->setFirstResult(0)
            ->setMaxResults(0);

        $paginator = new DoctrinePaginator(new Paginator($query));
        // Guard branch: maxResults <= 0 returns 1.
        self::assertSame(1, $paginator->getLastPage());
    }

    public function testGetTotalItemsCountsAllRowsIgnoringPagination(): void
    {
        // Even with small page size, total reflects all 5 rows.
        $paginator = new DoctrinePaginator($this->makePaginator(0, 2));
        self::assertSame(5, $paginator->getTotalItems());
    }

    public function testGetTotalItemsOnSecondPageStillCountsAll(): void
    {
        $paginator = new DoctrinePaginator($this->makePaginator(2, 2));
        self::assertSame(5, $paginator->getTotalItems());
    }

    public function testCountReturnsNumberOfItemsOnCurrentPage(): void
    {
        $paginator = new DoctrinePaginator($this->makePaginator(0, 2));
        self::assertSame(2, $paginator->count());
    }

    public function testCountReturnsRemainingItemsOnLastPage(): void
    {
        // Offset 4, page size 3: only 1 row remains.
        $paginator = new DoctrinePaginator($this->makePaginator(4, 3));
        self::assertSame(1, $paginator->count());
    }

    public function testGetIteratorYieldsCurrentPageEntities(): void
    {
        $paginator = new DoctrinePaginator($this->makePaginator(0, 2));
        $rows      = iterator_to_array($paginator->getIterator());
        self::assertCount(2, $rows);
        self::assertInstanceOf(TestEntity::class, $rows[0]);
        self::assertSame(1, $rows[0]->id);
        self::assertSame(2, $rows[1]->id);
    }

    public function testGetIteratorYieldsSecondPage(): void
    {
        $paginator = new DoctrinePaginator($this->makePaginator(2, 2));
        $rows      = iterator_to_array($paginator->getIterator());
        self::assertCount(2, $rows);
        self::assertInstanceOf(TestEntity::class, $rows[0]);
        self::assertSame(3, $rows[0]->id);
        self::assertSame(4, $rows[1]->id);
    }
}
