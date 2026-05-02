<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\EntityManagerInterface;
use Kraz\ReadModelDoctrine\Query\AbstractRawQuery;
use Kraz\ReadModelDoctrine\RawQueryReadDataProvider;
use Kraz\ReadModelDoctrine\Tests\Fixtures\UserSQLReadModelFixture;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function intval;
use function is_array;

#[CoversClass(RawQueryReadDataProvider::class)]
final class RawQueryReadDataProviderTest extends TestCase
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

    /**
     * @param iterable<mixed> $items
     *
     * @return list<int>
     */
    private function ids(iterable $items): array
    {
        $ids = [];
        foreach ($items as $item) {
            if (! is_array($item)) {
                continue;
            }

            $ids[] = intval($item['id']);
        }

        return $ids;
    }

    // -------------------------------------------------------------------------
    // Basic data retrieval
    // -------------------------------------------------------------------------

    public function testReturnsAllRows(): void
    {
        $rm = new UserSQLReadModelFixture($this->connection);

        self::assertCount(5, $rm->data());
    }

    public function testCountMatchesRowCount(): void
    {
        $rm = new UserSQLReadModelFixture($this->connection);

        self::assertSame(5, $rm->count());
        self::assertSame(5, $rm->totalCount());
    }

    public function testIsNotEmpty(): void
    {
        $rm = new UserSQLReadModelFixture($this->connection);

        self::assertFalse($rm->isEmpty());
    }

    public function testDefaultOrderById(): void
    {
        $rm = new UserSQLReadModelFixture($this->connection);

        self::assertSame([1, 2, 3, 4, 5], $this->ids($rm->data()));
    }

    // -------------------------------------------------------------------------
    // Built-in parametrized filter
    // -------------------------------------------------------------------------

    public function testOlderThan25FiltersCorrectly(): void
    {
        $rm = (new UserSQLReadModelFixture($this->connection))->olderThan25();

        // age > 25: Alice(30), Charlie(35), Dave(40), Eve(28); Bob(25) excluded
        self::assertSame([1, 3, 4, 5], $this->ids($rm->data()));
    }

    public function testOlderThan25DoesNotMutateOriginal(): void
    {
        $rm = new UserSQLReadModelFixture($this->connection);
        $rm->olderThan25();

        self::assertCount(5, $rm->data());
    }

    public function testOlderThan25CountMatchesFilteredRows(): void
    {
        $rm = (new UserSQLReadModelFixture($this->connection))->olderThan25();

        self::assertSame(4, $rm->count());
        self::assertSame(4, $rm->totalCount());
    }

    // -------------------------------------------------------------------------
    // Pagination
    // -------------------------------------------------------------------------

    public function testNotPaginatedByDefault(): void
    {
        $rm = new UserSQLReadModelFixture($this->connection);

        self::assertFalse($rm->isPaginated());
    }

    public function testSupportsPagination(): void
    {
        $rm = (new UserSQLReadModelFixture($this->connection))->withPagination(2, 2);

        self::assertTrue($rm->isPaginated());
        self::assertSame([3, 4], $this->ids($rm->data()));
        self::assertSame(5, $rm->totalCount());
    }

    public function testCombinesFilterAndPagination(): void
    {
        // olderThan25 → age > 25: ids 1, 3, 4, 5. Page 1, 2 per page → ids 1, 3.
        $rm = (new UserSQLReadModelFixture($this->connection))
            ->olderThan25()
            ->withPagination(1, 2);

        self::assertTrue($rm->isPaginated());
        self::assertSame([1, 3], $this->ids($rm->data()));
        self::assertSame(4, $rm->totalCount());
    }

    // -------------------------------------------------------------------------
    // getRawQuery
    // -------------------------------------------------------------------------

    public function testGetRawQueryReturnsQueryObject(): void
    {
        $rm = new UserSQLReadModelFixture($this->connection);

        self::assertInstanceOf(AbstractRawQuery::class, $rm->getRawQuery());
    }
}
