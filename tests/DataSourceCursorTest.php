<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests;

use Doctrine\ORM\AbstractQuery;
use Doctrine\ORM\EntityManagerInterface;
use Kraz\ReadModel\CursorReadResponse;
use Kraz\ReadModel\Exception\InvalidCursorException;
use Kraz\ReadModel\Pagination\Cursor\Base64JsonCursorCodec;
use Kraz\ReadModel\Pagination\Cursor\Cursor;
use Kraz\ReadModel\Pagination\Cursor\Direction;
use Kraz\ReadModel\Pagination\Cursor\SignedCursorCodec;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\SortExpression;
use Kraz\ReadModelDoctrine\DataSource;
use Kraz\ReadModelDoctrine\Pagination\DoctrineCursorPaginator;
use Kraz\ReadModelDoctrine\Tests\Fixtures\TestEntity;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function array_column;
use function array_map;
use function iterator_to_array;
use function substr;

#[CoversClass(DataSource::class)]
#[CoversClass(DoctrineCursorPaginator::class)]
final class DataSourceCursorTest extends TestCase
{
    private EntityManagerInterface $em;

    protected function setUp(): void
    {
        $this->em   = ORMTestKit::createEntityManager();
        $connection = $this->em->getConnection();
        // Seven rows so several page-boundary scenarios are visible.
        $rows = [
            [1, 'Anna', 'a@example.com', 'eng', 20, 1],
            [2, 'Bob', 'b@example.com', 'eng', 25, 1],
            [3, 'Carol', 'c@example.com', 'sales', 30, 1],
            [4, 'Dan', 'd@example.com', 'sales', 35, 1],
            [5, 'Eve', 'e@example.com', 'support', 40, 1],
            [6, 'Frank', 'f@example.com', 'support', 45, 1],
            [7, 'Gina', 'g@example.com', 'support', 50, 1],
        ];
        foreach ($rows as [$id, $name, $email, $dept, $age, $active]) {
            $connection->executeStatement(
                'INSERT INTO test_entity (id, name, email, department, age, active) VALUES (?, ?, ?, ?, ?, ?)',
                [$id, $name, $email, $dept, $age, $active],
            );
        }
    }

    /** @return DataSource<TestEntity> */
    private function dataSource(): DataSource
    {
        $qb = $this->em->createQueryBuilder()
            ->select('r')
            ->from(TestEntity::class, 'r');

        /** @var DataSource<TestEntity> $ds */
        $ds = new DataSource($qb, options: [
            'hydrator' => AbstractQuery::HYDRATE_OBJECT,
            'root_alias' => 'r',
            'root_identifier' => 'id',
        ]);

        return $ds;
    }

    /**
     * @phpstan-param iterable<TestEntity> $rows
     *
     * @phpstan-return list<int>
     */
    private function ids(iterable $rows): array
    {
        $ids = [];
        foreach ($rows as $row) {
            $ids[] = $row->id;
        }

        return $ids;
    }

    public function testFirstPageFetchesViaKeysetSql(): void
    {
        $ds = $this->dataSource()->withCursor(null, 3);

        self::assertTrue($ds->isCursored());
        self::assertFalse($ds->isPaginated());

        $paginator = $ds->cursorPaginator();
        self::assertNotNull($paginator);
        self::assertSame([1, 2, 3], $this->ids($paginator->getIterator()));
        self::assertTrue($paginator->hasNext());
        self::assertFalse($paginator->hasPrevious());
        self::assertNotNull($paginator->getNextCursor());
        self::assertNull($paginator->getPreviousCursor());
    }

    public function testWalkForwardEnumeratesAllRowsInOrder(): void
    {
        $ds    = $this->dataSource();
        $token = null;
        $pages = [];

        for ($i = 0; $i < 10; $i++) {
            $page      = $ds->withCursor($token, 3);
            $paginator = $page->cursorPaginator();
            self::assertNotNull($paginator);

            $pages[] = $this->ids($paginator->getIterator());
            $token   = $paginator->getNextCursor();
            if ($token === null) {
                break;
            }
        }

        self::assertSame([[1, 2, 3], [4, 5, 6], [7]], $pages);
    }

    public function testBackwardNavigationReconstructsPreviousWindow(): void
    {
        $ds    = $this->dataSource();
        $first = $ds->withCursor(null, 3)->cursorPaginator();
        self::assertNotNull($first);
        $nextToken = $first->getNextCursor();
        self::assertNotNull($nextToken);

        $second = $ds->withCursor($nextToken, 3)->cursorPaginator();
        self::assertNotNull($second);
        self::assertSame([4, 5, 6], $this->ids($second->getIterator()));

        $prevToken = $second->getPreviousCursor();
        self::assertNotNull($prevToken);

        $back = $ds->withCursor($prevToken, 3)->cursorPaginator();
        self::assertNotNull($back);
        // Caller sees natural order regardless of direction.
        self::assertSame([1, 2, 3], $this->ids($back->getIterator()));
        self::assertTrue($back->hasNext());
        self::assertFalse($back->hasPrevious());
    }

    public function testCursorRespectsCustomSortAndTieBreaker(): void
    {
        $ds = $this->dataSource()
            ->withQueryExpression(QueryExpression::create()->sortBy('department', SortExpression::DIR_ASC))
            ->withCursor(null, 4);

        $paginator = $ds->cursorPaginator();
        self::assertNotNull($paginator);

        $rows = iterator_to_array($paginator->getIterator(), false);
        // Sorted by department then id (the tiebreaker the adapter injects).
        self::assertSame(['eng', 'eng', 'sales', 'sales'], array_column(
            array_map(static fn (TestEntity $r): array => ['department' => $r->department], $rows),
            'department',
        ));
        self::assertSame([1, 2, 3, 4], $this->ids($rows));
    }

    public function testCursorClearsAndRestoresOnSwitchingModes(): void
    {
        $ds = $this->dataSource();

        $cursored = $ds->withCursor(null, 2);
        self::assertTrue($cursored->isCursored());
        self::assertNull($cursored->paginator());

        $paged = $cursored->withPagination(1, 2);
        self::assertFalse($paged->isCursored());
        self::assertTrue($paged->isPaginated());
        self::assertNotNull($paged->paginator());
        self::assertNull($paged->cursorPaginator());
    }

    public function testGetResultReturnsCursorReadResponse(): void
    {
        $result = $this->dataSource()->withCursor(null, 2)->getResult();

        self::assertInstanceOf(CursorReadResponse::class, $result);
        self::assertSame([1, 2], $this->ids($result->data ?? []));
        self::assertNotNull($result->nextCursor);
        self::assertTrue($result->hasNext);
        self::assertFalse($result->hasPrevious);
        // totalItems is intentionally null in cursor mode by default (keyset-friendly).
        self::assertNull($result->totalItems);
    }

    public function testCursorWithMismatchedSortSignatureIsRejected(): void
    {
        $codec = new Base64JsonCursorCodec();
        // Forge a cursor whose sort signature does not match the current effective sort.
        $foreignSort = SortExpression::create()->desc('email')->asc('id');
        $bad         = $codec->encode(new Cursor(
            Direction::FORWARD,
            [['field' => 'email', 'value' => 'zzz'], ['field' => 'id', 'value' => 0]],
            Cursor::signatureFor($foreignSort),
        ));

        $ds = $this->dataSource()
            ->withQueryExpression(QueryExpression::create()->sortBy('age', SortExpression::DIR_ASC))
            ->withCursor($bad, 3);

        $this->expectException(InvalidCursorException::class);
        $ds->cursorPaginator();
    }

    public function testSignedCodecCanRoundTripThroughDataSource(): void
    {
        $codec = new SignedCursorCodec(new Base64JsonCursorCodec(), 'integration-secret');
        $ds    = $this->dataSource()->withCursorCodec($codec);

        $first = $ds->withCursor(null, 3)->cursorPaginator();
        self::assertNotNull($first);
        $nextToken = $first->getNextCursor();
        self::assertNotNull($nextToken);

        $second = $ds->withCursor($nextToken, 3)->cursorPaginator();
        self::assertNotNull($second);
        self::assertSame([4, 5, 6], $this->ids($second->getIterator()));
    }

    public function testTamperedSignedCursorIsRejected(): void
    {
        $codec = new SignedCursorCodec(new Base64JsonCursorCodec(), 'integration-secret');
        $ds    = $this->dataSource()->withCursorCodec($codec);

        $first = $ds->withCursor(null, 3)->cursorPaginator();
        self::assertNotNull($first);
        $token = $first->getNextCursor();
        self::assertNotNull($token);

        // Flip one byte in the payload — signature must catch it.
        $tampered = ($token[0] === 'a' ? 'b' : 'a') . substr($token, 1);

        $this->expectException(InvalidCursorException::class);
        $ds->withCursor($tampered, 3)->cursorPaginator();
    }
}
