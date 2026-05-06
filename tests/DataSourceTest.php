<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\Query as ORMQuery;
use InvalidArgumentException;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\Query\QueryRequest;
use Kraz\ReadModel\ReadDataProviderAccess;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModel\ReadResponse;
use Kraz\ReadModelDoctrine\DataSource;
use Kraz\ReadModelDoctrine\Pagination\DoctrinePaginator;
use Kraz\ReadModelDoctrine\Pagination\RawSqlPaginator;
use Kraz\ReadModelDoctrine\Query\AbstractRawQuery;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Tests\Fixtures\Specifications\AgeAboveSpecification;
use Kraz\ReadModelDoctrine\Tests\Fixtures\Specifications\NameEqualsSpecification;
use Kraz\ReadModelDoctrine\Tests\Fixtures\TestEntity;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use Kraz\ReadModelDoctrine\Tools\ParametersCollection;
use Kraz\ReadModelDoctrine\Tools\QueryParts;
use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;

use function intval;
use function is_array;
use function iterator_to_array;

#[CoversClass(DataSource::class)]
#[CoversClass(ReadDataProviderAccess::class)]
final class DataSourceTest extends TestCase
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

    /** @phpstan-return DataSource<array<string, mixed>> */
    private function makeOrmDs(): DataSource
    {
        $qb = $this->em->createQueryBuilder()
            ->select('u')
            ->from(TestEntity::class, 'u')
            ->orderBy('u.id', 'ASC');

        /** @phpstan-var DataSource<array<string, mixed>> $ds */
        $ds = new DataSource($qb);

        return $ds;
    }

    /** @phpstan-return DataSource<array<string, mixed>> */
    private function makeRawDs(string $sql = 'SELECT * FROM test_entity ORDER BY id ASC'): DataSource
    {
        /** @phpstan-var DataSource<array<string, mixed>> $ds */
        $ds = new DataSource($sql, null, ['connection' => $this->connection]);

        return $ds;
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
            if ($item instanceof TestEntity) {
                $ids[] = $item->id;
            } elseif (is_array($item)) {
                $ids[] = intval($item['id']);
            }
        }

        return $ids;
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public function testStringSqlWithoutConnectionOptionThrows(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/connection/');
        new DataSource('SELECT 1');
    }

    public function testStringSqlWithConnectionCreatesRawDataSource(): void
    {
        $ds = $this->makeRawDs();

        self::assertInstanceOf(AbstractRawQuery::class, $ds->getQuery());
    }

    public function testQueryBuilderIsAccepted(): void
    {
        $ds = $this->makeOrmDs();

        self::assertInstanceOf(ORMQuery::class, $ds->getQuery());
    }

    public function testAbstractRawQueryIsClonedOnConstruction(): void
    {
        $raw = new RawQuery($this->connection);
        $raw->setSql('SELECT * FROM test_entity ORDER BY id ASC');

        /** @phpstan-var DataSource<array<string, mixed>> $ds */
        $ds = new DataSource($raw);

        self::assertNotSame($raw, $ds->getRawQuery());
    }

    // -------------------------------------------------------------------------
    // getQuery / getRawQuery
    // -------------------------------------------------------------------------

    public function testGetQueryIsCachedPerInstance(): void
    {
        $ds = $this->makeOrmDs();

        self::assertSame($ds->getQuery(), $ds->getQuery());
    }

    public function testGetQueryResetsOnClone(): void
    {
        $ds = $this->makeOrmDs();

        $query = $ds->getQuery();
        $clone = $ds->withPagination(1, 3);

        self::assertNotSame($query, $clone->getQuery());
    }

    public function testGetRawQueryThrowsForOrmDataSource(): void
    {
        $ds = $this->makeOrmDs();

        $this->expectException(InvalidArgumentException::class);
        $ds->getRawQuery();
    }

    public function testGetRawQueryReturnsForRawSqlDataSource(): void
    {
        $ds = $this->makeRawDs();

        self::assertInstanceOf(AbstractRawQuery::class, $ds->getRawQuery());
    }

    // -------------------------------------------------------------------------
    // Query expressions (history tracking)
    // -------------------------------------------------------------------------

    public function testQueryExpressionsInitiallyEmpty(): void
    {
        self::assertSame([], $this->makeOrmDs()->queryExpressions());
    }

    public function testWithQueryExpressionAddsToStack(): void
    {
        $ds = $this->makeOrmDs();
        $qe = QueryExpression::create();
        $ds = $ds->withQueryExpression($qe);

        self::assertSame([$qe], $ds->queryExpressions());
    }

    public function testMultipleQueryExpressionsStack(): void
    {
        $ds  = $this->makeOrmDs();
        $qe1 = QueryExpression::create();
        $qe2 = QueryExpression::create();
        $ds  = $ds->withQueryExpression($qe1)->withQueryExpression($qe2, true);

        self::assertSame([$qe1, $qe2], $ds->queryExpressions());
    }

    public function testWithoutQueryExpressionUndoRestoresPreviousStack(): void
    {
        $ds  = $this->makeOrmDs();
        $qe1 = QueryExpression::create();
        $qe2 = QueryExpression::create();

        $stacked = $ds->withQueryExpression($qe1)->withQueryExpression($qe2, true);
        $back    = $stacked->withoutQueryExpression(true);

        self::assertSame([$qe1], $back->queryExpressions());
    }

    public function testWithoutQueryExpressionUndoOnEmptyIsNoOp(): void
    {
        $ds = $this->makeOrmDs();

        self::assertSame([], $ds->withoutQueryExpression(true)->queryExpressions());
    }

    public function testWithoutQueryExpressionClearsAllAndHistory(): void
    {
        $ds  = $this->makeOrmDs();
        $qe1 = QueryExpression::create();
        $qe2 = QueryExpression::create();

        $stacked = $ds->withQueryExpression($qe1)->withQueryExpression($qe2, true);
        $cleared = $stacked->withoutQueryExpression();

        self::assertSame([], $cleared->queryExpressions());
    }

    public function testWithoutQueryExpressionClearAlsoClearsHistorySoUndoIsNoOp(): void
    {
        $ds = $this->makeOrmDs();
        $qe = QueryExpression::create();

        $cleared   = $ds->withQueryExpression($qe)->withoutQueryExpression();
        $afterUndo = $cleared->withoutQueryExpression(true);

        self::assertSame([], $afterUndo->queryExpressions());
    }

    public function testWithQueryExpressionDoesNotMutateOriginal(): void
    {
        $ds = $this->makeOrmDs();
        $qe = QueryExpression::create();

        $ds->withQueryExpression($qe);

        self::assertSame([], $ds->queryExpressions());
    }

    public function testQueryExpressionFiltersOrmResults(): void
    {
        $ds = $this->makeOrmDs();
        $qe = QueryExpression::create();
        $qe = $qe->andWhere($qe->expr()->equalTo('name', 'Alice'));
        $ds = $ds->withQueryExpression($qe);

        self::assertSame([1], $this->ids($ds->data()));
    }

    public function testQueryExpressionFiltersRawSqlResults(): void
    {
        // The /*#WHERE#*/ placeholder tells the SqlFormatter where to inject the WHERE clause.
        // The table alias 'r' matches the default root_alias so field names like r.department are valid.
        $ds = new DataSource(
            'SELECT * FROM test_entity r /*#WHERE#*/ ORDER BY r.id ASC',
            null,
            ['connection' => $this->connection],
        );
        $qe = QueryExpression::create();
        $qe = $qe->andWhere($qe->expr()->equalTo('department', 'eng'));
        $ds = $ds->withQueryExpression($qe);

        self::assertSame([1, 2], $this->ids($ds->data()));
    }

    public function testIndependentClonesDoNotShareExpressionHistory(): void
    {
        $ds  = $this->makeOrmDs();
        $qe1 = QueryExpression::create()->andWhere(QueryExpression::create()->expr()->equalTo('name', 'Alice'));
        $qe2 = QueryExpression::create()->andWhere(QueryExpression::create()->expr()->equalTo('name', 'Bob'));

        $branchA = $ds->withQueryExpression($qe1);
        $branchB = $branchA->withQueryExpression($qe2, true);

        // Undoing on branchA should not affect branchB
        $branchA = $branchA->withoutQueryExpression(true);

        self::assertSame([], $branchA->queryExpressions());
        self::assertSame([$qe1, $qe2], $branchB->queryExpressions());
    }

    // -------------------------------------------------------------------------
    // Query modifiers (history tracking)
    // -------------------------------------------------------------------------

    public function testWithQueryModifierDoesNotMutateOriginal(): void
    {
        $ds       = $this->makeRawDs();
        $modifier = static function (QueryParts $qp, ParametersCollection $params): void {
        };

        $ds->withQueryModifier($modifier);

        self::assertSame([], $ds->data() === [] ? [] : []);
        self::assertCount(5, $ds->data());
    }

    public function testWithQueryModifierFiltersRawSqlResults(): void
    {
        $ds = $this->makeRawDs('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC');
        $ds = $ds->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
            $qp->andWhere('department = :dept');
            $params->setParameter('dept', 'eng');
        });

        self::assertSame([1, 2], $this->ids($ds->data()));
    }

    public function testWithoutQueryModifierUndoRestoresPreviousStack(): void
    {
        $ds = $this->makeRawDs('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC');

        $mod1 = static function (QueryParts $qp, ParametersCollection $params): void {
            $qp->andWhere('department = :dept');
            $params->setParameter('dept', 'eng');
        };
        $mod2 = static function (QueryParts $qp, ParametersCollection $params): void {
            $qp->andWhere('age > :age');
            $params->setParameter('age', 26);
        };

        $stacked = $ds->withQueryModifier($mod1)->withQueryModifier($mod2, true);
        $back    = $stacked->withoutQueryModifier(true);

        // After undo, only mod1 is active: department = eng (ids 1, 2)
        self::assertSame([1, 2], $this->ids($back->data()));
    }

    public function testWithoutQueryModifierUndoOnEmptyIsNoOp(): void
    {
        $ds = $this->makeRawDs();

        self::assertCount(5, $ds->withoutQueryModifier(true)->data());
    }

    public function testWithoutQueryModifierClearsAllAndHistory(): void
    {
        $ds = $this->makeRawDs('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC');
        $ds = $ds
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('department = :dept');
                $params->setParameter('dept', 'eng');
            })
            ->withoutQueryModifier();

        self::assertCount(5, $ds->data());
    }

    public function testWithoutQueryModifierClearAlsoClearsHistorySoUndoIsNoOp(): void
    {
        $ds = $this->makeRawDs('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC');
        $ds = $ds
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('department = :dept');
                $params->setParameter('dept', 'eng');
            })
            ->withoutQueryModifier()
            ->withoutQueryModifier(true);

        self::assertCount(5, $ds->data());
    }

    // -------------------------------------------------------------------------
    // Pagination
    // -------------------------------------------------------------------------

    public function testWithPaginationEnablesPagination(): void
    {
        $ds = $this->makeOrmDs()->withPagination(1, 2);

        self::assertTrue($ds->isPaginated());
    }

    public function testWithPaginationRejectsNonPositivePage(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->makeOrmDs()->withPagination(0, 5);
    }

    public function testWithPaginationRejectsNonPositiveItemsPerPage(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->makeOrmDs()->withPagination(1, 0);
    }

    public function testWithoutPaginationClearsPagination(): void
    {
        $ds = $this->makeOrmDs()->withPagination(1, 2)->withoutPagination();

        self::assertFalse($ds->isPaginated());
        self::assertNull($ds->paginator());
    }

    public function testPaginatorReturnsNullWhenNotPaginated(): void
    {
        self::assertNull($this->makeOrmDs()->paginator());
    }

    public function testPaginatorReturnsDoctrinePaginatorForOrm(): void
    {
        $ds = $this->makeOrmDs()->withPagination(1, 3);

        self::assertInstanceOf(DoctrinePaginator::class, $ds->paginator());
    }

    public function testPaginatorReturnsRawSqlPaginatorForRawSql(): void
    {
        $ds = $this->makeRawDs()->withPagination(1, 3);

        self::assertInstanceOf(RawSqlPaginator::class, $ds->paginator());
    }

    public function testPaginatorIsCachedPerInstance(): void
    {
        $ds = $this->makeOrmDs()->withPagination(1, 3);

        self::assertSame($ds->paginator(), $ds->paginator());
    }

    public function testPaginationDoesNotMutateOriginal(): void
    {
        $ds = $this->makeOrmDs();
        $ds->withPagination(1, 2);

        self::assertFalse($ds->isPaginated());
    }

    // -------------------------------------------------------------------------
    // Data retrieval
    // -------------------------------------------------------------------------

    public function testDataReturnsAllItems(): void
    {
        self::assertCount(5, $this->makeOrmDs()->data());
    }

    public function testCountWhenNotPaginatedReturnsTotalCount(): void
    {
        self::assertSame(5, $this->makeOrmDs()->count());
    }

    public function testCountWhenPaginatedReturnsItemsOnPage(): void
    {
        $ds = $this->makeOrmDs()->withPagination(2, 2);

        self::assertSame(2, $ds->count());
        self::assertSame(5, $ds->totalCount());
    }

    public function testTotalCountIgnoresPaginationForRawSql(): void
    {
        $ds = $this->makeRawDs()->withPagination(1, 2);

        self::assertSame(5, $ds->totalCount());
    }

    public function testIsEmptyReturnsFalseWhenDataExists(): void
    {
        self::assertFalse($this->makeOrmDs()->isEmpty());
    }

    public function testIsEmptyReturnsTrueForEmptyResultSet(): void
    {
        $ds = new DataSource(
            'SELECT * FROM test_entity WHERE 1=0',
            null,
            ['connection' => $this->connection],
        );

        self::assertTrue($ds->isEmpty());
    }

    public function testGetIteratorYieldsAllItems(): void
    {
        $items = iterator_to_array($this->makeOrmDs()->getIterator());

        self::assertCount(5, $items);
    }

    public function testGetIteratorWithPaginationYieldsPageItems(): void
    {
        $ds    = $this->makeOrmDs()->withPagination(2, 2);
        $items = iterator_to_array($ds->getIterator());

        self::assertCount(2, $items);
        self::assertSame([3, 4], $this->ids($items));
    }

    // -------------------------------------------------------------------------
    // Item normalizer
    // -------------------------------------------------------------------------

    public function testItemNormalizerIsApplied(): void
    {
        /** @phpstan-var DataSource<array<string, mixed>> $ds */
        $ds = new DataSource(
            'SELECT * FROM test_entity ORDER BY id ASC',
            null,
            [
                'connection'      => $this->connection,
                'item_normalizer' => static fn (array $row): int => intval($row['id']),
            ],
        );

        self::assertSame([1, 2, 3, 4, 5], $ds->data());
    }

    public function testNormalizerOptionIsAliasForItemNormalizer(): void
    {
        /** @phpstan-var DataSource<array<string, mixed>> $ds */
        $ds = new DataSource(
            'SELECT * FROM test_entity ORDER BY id ASC',
            null,
            [
                'connection' => $this->connection,
                'normalizer' => static fn (array $row): int => intval($row['id']),
            ],
        );

        self::assertSame([1, 2, 3, 4, 5], $ds->data());
    }

    public function testDenormalizerOptionOverridesNormalizerOption(): void
    {
        $normalizerCalled   = false;
        $denormalizerCalled = false;

        /** @phpstan-var DataSource<array<string, mixed>> $ds */
        $ds = new DataSource(
            'SELECT * FROM test_entity ORDER BY id ASC',
            null,
            [
                'connection'  => $this->connection,
                'normalizer'  => static function (array $row) use (&$normalizerCalled): array {
                    $normalizerCalled = true;

                    return $row;
                },
                'denormalizer' => static function (array $row) use (&$denormalizerCalled): int {
                    $denormalizerCalled = true;

                    return intval($row['id']);
                },
            ],
        );

        self::assertSame([1, 2, 3, 4, 5], $ds->data());
        self::assertFalse($normalizerCalled, 'denormalizer must override normalizer');
        self::assertTrue($denormalizerCalled);
    }

    // -------------------------------------------------------------------------
    // getResult
    // -------------------------------------------------------------------------

    public function testGetResultReturnsReadResponseWhenNotValue(): void
    {
        $result = $this->makeOrmDs()->getResult();

        self::assertInstanceOf(ReadResponse::class, $result);
        self::assertSame(1, $result->page);
        self::assertSame(5, $result->total);
    }

    public function testGetResultReturnsCorrectPageInReadResponse(): void
    {
        $result = $this->makeOrmDs()->withPagination(2, 2)->getResult();

        self::assertInstanceOf(ReadResponse::class, $result);
        self::assertSame(2, $result->page);
        self::assertSame(5, $result->total);
        self::assertNotNull($result->data);
        self::assertCount(2, $result->data);
    }

    public function testGetResultReturnsArrayWhenQueryHasValues(): void
    {
        $ds = $this->makeOrmDs();
        $qe = QueryExpression::create()->withValues([1, 3]);
        $ds = $ds->withQueryExpression($qe);

        $result = $ds->getResult();

        self::assertIsArray($result);
    }

    // -------------------------------------------------------------------------
    // withQueryRequest
    // -------------------------------------------------------------------------

    public function testWithQueryRequestAppliesQueryExpression(): void
    {
        $qe      = QueryExpression::create()->andWhere(QueryExpression::create()->expr()->equalTo('name', 'Bob'));
        $request = QueryRequest::create()->withQueryExpression($qe);

        $ds = $this->makeOrmDs()->withQueryRequest($request);

        self::assertSame([2], $this->ids($ds->data()));
    }

    public function testWithQueryRequestAppliesPagination(): void
    {
        $request = QueryRequest::create()->withPagination(2, 2);

        $ds = $this->makeOrmDs()->withQueryRequest($request);

        self::assertTrue($ds->isPaginated());
        self::assertSame([3, 4], $this->ids($ds->data()));
    }

    public function testWithQueryRequestIgnoresNullQueryAndPagination(): void
    {
        $request = QueryRequest::create();

        $ds = $this->makeOrmDs()->withQueryRequest($request);

        self::assertFalse($ds->isPaginated());
        self::assertSame([], $ds->queryExpressions());
    }

    // -------------------------------------------------------------------------
    // handleRequest
    // -------------------------------------------------------------------------

    public function testHandleRequestThrowsForUnsupportedType(): void
    {
        $this->expectException(RuntimeException::class);
        $this->makeOrmDs()->handleRequest(new stdClass());
    }

    // -------------------------------------------------------------------------
    // Immutability
    // -------------------------------------------------------------------------

    public function testWithMethodsAlwaysReturnNewInstances(): void
    {
        $ds       = $this->makeOrmDs();
        $qe       = QueryExpression::create();
        $factory  = $this->createStub(ReadModelDescriptorFactoryInterface::class);
        $provider = $this->createStub(QueryExpressionProviderInterface::class);

        self::assertNotSame($ds, $ds->withPagination(1, 5));
        self::assertNotSame($ds, $ds->withoutPagination());
        self::assertNotSame($ds, $ds->withQueryExpression($qe));
        self::assertNotSame($ds, $ds->withoutQueryExpression());
        self::assertNotSame($ds, $ds->withoutQueryExpression(true));
        self::assertNotSame($ds, $ds->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
        }));
        self::assertNotSame($ds, $ds->withoutQueryModifier());
        self::assertNotSame($ds, $ds->withoutQueryModifier(true));
        self::assertNotSame($ds, $ds->withSpecification(new AgeAboveSpecification(25)));
        self::assertNotSame($ds, $ds->withoutSpecification());
        self::assertNotSame($ds, $ds->withoutSpecification(true));
        self::assertNotSame($ds, $ds->withQueryExpressionProvider($provider));
        self::assertNotSame($ds, $ds->withoutQueryExpressionProvider());
        self::assertNotSame($ds, $ds->withoutQueryExpressionProvider(true));
        self::assertNotSame($ds, $ds->withDescriptorFactory($factory));
        self::assertNotSame($ds, $ds->withoutDescriptorFactory());
        self::assertNotSame($ds, $ds->withoutDescriptorFactory(true));
        self::assertNotSame($ds, $ds->withItemNormalizer(static fn (mixed $item): mixed => $item));
        self::assertNotSame($ds, $ds->withoutItemNormalizer());
        self::assertNotSame($ds, $ds->withoutItemNormalizer(true));
    }

    public function testWithPaginationDoesNotMutateOriginalPaginationState(): void
    {
        $ds    = $this->makeOrmDs();
        $paged = $ds->withPagination(1, 2);

        self::assertFalse($ds->isPaginated());
        self::assertTrue($paged->isPaginated());
    }

    public function testCloneResetsQueryAndPaginatorCache(): void
    {
        $ds = $this->makeOrmDs()->withPagination(1, 3);

        $paginator = $ds->paginator();
        $query     = $ds->getQuery();

        $clone = $ds->withPagination(2, 3);

        self::assertNotSame($query, $clone->getQuery());
        self::assertNotSame($paginator, $clone->paginator());
    }

    // -------------------------------------------------------------------------
    // Specifications
    // -------------------------------------------------------------------------

    public function testWithSpecificationFiltersOrmResultsViaIsSatisfiedBy(): void
    {
        // NameEqualsSpecification has no QueryExpression → pure PHP-level filtering.
        $ds = $this->makeOrmDs()->withSpecification(new NameEqualsSpecification('Alice'));

        self::assertSame([1], $this->ids($ds->data()));
    }

    public function testWithSpecificationFiltersRawSqlResultsViaIsSatisfiedBy(): void
    {
        $ds = $this->makeRawDs()->withSpecification(new NameEqualsSpecification('Bob'));

        self::assertSame([2], $this->ids($ds->data()));
    }

    public function testWithSpecificationWithQueryExpressionFiltersAtDbLevel(): void
    {
        // AgeAboveSpecification provides a QueryExpression that filters at DB level,
        // and also validates each item via isSatisfiedBy().
        // age > 28: Alice(30), Charlie(35), Dave(40), Eve(28) → ids 1, 3, 4
        $ds = $this->makeOrmDs()->withSpecification(new AgeAboveSpecification(28));

        self::assertSame([1, 3, 4], $this->ids($ds->data()));
    }

    public function testWithSpecificationWithQueryExpressionFiltersRawSql(): void
    {
        $ds = $this->makeRawDs(
            'SELECT * FROM test_entity r /*#WHERE#*/ ORDER BY r.id ASC',
        )->withSpecification(new AgeAboveSpecification(28));

        self::assertSame([1, 3, 4], $this->ids($ds->data()));
    }

    public function testMultipleSpecificationsAreCombinedWithAnd(): void
    {
        $ds = $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withSpecification(new NameEqualsSpecification('Alice'), true);

        // age > 28 AND name = Alice → only Alice (id 1)
        self::assertSame([1], $this->ids($ds->data()));
    }

    public function testWithSpecificationDoesNotMutateOriginal(): void
    {
        $ds = $this->makeOrmDs();
        $ds->withSpecification(new AgeAboveSpecification(28));

        self::assertCount(5, $ds->data());
    }

    public function testWithSpecificationCombinedWithQueryExpression(): void
    {
        // QE filters by department=eng (ids 1, 2), spec filters name=Alice → id 1
        $ds = $this->makeOrmDs();
        $qe = QueryExpression::create()->andWhere($ds->expr()->equalTo('department', 'eng'));
        $ds = $ds->withQueryExpression($qe)->withSpecification(new NameEqualsSpecification('Alice'));

        self::assertSame([1], $this->ids($ds->data()));
    }

    public function testInvertedSpecificationFiltersOppositeItems(): void
    {
        // Not Alice → ids 2, 3, 4, 5
        $inverted = (new NameEqualsSpecification('Alice'))->invert();
        $ds       = $this->makeOrmDs()->withSpecification($inverted);

        self::assertSame([2, 3, 4, 5], $this->ids($ds->data()));
    }

    public function testWithSpecificationAfterPaginationThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeOrmDs()
            ->withPagination(1, 2)
            ->withSpecification(new AgeAboveSpecification(28));
    }

    public function testWithPaginationAfterSpecificationThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withPagination(1, 2);
    }

    public function testWithoutSpecificationUndoRestoresPreviousStack(): void
    {
        $ds = $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withSpecification(new NameEqualsSpecification('Alice'), true);

        $undone = $ds->withoutSpecification(true);

        // After undo only AgeAboveSpecification remains: ids 1, 3, 4
        self::assertSame([1, 3, 4], $this->ids($undone->data()));
    }

    public function testWithoutSpecificationUndoOnEmptyIsNoOp(): void
    {
        $ds = $this->makeOrmDs()->withoutSpecification(true);

        self::assertCount(5, $ds->data());
    }

    public function testWithoutSpecificationClearsAllAndHistory(): void
    {
        $ds = $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withSpecification(new NameEqualsSpecification('Alice'), true)
            ->withoutSpecification();

        self::assertCount(5, $ds->data());
    }

    public function testWithoutSpecificationClearAlsoClearsHistorySoUndoIsNoOp(): void
    {
        $ds = $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withoutSpecification()
            ->withoutSpecification(true);

        self::assertCount(5, $ds->data());
    }

    // -------------------------------------------------------------------------
    // count / totalCount accuracy with specifications
    // -------------------------------------------------------------------------

    public function testTotalCountWithoutSpecsStillUsesDbLevelCount(): void
    {
        self::assertSame(5, $this->makeOrmDs()->totalCount());
        self::assertSame(5, $this->makeRawDs()->totalCount());
    }

    public function testTotalCountWithSpecificationsThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeOrmDs()->withSpecification(new NameEqualsSpecification('Alice'))->totalCount();
    }

    public function testCountWithSpecificationsThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeOrmDs()->withSpecification(new NameEqualsSpecification('Alice'))->count();
    }

    public function testIsEmptyWithSpecificationsThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeOrmDs()->withSpecification(new NameEqualsSpecification('Alice'))->isEmpty();
    }

    public function testGetResultWithSpecificationsThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeOrmDs()->withSpecification(new NameEqualsSpecification('Alice'))->getResult();
    }

    public function testPaginatorWithSpecificationsThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeOrmDs()->withSpecification(new NameEqualsSpecification('Alice'))->paginator();
    }

    public function testCountFirstPageReturnsCorrectCount(): void
    {
        $ds = $this->makeOrmDs()->withPagination(1, 3);

        self::assertSame(3, $ds->count());
    }

    public function testCountSecondPageReturnsRemainder(): void
    {
        $ds = $this->makeOrmDs()->withPagination(2, 3);

        self::assertSame(2, $ds->count());
    }

    public function testTotalCountWithMultipleSpecsThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withSpecification(new NameEqualsSpecification('Alice'), true)
            ->totalCount();
    }

    // -------------------------------------------------------------------------
    // withLimit / withoutLimit — DB-level enforcement
    // -------------------------------------------------------------------------

    public function testWithLimitRestrictsOrmResultCount(): void
    {
        // All 5 rows seeded; limit 3 → first 3 by id.
        $ds = $this->makeOrmDs()->withLimit(3);

        self::assertSame([1, 2, 3], $this->ids($ds->data()));
    }

    public function testWithLimitAndOffsetSkipsOrmRows(): void
    {
        $ds = $this->makeOrmDs()->withLimit(2, 2);

        self::assertSame([3, 4], $this->ids($ds->data()));
    }

    public function testWithLimitRestrictsRawSqlResultCount(): void
    {
        $ds = $this->makeRawDs()->withLimit(2);

        self::assertSame([1, 2], $this->ids($ds->data()));
    }

    public function testWithLimitAndOffsetOnRawSql(): void
    {
        $ds = $this->makeRawDs()->withLimit(2, 3);

        self::assertSame([4, 5], $this->ids($ds->data()));
    }

    public function testWithLimitClearsPagination(): void
    {
        $limited = $this->makeOrmDs()->withPagination(1, 2)->withLimit(3);

        self::assertFalse($limited->isPaginated());
        self::assertSame([1, 2, 3], $this->ids($limited->data()));
    }

    public function testWithPaginationClearsLimit(): void
    {
        $paged = $this->makeOrmDs()->withLimit(2)->withPagination(2, 2);

        self::assertTrue($paged->isPaginated());
        self::assertSame([3, 4], $this->ids($paged->data()));
    }

    public function testWithoutLimitClearsLimit(): void
    {
        $ds = $this->makeOrmDs()->withLimit(2)->withoutLimit();

        self::assertSame([1, 2, 3, 4, 5], $this->ids($ds->data()));
    }

    public function testWithoutLimitUndoRestoresPreviousLimit(): void
    {
        $ds = $this->makeOrmDs()->withLimit(3)->withLimit(1)->withoutLimit(true);

        self::assertSame([1, 2, 3], $this->ids($ds->data()));
    }

    public function testWithLimitRejectsNonPositiveValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->makeOrmDs()->withLimit(0);
    }

    public function testWithLimitDoesNotMutateOriginal(): void
    {
        $original = $this->makeOrmDs();
        $original->withLimit(2);

        self::assertSame([1, 2, 3, 4, 5], $this->ids($original->data()));
    }

    // -------------------------------------------------------------------------
    // specificationsIterator — eager batch-fetching via DB limit/offset
    // -------------------------------------------------------------------------

    public function testSpecificationsIteratorReturnsMatchingOrmItems(): void
    {
        // age > 28: Alice(1,30), Charlie(3,35), Dave(4,40) → limit 2 → [1,3]
        $result = $this->makeOrmDs()->specificationsIterator([new AgeAboveSpecification(28)], limit: 2);

        self::assertSame([1, 3], $this->ids($result));
    }

    public function testSpecificationsIteratorRespectsOffset(): void
    {
        // age > 28 matches ids 1,3,4; skip first 1, take 2 → [3,4]
        $result = $this->makeOrmDs()->specificationsIterator([new AgeAboveSpecification(28)], limit: 2, offset: 1);

        self::assertSame([3, 4], $this->ids($result));
    }

    public function testSpecificationsIteratorOnRawSqlReturnsMatchingItems(): void
    {
        $result = $this->makeRawDs('SELECT * FROM test_entity r /*#WHERE#*/ ORDER BY r.id ASC')->specificationsIterator([new AgeAboveSpecification(28)], limit: 2);

        self::assertSame([1, 3], $this->ids($result));
    }

    public function testSpecificationsIteratorWithPhpOnlySpecFiltersCorrectly(): void
    {
        // NameEqualsSpecification has no QueryExpression → DB returns all, PHP keeps matches.
        $result = $this->makeOrmDs()->specificationsIterator([new NameEqualsSpecification('Alice')], limit: 5);

        self::assertSame([1], $this->ids($result));
    }

    public function testSpecificationsIteratorWithMultipleSpecsRequiresAllSatisfied(): void
    {
        // age > 28 AND name = Alice → only Alice (id 1)
        $result = $this->makeOrmDs()->specificationsIterator(
            [new AgeAboveSpecification(28), new NameEqualsSpecification('Alice')],
            limit: 5,
        );

        self::assertSame([1], $this->ids($result));
    }

    public function testSpecificationsIteratorReturnsEmptyWhenNothingMatches(): void
    {
        $result = $this->makeOrmDs()->specificationsIterator([new NameEqualsSpecification('Nobody')], limit: 5);

        self::assertSame([], $this->ids($result));
    }

    public function testSpecificationsIteratorWithoutLimitReturnsAllMatching(): void
    {
        // age > 28: ids 1,3,4
        $result = $this->makeOrmDs()->specificationsIterator([new AgeAboveSpecification(28)]);

        self::assertSame([1, 3, 4], $this->ids($result));
    }

    // -------------------------------------------------------------------------
    // getIterator / data with specs + limit — multi-batch behaviour
    //
    // Seeded rows (ordered by id):
    //   1 Alice   age=30  ← age>28 ✓
    //   2 Bob     age=25  ← age>28 ✗
    //   3 Charlie age=35  ← age>28 ✓
    //   4 Dave    age=40  ← age>28 ✓
    //   5 Eve     age=28  ← age>28 ✗
    //
    // Naive DB-limit-first strategy for limit=3:
    //   DB returns rows 1-3, PHP keeps [1,3] → only 2 items despite limit=3.
    //
    // Correct strategy (specificationsIterator):
    //   Batch 1 (rows 1-3) → matches [1,3]; collected=2, need 1 more
    //   Batch 2 (rows 4-5) → matches [4];   collected=3, done
    //   Result: [1,3,4]
    // -------------------------------------------------------------------------

    public function testDataWithSpecAndLimitSpansMultipleBatchesOrm(): void
    {
        // Proves the DataSource crosses batch boundaries to satisfy the limit.
        $ds = $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withLimit(3);

        self::assertSame([1, 3, 4], $this->ids($ds->data()));
    }

    public function testDataWithSpecAndLimitSpansMultipleBatchesRawSql(): void
    {
        $ds = $this->makeRawDs('SELECT * FROM test_entity r /*#WHERE#*/ ORDER BY r.id ASC')
            ->withSpecification(new AgeAboveSpecification(28))
            ->withLimit(3);

        self::assertSame([1, 3, 4], $this->ids($ds->data()));
    }

    public function testDataWithSpecAndLimitStopsEarlyWhenLimitReached(): void
    {
        // limit=2: only the first 2 matches are collected, remainder is not fetched.
        $ds = $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withLimit(2);

        self::assertSame([1, 3], $this->ids($ds->data()));
    }

    public function testDataWithSpecAndLimitReturnsFewerWhenMatchesAreExhausted(): void
    {
        // Only 3 rows match age>28, but limit=10 → all 3 are returned.
        $ds = $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withLimit(10);

        self::assertSame([1, 3, 4], $this->ids($ds->data()));
    }

    public function testDataWithPhpOnlySpecAndLimitSpansMultipleBatches(): void
    {
        // NameEqualsSpecification carries no QueryExpression → DB returns full batches,
        // PHP filters in memory.  Only Alice (id=1) matches; limit=3 → [1].
        $ds = $this->makeOrmDs()
            ->withSpecification(new NameEqualsSpecification('Alice'))
            ->withLimit(3);

        self::assertSame([1], $this->ids($ds->data()));
    }

    public function testDataWithSpecAndLimitAndOffsetSkipsMatchingItems(): void
    {
        // age>28 matches [1,3,4]. offset=1 skips Alice → [3,4]; limit=2 collects both.
        $ds = $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withLimit(2, 1);

        self::assertSame([3, 4], $this->ids($ds->data()));
    }

    public function testDataWithSpecAndLimitOnRawSqlRespectsOffset(): void
    {
        $ds = $this->makeRawDs('SELECT * FROM test_entity r /*#WHERE#*/ ORDER BY r.id ASC')
            ->withSpecification(new AgeAboveSpecification(28))
            ->withLimit(2, 1);

        self::assertSame([3, 4], $this->ids($ds->data()));
    }

    public function testDataWithSpecAndLimitDoesNotMutateOriginal(): void
    {
        $original = $this->makeOrmDs()->withSpecification(new AgeAboveSpecification(28));
        $original->withLimit(1);

        // The original (no limit) should still return all 3 matching items.
        self::assertSame([1, 3, 4], $this->ids($original->data()));
    }

    public function testTotalCountWithSpecAndLimitThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeOrmDs()
            ->withSpecification(new AgeAboveSpecification(28))
            ->withLimit(1)
            ->totalCount();
    }
}
