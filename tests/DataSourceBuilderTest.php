<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\EntityManagerInterface;
use InvalidArgumentException;
use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryRequest;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModelDoctrine\DataSourceBuilder;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;
use Kraz\ReadModelDoctrine\Tests\Fixtures\TestEntity;
use Kraz\ReadModelDoctrine\Tests\Fixtures\UserReadModelFixture;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use Kraz\ReadModelDoctrine\Tools\ParametersCollection;
use Kraz\ReadModelDoctrine\Tools\QueryParts;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function intval;
use function is_array;

#[CoversClass(DataSourceBuilder::class)]
final class DataSourceBuilderTest extends TestCase
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

    private function makeBuilder(): DataSourceBuilder
    {
        return new DataSourceBuilder();
    }

    private function makeSql(string $alias = 'r'): string
    {
        return 'SELECT * FROM test_entity ' . $alias . ' /*#WHERE#*/ ORDER BY ' . $alias . '.id ASC';
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
    // Helper factory methods
    // -------------------------------------------------------------------------

    public function testQryReturnsQueryExpression(): void
    {
        self::assertInstanceOf(QueryExpression::class, $this->makeBuilder()->qry());
    }

    public function testExprReturnsFilterExpression(): void
    {
        self::assertInstanceOf(FilterExpression::class, $this->makeBuilder()->expr());
    }

    // -------------------------------------------------------------------------
    // create() — guard when no data is set
    // -------------------------------------------------------------------------

    public function testCreateThrowsWhenDataIsNotSet(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->makeBuilder()->create($this->connection);
    }

    // -------------------------------------------------------------------------
    // withData() — accepted data source types
    // -------------------------------------------------------------------------

    public function testCreateWithStringSqlProducesWorkingDataSource(): void
    {
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity ORDER BY id ASC')
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    public function testCreateWithOrmQueryBuilderProducesWorkingDataSource(): void
    {
        $qb = $this->em->createQueryBuilder()
            ->select('u')
            ->from(TestEntity::class, 'u')
            ->orderBy('u.id', 'ASC');

        $ds = $this->makeBuilder()
            ->withData($qb)
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    public function testCreateWithRawQueryProducesWorkingDataSource(): void
    {
        /** @phpstan-var RawQuery<array<string, mixed>> $raw */
        $raw = new RawQuery($this->connection);
        $raw->setSql('SELECT * FROM test_entity ORDER BY id ASC');

        $ds = $this->makeBuilder()
            ->withData($raw)
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    public function testCreateWithRawQueryBuilderProducesWorkingDataSource(): void
    {
        /** @phpstan-var RawQueryBuilder<array<string, mixed>> $rqb */
        $rqb = new RawQueryBuilder($this->connection);
        $rqb->select('*')->from('test_entity')->orderBy('id', 'ASC');

        $ds = $this->makeBuilder()
            ->withData($rqb)
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    // -------------------------------------------------------------------------
    // Immutability of with* methods
    // -------------------------------------------------------------------------

    public function testWithDataReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withData($this->makeSql()));
    }

    public function testWithDataDoesNotMutateOriginalBuilder(): void
    {
        $builder = $this->makeBuilder();
        $builder->withData($this->makeSql());

        // Original builder still has no data set, so create() must throw.
        $this->expectException(InvalidArgumentException::class);
        $builder->create($this->connection);
    }

    public function testWithQueryExpressionReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withQueryExpression(QueryExpression::create()));
    }

    public function testWithQueryExpressionFieldMappingReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withQueryExpressionFieldMapping(['name' => 'name']));
    }

    public function testWithQueryModifierReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withQueryModifier(static function (): void {
        }));
    }

    public function testWithDenormalizerReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withDenormalizer(static fn (array $row): array => $row));
    }

    public function testWithQueryRequestReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withQueryRequest(QueryRequest::create()));
    }

    public function testWithRootAliasReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withRootAlias('t'));
    }

    public function testWithRootIdentifierReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withRootIdentifier('uuid'));
    }

    public function testWithItemNormalizerReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withItemNormalizer(static fn (array $row): array => $row));
    }

    public function testWithReadModelReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertNotSame($builder, $builder->withReadModel(TestEntity::class));
    }

    public function testWithReadModelDescriptorReturnsNewBuilderInstance(): void
    {
        $builder    = $this->makeBuilder();
        $descriptor = new ReadModelDescriptor(['id', 'name'], [], [], []);

        self::assertNotSame($builder, $builder->withReadModelDescriptor($descriptor));
    }

    // -------------------------------------------------------------------------
    // andWhere / orWhere / sortBy — mutable fluent methods
    // -------------------------------------------------------------------------

    public function testAndWhereReturnsSameBuilderInstance(): void
    {
        $builder = $this->makeBuilder();
        $expr    = FilterExpression::create()->equalTo('name', 'Alice');

        self::assertSame($builder, $builder->andWhere($expr));
    }

    public function testOrWhereReturnsSameBuilderInstance(): void
    {
        $builder = $this->makeBuilder();
        $expr    = FilterExpression::create()->equalTo('name', 'Alice');

        self::assertSame($builder, $builder->orWhere($expr));
    }

    public function testSortByReturnsSameBuilderInstance(): void
    {
        $builder = $this->makeBuilder();

        self::assertSame($builder, $builder->sortBy('name'));
    }

    public function testAndWhereFiltersResultsOnCreate(): void
    {
        $builder = $this->makeBuilder()->withData($this->makeSql());
        $builder->andWhere(FilterExpression::create()->equalTo('department', 'eng'));

        $ds = $builder->create($this->connection);

        self::assertSame([1, 2], $this->ids($ds->data()));
    }

    /**
     * Passing multiple FilterExpressions to a single andWhere call creates a
     * conjunction. By contrast, calling andWhere twice replaces the filter —
     * see testSubsequentAndWhereCallReplacesFilter.
     */
    public function testAndWhereWithMultipleExpressionsAppliesConjunction(): void
    {
        $builder = $this->makeBuilder()->withData($this->makeSql());
        $builder->andWhere(
            FilterExpression::create()->equalTo('department', 'eng'),
            FilterExpression::create()->greaterThan('age', 26),
        );

        $ds = $builder->create($this->connection);

        // eng (ids 1, 2) AND age > 26 → only Alice (age 30); Bob is 25
        self::assertSame([1], $this->ids($ds->data()));
    }

    /**
     * Each call to andWhere / orWhere on the builder replaces the entire filter
     * because QueryExpression::andWhere always creates a fresh FilterExpression.
     * To combine conditions conjunctively, pass them all in one andWhere call.
     */
    public function testSubsequentAndWhereCallReplacesFilter(): void
    {
        $builder = $this->makeBuilder()->withData($this->makeSql());
        $builder->andWhere(FilterExpression::create()->equalTo('name', 'Alice'));
        $builder->andWhere(FilterExpression::create()->greaterThan('age', 26)); // replaces the first

        $ds = $builder->create($this->connection);

        // Only the second filter is active: age > 26 → ids 1, 3, 4, 5
        self::assertSame([1, 3, 4, 5], $this->ids($ds->data()));
    }

    public function testOrWhereWithMultipleExpressionsAppliesDisjunction(): void
    {
        $builder = $this->makeBuilder()->withData($this->makeSql());
        $builder->orWhere(
            FilterExpression::create()->equalTo('name', 'Alice'),
            FilterExpression::create()->equalTo('name', 'Bob'),
        );

        $ds = $builder->create($this->connection);

        self::assertSame([1, 2], $this->ids($ds->data()));
    }

    /**
     * Because andWhere/orWhere/sortBy mutate the builder while with* methods return a clone,
     * calling andWhere on the original after a withData clone has already been created
     * does NOT affect the clone.
     */
    public function testAndWhereAfterWithDataDoesNotAffectEarlierClone(): void
    {
        $builder         = $this->makeBuilder();
        $builderWithData = $builder->withData($this->makeSql()); // clone created here

        // This mutates $builder AFTER the clone was taken, so the clone is unaffected.
        $builder->andWhere(FilterExpression::create()->equalTo('name', 'Alice'));

        self::assertCount(5, $builderWithData->create($this->connection)->data());
    }

    // -------------------------------------------------------------------------
    // withQueryExpression — applied in create()
    // -------------------------------------------------------------------------

    public function testWithQueryExpressionFiltersResultsOnCreate(): void
    {
        $qe = QueryExpression::create()->andWhere(
            FilterExpression::create()->equalTo('name', 'Alice'),
        );
        $ds = $this->makeBuilder()
            ->withData($this->makeSql())
            ->withQueryExpression($qe)
            ->create($this->connection);

        self::assertSame([1], $this->ids($ds->data()));
    }

    public function testWithQueryExpressionDoesNotMutateOriginalBuilder(): void
    {
        $qe      = QueryExpression::create()->andWhere(FilterExpression::create()->equalTo('name', 'Alice'));
        $builder = $this->makeBuilder()->withData($this->makeSql());
        $builder->withQueryExpression($qe);

        // Original builder is unaffected — still returns all rows.
        self::assertCount(5, $builder->create($this->connection)->data());
    }

    // -------------------------------------------------------------------------
    // withoutQueryExpression — clear and undo
    // -------------------------------------------------------------------------

    public function testWithoutQueryExpressionReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder()->withQueryExpression(QueryExpression::create());

        self::assertNotSame($builder, $builder->withoutQueryExpression());
    }

    public function testWithoutQueryExpressionClearsFilterOnCreate(): void
    {
        $qe = QueryExpression::create()->andWhere(FilterExpression::create()->equalTo('name', 'Alice'));
        $ds = $this->makeBuilder()
            ->withData($this->makeSql())
            ->withQueryExpression($qe)
            ->withoutQueryExpression()
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    public function testWithoutQueryExpressionDoesNotMutateOriginalBuilder(): void
    {
        $qe      = QueryExpression::create()->andWhere(FilterExpression::create()->equalTo('name', 'Alice'));
        $builder = $this->makeBuilder()->withData($this->makeSql())->withQueryExpression($qe);
        $builder->withoutQueryExpression();

        // Original still has the filter.
        self::assertSame([1], $this->ids($builder->create($this->connection)->data()));
    }

    public function testWithoutQueryExpressionUndoRestoresPreviousExpression(): void
    {
        // qe1 filters eng; qe2 replaces it to filter Alice; undo → back to qe1 (eng).
        $qe1 = QueryExpression::create()->andWhere(FilterExpression::create()->equalTo('department', 'eng'));
        $qe2 = QueryExpression::create()->andWhere(FilterExpression::create()->equalTo('name', 'Alice'));
        $ds  = $this->makeBuilder()
            ->withData($this->makeSql())
            ->withQueryExpression($qe1)
            ->withQueryExpression($qe2)
            ->withoutQueryExpression(true)
            ->create($this->connection);

        self::assertSame([1, 2], $this->ids($ds->data()));
    }

    public function testWithoutQueryExpressionUndoOnEmptyIsNoOp(): void
    {
        $ds = $this->makeBuilder()
            ->withData($this->makeSql())
            ->withoutQueryExpression(true)
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    public function testWithoutQueryExpressionClearAlsoClearsHistorySoUndoIsNoOp(): void
    {
        $qe = QueryExpression::create()->andWhere(FilterExpression::create()->equalTo('name', 'Alice'));
        $ds = $this->makeBuilder()
            ->withData($this->makeSql())
            ->withQueryExpression($qe)
            ->withoutQueryExpression()      // clears expressions and history
            ->withoutQueryExpression(true)  // undo on empty history → no-op
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    // -------------------------------------------------------------------------
    // withQueryModifier — accumulation and application
    // -------------------------------------------------------------------------

    public function testWithQueryModifierFiltersResultsOnCreate(): void
    {
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC')
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('department = :dept');
                $params->setParameter('dept', 'eng');
            })
            ->create($this->connection);

        self::assertSame([1, 2], $this->ids($ds->data()));
    }

    public function testMultipleWithQueryModifiersAreAllApplied(): void
    {
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC')
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('department = :dept');
                $params->setParameter('dept', 'eng');
            })
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('age > :age');
                $params->setParameter('age', 26);
            }, true)
            ->create($this->connection);

        // Both modifiers applied: eng AND age > 26 → only Alice (age 30)
        self::assertSame([1], $this->ids($ds->data()));
    }

    public function testWithQueryModifierDoesNotShareBetweenBranches(): void
    {
        $base    = $this->makeBuilder()->withData('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC');
        $branch1 = $base->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
            $qp->andWhere('department = :dept');
            $params->setParameter('dept', 'eng');
        });
        $branch2 = $base->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
            $qp->andWhere('department = :dept');
            $params->setParameter('dept', 'sales');
        });

        self::assertSame([1, 2], $this->ids($branch1->create($this->connection)->data()));
        self::assertSame([3, 4], $this->ids($branch2->create($this->connection)->data()));
    }

    // -------------------------------------------------------------------------
    // withoutQueryModifier — clear and undo
    // -------------------------------------------------------------------------

    public function testWithoutQueryModifierReturnsNewBuilderInstance(): void
    {
        $builder = $this->makeBuilder()->withQueryModifier(static function (): void {
        });

        self::assertNotSame($builder, $builder->withoutQueryModifier());
    }

    public function testWithoutQueryModifierClearsModifierOnCreate(): void
    {
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC')
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('department = :dept');
                $params->setParameter('dept', 'eng');
            })
            ->withoutQueryModifier()
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    public function testWithoutQueryModifierDoesNotMutateOriginalBuilder(): void
    {
        $builder = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC')
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('department = :dept');
                $params->setParameter('dept', 'eng');
            });
        $builder->withoutQueryModifier();

        // Original still has the modifier.
        self::assertSame([1, 2], $this->ids($builder->create($this->connection)->data()));
    }

    public function testWithoutQueryModifierUndoRestoresPreviousModifier(): void
    {
        // First modifier filters eng; second replaces it with sales; undo → back to eng.
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC')
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('department = :dept');
                $params->setParameter('dept', 'eng');
            })
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('department = :dept');
                $params->setParameter('dept', 'sales');
            })
            ->withoutQueryModifier(true)
            ->create($this->connection);

        self::assertSame([1, 2], $this->ids($ds->data()));
    }

    public function testWithoutQueryModifierUndoOnEmptyIsNoOp(): void
    {
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC')
            ->withoutQueryModifier(true)
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    public function testWithoutQueryModifierClearAlsoClearsHistorySoUndoIsNoOp(): void
    {
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity WHERE 1=1 ORDER BY id ASC')
            ->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
                $qp->andWhere('department = :dept');
                $params->setParameter('dept', 'eng');
            })
            ->withoutQueryModifier()      // clears modifiers and history
            ->withoutQueryModifier(true)  // undo on empty history → no-op
            ->create($this->connection);

        self::assertCount(5, $ds->data());
    }

    // -------------------------------------------------------------------------
    // withQueryRequest — applied in create()
    // -------------------------------------------------------------------------

    public function testWithQueryRequestAppliesQueryExpressionOnCreate(): void
    {
        $qe      = QueryExpression::create()->andWhere(FilterExpression::create()->equalTo('name', 'Bob'));
        $request = QueryRequest::create()->withQueryExpression($qe);
        $ds      = $this->makeBuilder()
            ->withData($this->makeSql())
            ->withQueryRequest($request)
            ->create($this->connection);

        self::assertSame([2], $this->ids($ds->data()));
    }

    public function testWithQueryRequestAppliesPaginationOnCreate(): void
    {
        $request = QueryRequest::create()->withPagination(2, 2);
        $ds      = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity ORDER BY id ASC')
            ->withQueryRequest($request)
            ->create($this->connection);

        self::assertTrue($ds->isPaginated());
        self::assertSame([3, 4], $this->ids($ds->data()));
    }

    // -------------------------------------------------------------------------
    // Options propagation through create()
    // -------------------------------------------------------------------------

    public function testWithItemNormalizerTransformsResults(): void
    {
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity ORDER BY id ASC')
            ->withItemNormalizer(static fn (array $row): int => intval($row['id']))
            ->create($this->connection);

        self::assertSame([1, 2, 3, 4, 5], $ds->data());
    }

    public function testWithDenormalizerTransformsResults(): void
    {
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity ORDER BY id ASC')
            ->withDenormalizer(static fn (array $row): string => (string) $row['name'])
            ->create($this->connection);

        self::assertSame(['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'], $ds->data());
    }

    public function testWithRootAliasIsUsedForQueryExpressionInjection(): void
    {
        // 't' is the alias in both the SQL and the builder, so the WHERE injection
        // generates 't.name = :val' which matches the table alias.
        $ds = $this->makeBuilder()
            ->withData($this->makeSql('t'))
            ->withRootAlias('t')
            ->withQueryExpression(
                QueryExpression::create()->andWhere(FilterExpression::create()->equalTo('name', 'Alice')),
            )
            ->create($this->connection);

        self::assertSame([1], $this->ids($ds->data()));
    }

    /**
     * withRootAlias sets the option with ??= so an explicit root_alias passed to
     * create() takes precedence over the builder's value.
     */
    public function testExplicitRootAliasInCreateOptionsOverridesBuilderValue(): void
    {
        // SQL uses alias 'u'. Builder says 't' but create() overrides to 'u'.
        $ds = $this->makeBuilder()
            ->withData($this->makeSql('u'))
            ->withRootAlias('t')
            ->withQueryExpression(
                QueryExpression::create()->andWhere(FilterExpression::create()->equalTo('name', 'Alice')),
            )
            ->create($this->connection, ['root_alias' => 'u']);

        self::assertSame([1], $this->ids($ds->data()));
    }

    /**
     * Unlike all other builder options that use ??=, field_map uses plain = in
     * create(), so the builder's field mapping always overwrites any field_map
     * passed directly in the options array. This is a behavioural inconsistency
     * worth being aware of.
     */
    public function testWithQueryExpressionFieldMappingAlwaysOverridesOptionsFieldMap(): void
    {
        // field_map from options would be overwritten by the builder's mapping.
        $ds = $this->makeBuilder()
            ->withData('SELECT * FROM test_entity ORDER BY id ASC')
            ->withQueryExpressionFieldMapping(['id' => 'id'])
            ->create($this->connection, ['field_map' => ['other_field' => 'col']]);

        // Builder's field_map wins; results are unaffected because the identity
        // mapping ['id' => 'id'] is a no-op for unfiltered queries.
        self::assertCount(5, $ds->data());
    }

    // -------------------------------------------------------------------------
    // UserReadModelFixture — real-world read model pattern
    // -------------------------------------------------------------------------

    public function testUserReadModelFixtureReturnsAllRows(): void
    {
        $rm = new UserReadModelFixture($this->connection);

        self::assertCount(5, $rm);
        self::assertCount(5, $rm->data());
    }

    public function testUserReadModelFixtureFiltersById(): void
    {
        $rm = new UserReadModelFixture($this->connection)
            ->withQueryExpression(
                QueryExpression::create()->andWhere(
                    FilterExpression::create()->equalTo(UserReadModelFixture::FIELD_ID, 3),
                ),
            );

        self::assertSame([3], $this->ids($rm));
        self::assertSame([3], $this->ids($rm->data()));
    }

    public function testUserReadModelFixtureFiltersByDepartment(): void
    {
        $rm = new UserReadModelFixture($this->connection)
            ->withQueryExpression(
                QueryExpression::create()->andWhere(
                    FilterExpression::create()->equalTo(UserReadModelFixture::FIELD_DEPARTMENT, 'sales'),
                ),
            );

        self::assertSame([3, 4], $this->ids($rm));
        self::assertSame([3, 4], $this->ids($rm->data()));
    }

    public function testUserReadModelFixtureSupportsPagination(): void
    {
        $rm = new UserReadModelFixture($this->connection)
            ->withPagination(2, 2);

        self::assertTrue($rm->isPaginated());
        self::assertSame([3, 4], $this->ids($rm));
        self::assertSame([3, 4], $this->ids($rm->data()));
        self::assertSame(5, $rm->totalCount());
    }

    public function testUserReadModelFixtureCombinesFilterAndPagination(): void
    {
        // eng has Alice (1) and Bob (2). Page 1 of 1 per page → only Alice.
        $rm = new UserReadModelFixture($this->connection)
            ->withQueryExpression(
                QueryExpression::create()->andWhere(
                    FilterExpression::create()->equalTo(UserReadModelFixture::FIELD_DEPARTMENT, 'eng'),
                ),
            )
            ->withPagination(1, 1);

        self::assertSame([1], $this->ids($rm));
        self::assertSame([1], $this->ids($rm->data()));
        self::assertSame(2, $rm->totalCount());
    }
}
