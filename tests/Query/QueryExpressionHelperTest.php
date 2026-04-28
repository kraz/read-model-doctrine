<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Query;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\QueryBuilder;
use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\Query\SortExpression;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModelDoctrine\Query\AbstractRawQuery;
use Kraz\ReadModelDoctrine\Query\QueryExpressionHelper;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Tests\Fixtures\CompositeKeyEntity;
use Kraz\ReadModelDoctrine\Tests\Fixtures\TestEntity;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function array_map;
use function count;
use function iterator_to_array;
use function sort;

/**
 * @phpstan-import-type FilterComposite from FilterExpression
 * @phpstan-import-type SortComposite from SortExpression
 */
#[CoversClass(QueryExpressionHelper::class)]
final class QueryExpressionHelperTest extends TestCase
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
            [6, 'alfred', 'alfred@example.com', 'eng', 50, 1],
        ];
        foreach ($rows as [$id, $name, $email, $dept, $age, $active]) {
            $this->connection->executeStatement(
                'INSERT INTO test_entity (id, name, email, department, age, active) VALUES (?, ?, ?, ?, ?, ?)',
                [$id, $name, $email, $dept, $age, $active],
            );
        }
    }

    private function newQb(): QueryBuilder
    {
        return $this->em->createQueryBuilder()
            ->select('u')
            ->from(TestEntity::class, 'u');
    }

    /** @phpstan-param QueryBuilder|AbstractRawQuery<array<string, mixed>|object> $value */
    private function asQb(QueryBuilder|AbstractRawQuery $value): QueryBuilder
    {
        self::assertInstanceOf(QueryBuilder::class, $value);

        return $value;
    }

    /**
     * @phpstan-param FilterComposite|array<never, never> $filter
     * @phpstan-param SortComposite|null                  $sort
     * @phpstan-param list<int|string>|null               $values
     *
     * @return list<int>
     */
    private function applyAndGetIds(array $filter, array|null $sort = null, array|null $values = null): array
    {
        $qb = $this->newQb();
        $qe = QueryExpression::create([
            'filter' => $filter !== [] ? $filter : null,
            'sort' => $sort,
            'values' => $values,
        ]);

        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe));

        $rows = $result->getQuery()->getArrayResult();
        $ids  = [];
        foreach ($rows as $row) {
            $ids[] = (int) $row['id'];
        }

        return $ids;
    }

    public function testApplyWithEmptyExpressionReturnsUnchangedClone(): void
    {
        $qb = $this->newQb();
        $qe = QueryExpression::create();

        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe));

        self::assertNotSame($qb, $result);
        self::assertSame($qb->getDQL(), $result->getDQL());
    }

    public function testEqOperator(): void
    {
        $ids = $this->applyAndGetIds(['field' => 'name', 'operator' => 'eq', 'value' => 'Alice']);
        self::assertSame([1], $ids);
    }

    public function testEqOperatorIsCaseInsensitiveByDefault(): void
    {
        // With default ignoreCase=true (the FilterExpression default for valX)
        $ids = $this->applyAndGetIds([
            'field' => 'name',
            'operator' => 'eq',
            'value' => 'ALICE',
            'ignoreCase' => true,
        ]);
        self::assertSame([1], $ids);
    }

    public function testEqOperatorCaseSensitiveWhenIgnoreCaseFalse(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'name',
            'operator' => 'eq',
            'value' => 'ALICE',
            'ignoreCase' => false,
        ]);
        self::assertSame([], $ids);
    }

    public function testNeqOperator(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'department',
            'operator' => 'neq',
            'value' => 'eng',
            'ignoreCase' => false,
        ]);
        sort($ids);
        self::assertSame([3, 4, 5], $ids);
    }

    public function testIsNullOperator(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'email',
            'operator' => 'isnull',
        ]);
        self::assertSame([5], $ids);
    }

    public function testIsNotNullOperator(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'email',
            'operator' => 'isnotnull',
        ]);
        sort($ids);
        self::assertSame([1, 2, 3, 4, 6], $ids);
    }

    public function testLtOperator(): void
    {
        $ids = $this->applyAndGetIds(['field' => 'age', 'operator' => 'lt', 'value' => 30]);
        sort($ids);
        self::assertSame([2, 5], $ids);
    }

    public function testLteOperator(): void
    {
        $ids = $this->applyAndGetIds(['field' => 'age', 'operator' => 'lte', 'value' => 30]);
        sort($ids);
        self::assertSame([1, 2, 5], $ids);
    }

    public function testGtOperator(): void
    {
        $ids = $this->applyAndGetIds(['field' => 'age', 'operator' => 'gt', 'value' => 30]);
        sort($ids);
        self::assertSame([3, 4, 6], $ids);
    }

    public function testGteOperator(): void
    {
        $ids = $this->applyAndGetIds(['field' => 'age', 'operator' => 'gte', 'value' => 30]);
        sort($ids);
        self::assertSame([1, 3, 4, 6], $ids);
    }

    public function testStartsWithCaseInsensitive(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'name',
            'operator' => 'startswith',
            'value' => 'AL',
        ]);
        sort($ids);
        // Alice (id=1) and alfred (id=6) match.
        self::assertSame([1, 6], $ids);
    }

    public function testStartsWithCaseSensitive(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'name',
            'operator' => 'startswith',
            'value' => 'A',
            'ignoreCase' => false,
        ]);
        self::assertSame([1], $ids);
    }

    public function testDoesNotStartWith(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'name',
            'operator' => 'doesnotstartwith',
            'value' => 'a',
        ]);
        sort($ids);
        // Excludes Alice and alfred.
        self::assertSame([2, 3, 4, 5], $ids);
    }

    public function testEndsWith(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'name',
            'operator' => 'endswith',
            'value' => 'e',
        ]);
        sort($ids);
        // Alice, Charlie, Dave, Eve.
        self::assertSame([1, 3, 4, 5], $ids);
    }

    public function testDoesNotEndWith(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'name',
            'operator' => 'doesnotendwith',
            'value' => 'e',
        ]);
        sort($ids);
        self::assertSame([2, 6], $ids);
    }

    public function testContains(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'name',
            'operator' => 'contains',
            'value' => 'li',
        ]);
        sort($ids);
        // Alice, Charlie.
        self::assertSame([1, 3], $ids);
    }

    public function testDoesNotContain(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'name',
            'operator' => 'doesnotcontain',
            'value' => 'li',
        ]);
        sort($ids);
        self::assertSame([2, 4, 5, 6], $ids);
    }

    public function testIsEmptyMatchesNullAndEmptyString(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'email',
            'operator' => 'isempty',
        ]);
        sort($ids);
        // Dave's email is '' and Eve's email is null.
        self::assertSame([4, 5], $ids);
    }

    public function testIsNotEmpty(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'email',
            'operator' => 'isnotempty',
        ]);
        sort($ids);
        self::assertSame([1, 2, 3, 6], $ids);
    }

    public function testInListWithSingleValueUsesEquality(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'department',
            'operator' => 'inlist',
            'value' => ['eng'],
        ]);
        sort($ids);
        self::assertSame([1, 2, 6], $ids);
    }

    public function testInListWithCommaSeparatedString(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'department',
            'operator' => 'inlist',
            'value' => 'eng, sales',
        ]);
        sort($ids);
        self::assertSame([1, 2, 3, 4, 6], $ids);
    }

    public function testNotInList(): void
    {
        $ids = $this->applyAndGetIds([
            'field' => 'department',
            'operator' => 'notinlist',
            'value' => ['eng', 'sales'],
        ]);
        self::assertSame([5], $ids);
    }

    public function testCompositeAndExpression(): void
    {
        $ids = $this->applyAndGetIds([
            'logic' => 'and',
            'filters' => [
                ['field' => 'department', 'operator' => 'eq', 'value' => 'eng'],
                ['field' => 'age', 'operator' => 'gte', 'value' => 30],
            ],
        ]);
        sort($ids);
        self::assertSame([1, 6], $ids);
    }

    public function testCompositeOrExpression(): void
    {
        $ids = $this->applyAndGetIds([
            'logic' => 'or',
            'filters' => [
                ['field' => 'department', 'operator' => 'eq', 'value' => 'support'],
                ['field' => 'age', 'operator' => 'gte', 'value' => 40],
            ],
        ]);
        sort($ids);
        // dept=support => 5; age>=40 => 4, 6.
        self::assertSame([4, 5, 6], $ids);
    }

    public function testNotInvertsExpression(): void
    {
        $ids = $this->applyAndGetIds([
            'not' => true,
            'logic' => 'and',
            'filters' => [
                ['field' => 'department', 'operator' => 'eq', 'value' => 'eng'],
            ],
        ]);
        sort($ids);
        // NOT (department = 'eng') => 3, 4, 5.
        self::assertSame([3, 4, 5], $ids);
    }

    public function testSortAsc(): void
    {
        $ids = $this->applyAndGetIds(
            [],
            [['field' => 'age', 'dir' => 'ASC']],
        );
        self::assertSame([2, 5, 1, 3, 4, 6], $ids);
    }

    public function testSortDesc(): void
    {
        $ids = $this->applyAndGetIds(
            [],
            [['field' => 'age', 'dir' => 'DESC']],
        );
        self::assertSame([6, 4, 3, 1, 5, 2], $ids);
    }

    public function testSortMultipleFields(): void
    {
        $ids = $this->applyAndGetIds(
            [],
            [
                ['field' => 'department', 'dir' => 'ASC'],
                ['field' => 'age', 'dir' => 'ASC'],
            ],
        );
        // eng (2,1,6), sales (3,4), support (5).
        self::assertSame([2, 1, 6, 3, 4, 5], $ids);
    }

    public function testInvalidSortDirectionThrows(): void
    {
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'sort' => [['field' => 'age', 'dir' => 'BOGUS']],
        ]);
        $helper = QueryExpressionHelper::create($qb);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/Invalid sort direction/');
        $helper->apply($qe);
    }

    public function testEmptySortFieldThrows(): void
    {
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'sort' => [['field' => '', 'dir' => 'ASC']],
        ]);
        $helper = QueryExpressionHelper::create($qb);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/sort rule must specify a field/');
        $helper->apply($qe);
    }

    public function testValuesQuerySingleId(): void
    {
        $qb = $this->newQb();
        $qe = QueryExpression::create()->withValues([3]);

        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe));

        $rows = $result->getQuery()->getArrayResult();
        self::assertCount(1, $rows);
        self::assertSame(3, (int) $rows[0]['id']);
    }

    public function testValuesQueryMultipleIds(): void
    {
        $qb = $this->newQb();
        $qe = QueryExpression::create()->withValues([2, 4, 6]);

        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe));

        $rows = $result->getQuery()->getArrayResult();
        $ids  = array_map(static fn ($r) => (int) $r['id'], $rows);
        sort($ids);
        self::assertSame([2, 4, 6], $ids);
    }

    public function testFieldMappingViaOptions(): void
    {
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'filter' => ['field' => 'mappedDept', 'operator' => 'eq', 'value' => 'eng', 'ignoreCase' => false],
        ]);
        $helper = QueryExpressionHelper::create($qb, null, [
            'field_map' => ['mappedDept' => 'department'],
        ]);
        $result = $this->asQb($helper->apply($qe));
        $rows   = $result->getQuery()->getArrayResult();
        $ids    = array_map(static fn ($r) => (int) $r['id'], $rows);
        sort($ids);
        self::assertSame([1, 2, 6], $ids);
    }

    public function testFieldMapFromDescriptor(): void
    {
        $descriptor = new ReadModelDescriptor(
            properties: ['mappedDept'],
            operators: [],
            ignoreCase: [],
            fieldMap: ['mappedDept' => 'department'],
        );

        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'filter' => ['field' => 'mappedDept', 'operator' => 'eq', 'value' => 'sales', 'ignoreCase' => false],
        ]);
        $helper = QueryExpressionHelper::create($qb, $descriptor);
        $result = $this->asQb($helper->apply($qe));
        $rows   = $result->getQuery()->getArrayResult();
        $ids    = array_map(static fn ($r) => (int) $r['id'], $rows);
        sort($ids);
        self::assertSame([3, 4], $ids);
    }

    public function testIncludeDataFilterOnlySkipsSort(): void
    {
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'filter' => ['field' => 'department', 'operator' => 'eq', 'value' => 'eng', 'ignoreCase' => false],
            'sort' => [['field' => 'age', 'dir' => 'DESC']],
        ]);
        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe, QueryExpressionProviderInterface::INCLUDE_DATA_FILTER));

        $dql = $result->getDQL();
        self::assertStringContainsString('WHERE', $dql);
        self::assertStringNotContainsString('ORDER BY', $dql);
    }

    public function testIncludeDataSortOnlySkipsFilter(): void
    {
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'filter' => ['field' => 'department', 'operator' => 'eq', 'value' => 'eng'],
            'sort' => [['field' => 'age', 'dir' => 'ASC']],
        ]);
        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe, QueryExpressionProviderInterface::INCLUDE_DATA_SORT));

        $dql = $result->getDQL();
        self::assertStringNotContainsString('WHERE', $dql);
        self::assertStringContainsString('ORDER BY', $dql);
    }

    public function testExpressionsOptionForSort(): void
    {
        // Use SQL expression for sort ordering.
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'sort' => [['field' => 'ageBucket', 'dir' => 'ASC']],
        ]);
        $helper = QueryExpressionHelper::create($qb, null, [
            'expressions' => [
                'ageBucket' => ['exp' => 'u.age'],
            ],
        ]);
        $result = $this->asQb($helper->apply($qe));
        $rows   = $result->getQuery()->getArrayResult();
        $ids    = array_map(static fn ($r) => (int) $r['id'], $rows);
        self::assertSame([2, 5, 1, 3, 4, 6], $ids);
    }

    public function testExpressionsOptionForFilter(): void
    {
        // Filter on a custom expression.
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'filter' => ['field' => 'u.age', 'operator' => 'gte', 'value' => 35],
        ]);
        $helper = QueryExpressionHelper::create($qb, null, [
            'expressions' => [
                'u.age' => ['exp' => 'u.age'],
            ],
        ]);
        $result = $this->asQb($helper->apply($qe));
        $rows   = $result->getQuery()->getArrayResult();
        $ids    = array_map(static fn ($r) => (int) $r['id'], $rows);
        sort($ids);
        self::assertSame([3, 4, 6], $ids);
    }

    public function testGroupsOptionExpandsFieldToMultipleFields(): void
    {
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'filter' => [
                'field' => 'search',
                'operator' => 'contains',
                'value' => 'alice',
            ],
        ]);
        $helper = QueryExpressionHelper::create($qb, null, [
            'groups' => [
                'search' => [
                    'logic' => 'or',
                    'fields' => ['name', 'email'],
                ],
            ],
        ]);
        $result = $this->asQb($helper->apply($qe));
        $rows   = $result->getQuery()->getArrayResult();
        $ids    = array_map(static fn ($r) => (int) $r['id'], $rows);
        self::assertSame([1], $ids);
    }

    public function testMissingFilterOperatorInsideLogicGroupThrows(): void
    {
        // Wrap the bad inner filter in a logic group so it is reached by the normalizer.
        $qb = $this->newQb();
        $qe = QueryExpression::create([
            'filter' => [
                'logic' => 'and',
                'filters' => [
                    ['field' => 'name'], // no operator
                ],
            ],
        ]);

        $helper = QueryExpressionHelper::create($qb);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/Missing filter operator/');
        $helper->apply($qe);
    }

    public function testMissingFilterValueThrowsForOperatorThatNeedsIt(): void
    {
        $qb = $this->newQb();
        $qe = QueryExpression::create([
            'filter' => [
                'logic' => 'and',
                'filters' => [
                    ['field' => 'name', 'operator' => 'eq'], // missing value
                ],
            ],
        ]);

        $helper = QueryExpressionHelper::create($qb);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/Missing filter value/');
        $helper->apply($qe);
    }

    public function testUnsupportedFilterOperatorThrows(): void
    {
        $qb = $this->newQb();
        $qe = QueryExpression::create([
            'filter' => [
                'logic' => 'and',
                'filters' => [
                    ['field' => 'name', 'operator' => 'bogusop', 'value' => 'x'],
                ],
            ],
        ]);

        $helper = QueryExpressionHelper::create($qb);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/Unsupported filter operator/');
        $helper->apply($qe);
    }

    public function testCompositeIdentifierThrowsForValuesQuery(): void
    {
        $qb = $this->em->createQueryBuilder()
            ->select('c')
            ->from(CompositeKeyEntity::class, 'c');
        $qe = QueryExpression::create()->withValues([1]);

        $helper = QueryExpressionHelper::create($qb);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/Composite root identifiers/');
        $helper->apply($qe);
    }

    public function testRootIdentifierWithDotThrows(): void
    {
        $qb = $this->newQb();
        $qe = QueryExpression::create()->withValues([1]);

        $helper = QueryExpressionHelper::create($qb, null, ['root_identifier' => 'u.id']);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/must not contain "."/');
        $helper->apply($qe);
    }

    public function testValuesQueryWithExplicitRootIdentifierOption(): void
    {
        $qb = $this->newQb();
        $qe = QueryExpression::create()->withValues([1, 2]);

        $helper = QueryExpressionHelper::create($qb, null, ['root_identifier' => 'id']);
        $result = $this->asQb($helper->apply($qe));
        $rows   = $result->getQuery()->getArrayResult();
        $ids    = array_map(static fn ($r) => (int) $r['id'], $rows);
        sort($ids);
        self::assertSame([1, 2], $ids);
    }

    public function testRawQueryPathWithFilterAndSort(): void
    {
        $rawQuery = new RawQuery($this->connection);
        $rawQuery->setSql('SELECT id, name, age FROM test_entity');

        $qe = QueryExpression::create([
            'filter' => ['field' => 'age', 'operator' => 'gte', 'value' => 30],
            'sort' => [['field' => 'age', 'dir' => 'ASC']],
        ]);

        $helper = QueryExpressionHelper::create($rawQuery);
        /** @var RawQuery<array<string, mixed>> $applied */
        $applied = $helper->apply($qe);

        $rows = iterator_to_array($applied->toIterable());
        $ids  = array_map(static fn ($r) => (int) $r['id'], $rows);
        self::assertSame([1, 3, 4, 6], $ids);
    }

    public function testRawQueryValuesUsesRootIdentifierOption(): void
    {
        $rawQuery = new RawQuery($this->connection);
        $rawQuery->setSql('SELECT id, name FROM test_entity');

        $qe = QueryExpression::create()->withValues([3, 5]);

        $helper = QueryExpressionHelper::create($rawQuery, null, ['root_identifier' => 'id']);
        /** @var RawQuery<array<string, mixed>> $applied */
        $applied = $helper->apply($qe);

        $rows = iterator_to_array($applied->toIterable());
        $ids  = array_map(static fn ($r) => (int) $r['id'], $rows);
        sort($ids);
        self::assertSame([3, 5], $ids);
    }

    public function testRawQueryWithoutWherePlaceholderWrapsQuery(): void
    {
        $rawQuery = new RawQuery($this->connection);
        $rawQuery->setSql('SELECT id, name FROM test_entity');

        $qe = QueryExpression::create([
            'filter' => ['field' => 'name', 'operator' => 'eq', 'value' => 'Alice'],
        ]);

        $helper = QueryExpressionHelper::create($rawQuery);
        /** @var RawQuery<array<string, mixed>> $applied */
        $applied = $helper->apply($qe);

        // The helper wraps the query when no placeholder is present.
        self::assertStringContainsString('SELECT * FROM (', $applied->getExtendedSql());

        $rows = iterator_to_array($applied->toIterable());
        $ids  = array_map(static fn ($r) => (int) $r['id'], $rows);
        self::assertSame([1], $ids);
    }

    public function testRawQueryUsesPlaceholderInsteadOfWrapping(): void
    {
        $rawQuery = new RawQuery($this->connection);
        $rawQuery->setSql('SELECT id, name, age, department FROM test_entity /*#WHERE#*/ /*#ORDERBY#*/');

        $qe = QueryExpression::create([
            'filter' => ['field' => 'department', 'operator' => 'eq', 'value' => 'eng'],
            'sort' => [['field' => 'age', 'dir' => 'DESC']],
        ]);

        $helper = QueryExpressionHelper::create($rawQuery);
        /** @var RawQuery<array<string, mixed>> $applied */
        $applied = $helper->apply($qe);

        $sql = $applied->getExtendedSql();
        self::assertStringNotContainsString('SELECT * FROM (', $sql);
        self::assertStringContainsString(' WHERE ', $sql);
        self::assertStringContainsString(' ORDER BY ', $sql);

        $rows = iterator_to_array($applied->toIterable());
        $ids  = array_map(static fn ($r) => (int) $r['id'], $rows);
        // eng members ordered by age desc => 6 (50), 1 (30), 2 (25).
        self::assertSame([6, 1, 2], $ids);
    }

    public function testQuoteFieldNamesAndTableAlias(): void
    {
        // Build a raw SQL query against quoted columns.
        $rawQuery = new RawQuery($this->connection);
        $rawQuery->setSql('SELECT * FROM (SELECT id AS "id", name AS "name" FROM test_entity) "u" /*#WHERE#*/');

        $qe = QueryExpression::create([
            'filter' => ['field' => 'u.name', 'operator' => 'eq', 'value' => 'Alice', 'ignoreCase' => false],
        ]);

        $helper = QueryExpressionHelper::create($rawQuery, null, [
            'root_alias' => 'u',
            'quoteFieldNames' => true,
            'quoteTableAlias' => true,
            'quoteFieldNamesChar' => '"',
        ]);
        /** @var RawQuery<array<string, mixed>> $applied */
        $applied = $helper->apply($qe);

        $sql = $applied->getExtendedSql();
        self::assertStringContainsString('"u"."name"', $sql);

        $rows = iterator_to_array($applied->toIterable());
        $ids  = array_map(static fn ($r) => (int) $r['id'], $rows);
        self::assertSame([1], $ids);
    }

    public function testApplyClonesInputQueryBuilder(): void
    {
        $qb = $this->newQb();
        $qe = QueryExpression::create([
            'filter' => ['field' => 'name', 'operator' => 'eq', 'value' => 'Alice'],
        ]);

        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe));

        self::assertNotSame($qb, $result);
        self::assertStringNotContainsString('WHERE', $qb->getDQL());
        self::assertStringContainsString('WHERE', $result->getDQL());
    }

    public function testFilterExpressionInstanceIsAccepted(): void
    {
        $filter = FilterExpression::create()->equalTo('name', 'Alice', false);
        $qe     = QueryExpression::create([
            'filter' => $filter->toArray(),
        ]);

        $qb     = $this->newQb();
        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe));

        $rows = $result->getQuery()->getArrayResult();
        $ids  = array_map(static fn ($r) => (int) $r['id'], $rows);
        self::assertSame([1], $ids);
    }

    public function testEqGeneratesUpperLikeWhenIgnoreCaseExplicit(): void
    {
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'filter' => ['field' => 'name', 'operator' => 'eq', 'value' => 'alice', 'ignoreCase' => true],
        ]);
        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe));

        $dql = $result->getDQL();
        self::assertStringContainsString('UPPER(u.name) =', $dql);
    }

    public function testNumericIdComparisonGeneratesIntegerParam(): void
    {
        $qb     = $this->newQb();
        $qe     = QueryExpression::create([
            'filter' => ['field' => 'age', 'operator' => 'lt', 'value' => 30],
        ]);
        $helper = QueryExpressionHelper::create($qb);
        $result = $this->asQb($helper->apply($qe));

        $params = $result->getParameters();
        self::assertGreaterThan(0, count($params));
        $first = $params[0];
        self::assertNotNull($first);
        self::assertSame(30, $first->getValue());
    }
}
