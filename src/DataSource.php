<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform as AbstractDatabasePlatform;
use Doctrine\ORM\AbstractQuery;
use Doctrine\ORM\NativeQuery;
use Doctrine\ORM\Query as ORMQuery;
use Doctrine\ORM\Query\Expr;
use Doctrine\ORM\QueryBuilder;
use Doctrine\ORM\Tools\Pagination\Paginator;
use InvalidArgumentException;
use Kraz\ReadModel\Exception\InvalidCursorException;
use Kraz\ReadModel\Pagination\Cursor\Base64JsonCursorCodec;
use Kraz\ReadModel\Pagination\Cursor\Cursor;
use Kraz\ReadModel\Pagination\Cursor\CursorCodecInterface;
use Kraz\ReadModel\Pagination\Cursor\CursorPaginatorInterface;
use Kraz\ReadModel\Pagination\Cursor\Direction;
use Kraz\ReadModel\Pagination\PaginatorInterface;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\Query\SortExpression;
use Kraz\ReadModel\ReadDataProviderAccess;
use Kraz\ReadModel\ReadDataProviderComposition;
use Kraz\ReadModel\ReadDataProviderCompositionInterface;
use Kraz\ReadModel\ReadDataProviderInterface;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModelDoctrine\Pagination\DoctrineCursorPaginator;
use Kraz\ReadModelDoctrine\Pagination\DoctrinePaginator;
use Kraz\ReadModelDoctrine\Pagination\RawSqlPaginator;
use Kraz\ReadModelDoctrine\Query\AbstractRawQuery;
use Kraz\ReadModelDoctrine\Query\QueryExpressionProvider;
use Kraz\ReadModelDoctrine\Query\RawNativeQuery;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;
use Kraz\ReadModelDoctrine\Tools\ParametersCollection;
use Kraz\ReadModelDoctrine\Tools\QueryParts;
use LogicException;
use Nyholm\Psr7\Factory\Psr17Factory;
use Override;
use Psr\Http\Message\RequestInterface;
use RuntimeException;
use Symfony\Bridge\PsrHttpMessage\Factory\PsrHttpFactory;
use Symfony\Component\HttpFoundation\Request as SymfonyRequest;
use Traversable;

use function array_key_exists;
use function array_replace_recursive;
use function call_user_func;
use function class_exists;
use function count;
use function get_class;
use function gettype;
use function is_callable;
use function is_object;
use function is_string;
use function parse_str;
use function sprintf;

/**
 * @phpstan-import-type SqlFormatterOptions from Tools\SqlFormatter
 * @phpstan-type DataSourceOptions = array{
 *     connection: Connection|null,
 *     hydrator: 1|2|3|4|5|6|string,
 *     root_identifier: string|string[],
 *     root_alias: string|string[],
 *     quoteTableAlias: bool,
 *     quoteFieldNames: bool,
 *     quoteFieldNamesChar: string,
 *     read_model_descriptor: ReadModelDescriptor|string|null,
 *     item_normalizer: callable|null,
 *     query?: array{
 *         item_normalizer?: callable|null,
 *     },
 *     paginator?: array{
 *         fetchJoinCollection: bool,
 *         useOutputWalkers: bool,
 *     },
 *     normalizer: callable|null,
 *     denormalizer: callable|null,
 *     field_map: array<string, string>|null,
 *     database_platform_class: array<string, class-string<AbstractDatabasePlatform>>,
 *     sql_formatter: SqlFormatterOptions,
 *     use_count_cache: bool,
 * }
 * @phpstan-type DataSourceOptionsWrapper = DataSourceOptions|array<never, never>
 * @phpstan-template-covariant T of object|array<string, mixed>
 * @phpstan-implements ReadDataProviderInterface<T>
 */
class DataSource implements ReadDataProviderInterface
{
    /** @use ReadDataProviderComposition<T> */
    use ReadDataProviderComposition;
    /** @use ReadDataProviderAccess<T> */
    use ReadDataProviderAccess;

    public const int DEFAULT_HYDRATOR = AbstractQuery::HYDRATE_ARRAY;

    /** @phpstan-var DataSourceOptions */
    private array $options;
    /** @phpstan-var QueryBuilder|AbstractRawQuery<T> */
    private QueryBuilder|AbstractRawQuery $dataSet;
    /** @phpstan-var ORMQuery|AbstractRawQuery<T> */
    private ORMQuery|AbstractRawQuery|null $query = null;
    /** @phpstan-var PaginatorInterface<T>|null */
    private PaginatorInterface|null $paginator = null;

    /** @phpstan-var CursorPaginatorInterface<T>|null */
    private CursorPaginatorInterface|null $cursorPaginator = null;

    private CursorCodecInterface $cursorCodec;

    /**
     * @phpstan-param string|AbstractRawQuery<T>|RawQueryBuilder<T>|NativeQuery|QueryBuilder $data
     * @phpstan-param DataSourceOptionsWrapper $options
     */
    public function __construct(string|AbstractRawQuery|RawQueryBuilder|NativeQuery|QueryBuilder $data, QueryExpressionProviderInterface|null $queryExpressionProvider = null, array $options = [])
    {
        /** @phpstan-var DataSourceOptions $options */
        $options = array_replace_recursive([
            'connection' => null,
            'hydrator' => self::DEFAULT_HYDRATOR,
            'root_identifier' => 'id',
            'root_alias' => 'r',
            'quoteTableAlias' => false,
            'quoteFieldNames' => false,
            'quoteFieldNamesChar' => '"',
            'read_model_descriptor' => null,
            'item_normalizer' => null,
            'normalizer' => null,
            'denormalizer' => null,
            'field_map' => null,
            'database_platform_class' => [],
            'sql_formatter' => [],
            'use_count_cache' => true,
        ], $options);

        $this->options = $options;

        if ($this->options['normalizer'] !== null) {
            $this->options['item_normalizer'] = $this->options['normalizer'];
        }

        if ($this->options['denormalizer'] !== null) {
            $this->options['item_normalizer'] = $this->options['denormalizer'];
        }

        if ($this->options['item_normalizer'] === null) {
            $legacyItemNormalizer = $this->options['query']['item_normalizer'] ?? null;
            if ($legacyItemNormalizer !== null) {
                $this->options['item_normalizer'] = $legacyItemNormalizer;
            }
        }

        /** @phpstan-var QueryBuilder|AbstractRawQuery<T>|null $dataSet */
        $dataSet = null;

        if (is_string($data)) {
            if (($this->options['connection'] ?? null) === null) {
                throw new RuntimeException('You must specify a doctrine "connection" into the DataSource options when using the DataSource with plain (RAW) SQL!');
            }

            if (! $this->options['connection'] instanceof Connection) {
                throw new RuntimeException(sprintf('Invalid DataSource connection. Expected instance of "%s", but got "%s"', Connection::class, is_object($this->options['connection']) ? get_class($this->options['connection']) : gettype($this->options['connection'])));
            }

            /** @phpstan-var RawQuery<T> $dataSet */
            /** @phpstan-ignore argument.type */
            $dataSet = new RawQuery($this->options['connection'], $this->options);
            $dataSet->setSql($data);
        }

        if ($data instanceof NativeQuery) {
            /** @phpstan-var RawNativeQuery<T> $dataSet */
            /** @phpstan-ignore argument.type */
            $dataSet = new RawNativeQuery($data, $this->options);
        }

        if ($data instanceof RawQueryBuilder) {
            $dataSet = $data->getQuery();
        }

        if ($data instanceof AbstractRawQuery) {
            $dataSet = clone $data;
            /** @phpstan-ignore argument.type */
            $dataSet->setOptions($this->options);
        }

        if ($data instanceof QueryBuilder) {
            $dataSet = clone $data;
        }

        if ($dataSet === null) {
            throw new RuntimeException(sprintf('Unsupported data type: %s', is_object($data) ? $data::class : gettype($data)));
        }

        $this->dataSet                 = $dataSet;
        $this->queryExpressionProvider = $queryExpressionProvider;
        $this->cursorCodec             = new Base64JsonCursorCodec();
    }

    /**
     * Replace the codec used to encode and decode cursor tokens.
     *
     * Pass an instance of {@see \Kraz\ReadModel\Pagination\Cursor\SignedCursorCodec} to
     * enable HMAC-based tamper detection. The default codec is opaque but unsigned.
     *
     * @phpstan-return static<T>
     */
    public function withCursorCodec(CursorCodecInterface $codec): static
    {
        /** @phpstan-var static<T> $cloned */
        $cloned              = clone $this;
        $cloned->cursorCodec = $codec;

        return $cloned;
    }

    protected function createDefaultDescriptorFactory(): ReadModelDescriptorFactoryInterface
    {
        return new ReadModelDescriptorFactory();
    }

    protected function createDefaultQueryExpressionProvider(ReadModelDescriptorFactoryInterface $factory): QueryExpressionProviderInterface
    {
        $provider = new QueryExpressionProvider($factory);

        $rootIdentifier = $this->options['root_identifier'] ?? null;
        if ($rootIdentifier !== null && $rootIdentifier !== $provider->getRootIdentifier()) {
            $provider->setRootIdentifier($rootIdentifier);
        }

        $rootAlias = $this->options['root_alias'] ?? null;
        if ($rootAlias !== null && $rootAlias !== $provider->getRootAlias()) {
            $provider->setRootAlias($rootAlias);
        }

        $fieldMapping = $this->options['field_map'] ?? null;
        if ($fieldMapping !== null && $fieldMapping !== $provider->getFieldMapping()) {
            $provider->setFieldMapping($fieldMapping);
        }

        return $provider;
    }

    /** @return AbstractRawQuery<T>|ORMQuery */
    public function getQuery(): AbstractRawQuery|ORMQuery
    {
        if ($this->query !== null) {
            return $this->query;
        }

        $preparedDataSet = clone $this->dataSet;

        $specQEs = [];
        foreach ($this->specifications as $specification) {
            $qe = $specification->getQueryExpression();
            if ($qe === null || $qe->isEmpty()) {
                continue;
            }

            $specQEs[] = $qe;
        }

        $queryExpressionProvider = $this->getOrCreateQueryExpressionProvider();
        $allQEs                  = [...$specQEs, ...$this->queryExpressions];
        $mergedValues            = $this->collectInputValues();
        if (count($mergedValues) > 0) {
            foreach ($allQEs as $queryExpression) {
                $preparedDataSet = $queryExpressionProvider->apply(
                    $preparedDataSet,
                    $queryExpression,
                    null,
                    $this->options,
                    QueryExpressionProviderInterface::INCLUDE_DATA_FILTER | QueryExpressionProviderInterface::INCLUDE_DATA_SORT,
                );
            }

            $preparedDataSet = $queryExpressionProvider->apply(
                $preparedDataSet,
                QueryExpression::create()->withValues($mergedValues),
                null,
                $this->options,
            );
        } else {
            foreach ($allQEs as $item) {
                $preparedDataSet = $queryExpressionProvider->apply($preparedDataSet, $item, null, $this->options);
            }
        }

        $queryParts = null;

        if (count($this->queryModifiers) > 0) {
            $queryParams = new ParametersCollection();
            $queryParts  = new QueryParts();
            foreach ($this->queryModifiers as $modifier) {
                call_user_func($modifier, $queryParts, $queryParams);
            }

            foreach ($queryParams->getParameters() as $param) {
                $preparedDataSet->setParameter($param->getName(), $param->getValue(), $param->getType());
            }
        }

        if ($this->pagination !== null) {
            [$page, $itemsPerPage] = $this->pagination;
            $firstResult           = ($page - 1) * $itemsPerPage;
            $maxResults            = $itemsPerPage;
        } elseif ($this->limit !== null) {
            [$maxResults, $firstResult] = $this->limit;
        } else {
            $firstResult = null;
            $maxResults  = null;
        }

        $preparedDataSet->setFirstResult($firstResult);
        $preparedDataSet->setMaxResults($maxResults);

        if ($preparedDataSet instanceof QueryBuilder) {
            $queryParts?->addTo($preparedDataSet);
            $this->query = $preparedDataSet->getQuery();
        }

        if ($preparedDataSet instanceof AbstractRawQuery) {
            $queryParts?->addTo($preparedDataSet->sql());
            $this->query = $preparedDataSet;
        }

        if ($this->query instanceof ORMQuery) {
            $this->query->setHydrationMode($this->options['hydrator']);
        }

        if ($this->query instanceof RawNativeQuery) {
            $this->query->getNativeQuery()->setHydrationMode($this->options['hydrator']);
        }

        return $this->query;
    }

    /** @return AbstractRawQuery<T> */
    public function getRawQuery(): AbstractRawQuery
    {
        $query = $this->getQuery();
        if (! ($query instanceof AbstractRawQuery)) {
            throw new InvalidArgumentException(sprintf('Expected an instance of %s. Got: %s', AbstractRawQuery::class, $query::class));
        }

        return $query;
    }

    /** @return PaginatorInterface<T>|null */
    #[Override]
    public function paginator(): PaginatorInterface|null
    {
        $this->assertNoSpecifications();

        if ($this->cursor !== null) {
            // Cursor mode is mutually exclusive with offset/page pagination.
            return null;
        }

        if ($this->paginator) {
            return $this->paginator;
        }

        if ($this->pagination === null) {
            return null;
        }

        [$page, $itemsPerPage] = $this->pagination;
        if ($page === null || $itemsPerPage === null) {
            return null;
        }

        $query = $this->getQuery();

        $paginator = null;
        if ($query instanceof ORMQuery) {
            /** @phpstan-var Paginator<T> $doctrinePaginator */
            $doctrinePaginator = new Paginator($query, $this->options['paginator']['fetchJoinCollection'] ?? true);
            if (array_key_exists('useOutputWalkers', $this->options['paginator'] ?? [])) {
                $doctrinePaginator->setUseOutputWalkers($this->options['paginator']['useOutputWalkers']);
            }

            /** @phpstan-var DoctrinePaginator<T> $paginator */
            $paginator = new DoctrinePaginator($doctrinePaginator);
        }

        if ($query instanceof AbstractRawQuery) {
            /** @phpstan-var RawSqlPaginator<T> $paginator */
            $paginator = new RawSqlPaginator($query);
        }

        if ($paginator === null) {
            throw new RuntimeException('Can not create the data source query paginator.');
        }

        $this->paginator = $paginator;

        return $this->paginator;
    }

    /** @return Traversable<array-key, T> */
    #[Override]
    public function getIterator(): Traversable
    {
        $specifications = $this->specifications;
        $hasSpecs       = count($specifications) > 0;

        if ($hasSpecs && $this->limit === null) {
            throw new LogicException('Specifications can only be used with a limit. Call withLimit() before using withSpecification().');
        }

        if ($hasSpecs && $this->limit !== null) {
            [$limitValue, $offsetValue] = $this->limit;

            yield from $this->withoutSpecification()->withoutLimit()->specificationsIterator(
                $specifications,
                $limitValue,
                $offsetValue ?? 0,
            );

            return;
        }

        if ($this->cursor !== null) {
            $cursorPaginator = $this->cursorPaginator();
            if ($cursorPaginator !== null) {
                yield from $cursorPaginator->getIterator();

                return;
            }
        }

        $query    = $this->getQuery();
        $iterator = $this->paginator();
        if ($iterator === null) {
            $iterator = $query instanceof ORMQuery
                ? $query->toIterable([], $this->options['hydrator'])
                : $query->toIterable();
        }

        $itemNormalizer = $query instanceof AbstractQuery
            ? ($this->itemNormalizer ?? $this->options['item_normalizer'] ?? null)
            : null;

        $hasNormalizer = is_callable($itemNormalizer);

        if ($hasNormalizer) {
            foreach ($iterator as $item) {
                /** @phpstan-var callable $itemNormalizer */
                $item = $itemNormalizer($item);

                yield $item;
            }

            return;
        }

        yield from $iterator;
    }

    #[Override]
    public function count(): int
    {
        $this->assertNoSpecifications();

        if ($this->cursor !== null) {
            return $this->cursorPaginator()?->count() ?? 0;
        }

        if ($this->isPaginated()) {
            return $this->paginator()?->count() ?? 0;
        }

        return $this->totalCount();
    }

    #[Override]
    public function totalCount(): int
    {
        $this->assertNoSpecifications();

        if ($this->cursor !== null) {
            // Cursor mode does not pre-compute total counts by default. Surface 0 when
            // unknown so isEmpty() and other consumers behave sanely.
            return $this->cursorPaginator()?->getTotalItems() ?? 0;
        }

        $query = $this->getQuery();
        if ($query instanceof AbstractRawQuery) {
            return $query->getCount();
        }

        if ($this->isPaginated()) {
            return $this->paginator()?->getTotalItems() ?? 0;
        }

        // Fetching total count from ORM Query is easier via pagination
        return $this->withPagination(1, 1)->paginator()?->getTotalItems() ?? 0;
    }

    #[Override]
    public function isPaginated(): bool
    {
        if (count($this->specifications) > 0) {
            return false;
        }

        if ($this->pagination === null) {
            return false;
        }

        [$page, $itemsPerPage] = $this->pagination;

        return $page > 0 && $itemsPerPage > 0;
    }

    #[Override]
    public function isCursored(): bool
    {
        return $this->cursor !== null;
    }

    /** @return CursorPaginatorInterface<T>|null */
    #[Override]
    public function cursorPaginator(): CursorPaginatorInterface|null
    {
        $this->assertNoSpecifications();

        if ($this->cursorPaginator !== null) {
            return $this->cursorPaginator;
        }

        if ($this->cursor === null) {
            return null;
        }

        [$token, $limit] = $this->cursor;
        $cursor          = $token !== null ? $this->cursorCodec->decode($token) : null;

        $effectiveSort = $this->buildCursorEffectiveSort();
        $signature     = Cursor::signatureFor($effectiveSort);
        if ($cursor !== null && $cursor->getSortSignature() !== $signature) {
            throw new InvalidCursorException('Cursor was issued under a different sort order.');
        }

        $direction   = $cursor?->getDirection() ?? Direction::FORWARD;
        $orderBySort = $direction === Direction::BACKWARD ? $effectiveSort->invert() : $effectiveSort;

        $preparedDataSet = $this->prepareCursorDataSet();
        $this->replaceOrderBy($preparedDataSet, $orderBySort);

        if ($cursor !== null) {
            $this->applyKeysetPredicate($preparedDataSet, $cursor, $orderBySort);
        }

        $preparedDataSet->setFirstResult(null);
        $preparedDataSet->setMaxResults($limit + 1);

        if ($preparedDataSet instanceof QueryBuilder) {
            $executable = $preparedDataSet->getQuery();
            $executable->setHydrationMode($this->options['hydrator']);
        } else {
            /** @phpstan-var AbstractRawQuery<T> $executable */
            $executable = $preparedDataSet;
        }

        $rows           = [];
        $itemNormalizer = $this->itemNormalizer ?? $this->options['item_normalizer'] ?? null;
        $hasNormalizer  = is_callable($itemNormalizer);
        $iter           = $executable instanceof ORMQuery
            ? $executable->toIterable([], $this->options['hydrator'])
            : $executable->toIterable();
        foreach ($iter as $row) {
            $rows[] = $hasNormalizer ? $itemNormalizer($row) : $row;
        }

        /** @phpstan-var list<T> $rows */
        /** @phpstan-var DoctrineCursorPaginator<T> $paginator */
        $paginator = new DoctrineCursorPaginator(
            $rows,
            $effectiveSort,
            $direction,
            $limit,
            $this->cursorCodec,
            $cursor !== null,
        );

        $this->cursorPaginator = $paginator;

        return $paginator;
    }

    /**
     * Compose the SortExpression that anchors cursor pagination, mirroring the
     * in-memory adapter's approach: the last non-empty sort from the active query
     * expressions wins; a root-identifier ASC tiebreaker is appended if absent.
     */
    private function buildCursorEffectiveSort(): SortExpression
    {
        $sort = SortExpression::create();
        foreach ($this->queryExpressions as $qe) {
            $qeSort = $qe->getSort();
            if ($qeSort === null || $qeSort->isSortEmpty()) {
                continue;
            }

            $sort = $qeSort;
        }

        $queryExpressionProvider = $this->getOrCreateQueryExpressionProvider();
        $rootIdentifier          = $queryExpressionProvider->requireSingleRootIdentifier();

        if ($sort->dir($rootIdentifier) === null) {
            $sort = $sort->asc($rootIdentifier);
        }

        return $sort;
    }

    /**
     * Build a fresh prepared data set for cursor mode — applies the active query
     * expressions and modifiers but skips the offset/page pagination wiring, which is
     * irrelevant in cursor mode.
     *
     * @phpstan-return QueryBuilder|AbstractRawQuery<T>
     */
    private function prepareCursorDataSet(): QueryBuilder|AbstractRawQuery
    {
        $preparedDataSet = clone $this->dataSet;

        $specQEs = [];
        foreach ($this->specifications as $specification) {
            $qe = $specification->getQueryExpression();
            if ($qe === null || $qe->isEmpty()) {
                continue;
            }

            $specQEs[] = $qe;
        }

        $queryExpressionProvider = $this->getOrCreateQueryExpressionProvider();
        $allQEs                  = [...$specQEs, ...$this->queryExpressions];
        $mergedValues            = $this->collectInputValues();
        if (count($mergedValues) > 0) {
            foreach ($allQEs as $queryExpression) {
                $preparedDataSet = $queryExpressionProvider->apply(
                    $preparedDataSet,
                    $queryExpression,
                    null,
                    $this->options,
                    QueryExpressionProviderInterface::INCLUDE_DATA_FILTER | QueryExpressionProviderInterface::INCLUDE_DATA_SORT,
                );
            }

            $preparedDataSet = $queryExpressionProvider->apply(
                $preparedDataSet,
                QueryExpression::create()->withValues($mergedValues),
                null,
                $this->options,
            );
        } else {
            foreach ($allQEs as $item) {
                $preparedDataSet = $queryExpressionProvider->apply($preparedDataSet, $item, null, $this->options);
            }
        }

        $queryParts = null;
        if (count($this->queryModifiers) > 0) {
            $queryParams = new ParametersCollection();
            $queryParts  = new QueryParts();
            foreach ($this->queryModifiers as $modifier) {
                call_user_func($modifier, $queryParts, $queryParams);
            }

            foreach ($queryParams->getParameters() as $param) {
                $preparedDataSet->setParameter($param->getName(), $param->getValue(), $param->getType());
            }
        }

        if ($preparedDataSet instanceof QueryBuilder) {
            $queryParts?->addTo($preparedDataSet);
        } elseif ($preparedDataSet instanceof AbstractRawQuery) {
            $queryParts?->addTo($preparedDataSet->sql());
        }

        return $preparedDataSet;
    }

    /** @phpstan-param QueryBuilder|AbstractRawQuery<T> $preparedDataSet */
    private function replaceOrderBy(QueryBuilder|AbstractRawQuery $preparedDataSet, SortExpression $sort): void
    {
        if ($preparedDataSet instanceof QueryBuilder) {
            $first = true;
            foreach ($sort->items() as $item) {
                $field = $this->mapCursorField($item['field'], $preparedDataSet);
                if ($first) {
                    $preparedDataSet->orderBy($field, $item['dir']);
                    $first = false;
                } else {
                    $preparedDataSet->addOrderBy($field, $item['dir']);
                }
            }

            return;
        }

        $sqlParts = $preparedDataSet->sql();
        $sqlParts->resetQueryPart('orderBy');
        foreach ($sort->items() as $item) {
            $sqlParts->addOrderBy($this->mapCursorField($item['field'], $preparedDataSet), $item['dir']);
        }
    }

    /** @phpstan-param QueryBuilder|AbstractRawQuery<T> $preparedDataSet */
    private function applyKeysetPredicate(QueryBuilder|AbstractRawQuery $preparedDataSet, Cursor $cursor, SortExpression $orderBySort): void
    {
        $position  = $cursor->getPosition();
        $sortItems = $orderBySort->items();
        $expr      = new Expr();
        $orX       = $expr->orX();

        // Expanded keyset form — more portable than (a,b,c) > (?,?,?). The expansion is:
        //   (a >cmp ?)
        //   OR (a = ? AND b >cmp ?)
        //   OR (a = ? AND b = ? AND c >cmp ?)
        // where >cmp flips to < when the corresponding sort direction is DESC.
        $rounds = count($sortItems);
        for ($i = 0; $i < $rounds; $i++) {
            $andX = $expr->andX();
            for ($j = 0; $j < $i; $j++) {
                if (! isset($position[$j]) || $position[$j]['field'] !== $sortItems[$j]['field']) {
                    throw new InvalidCursorException('Cursor position does not match the effective sort.');
                }

                $col       = $this->mapCursorField($sortItems[$j]['field'], $preparedDataSet);
                $paramName = 'cursorEq' . $j;
                $andX->add($expr->eq($col, ':' . $paramName));
                $preparedDataSet->setParameter($paramName, $position[$j]['value']);
            }

            if (! isset($position[$i]) || $position[$i]['field'] !== $sortItems[$i]['field']) {
                throw new InvalidCursorException('Cursor position does not match the effective sort.');
            }

            $col       = $this->mapCursorField($sortItems[$i]['field'], $preparedDataSet);
            $paramName = 'cursorCmp' . $i;
            $preparedDataSet->setParameter($paramName, $position[$i]['value']);

            $cmpExpr = $sortItems[$i]['dir'] === SortExpression::DIR_DESC
                ? $expr->lt($col, ':' . $paramName)
                : $expr->gt($col, ':' . $paramName);
            $andX->add($cmpExpr);

            $orX->add($andX);
        }

        if ($preparedDataSet instanceof QueryBuilder) {
            $preparedDataSet->andWhere($orX);

            return;
        }

        $preparedDataSet->sql()->andWhere($orX);
    }

    /** @phpstan-param QueryBuilder|AbstractRawQuery<T> $data */
    private function mapCursorField(string $field, QueryBuilder|AbstractRawQuery $data): string
    {
        $queryExpressionProvider = $this->getOrCreateQueryExpressionProvider();

        return $queryExpressionProvider->mapField($field, $data, null, $this->options);
    }

    /**
     * @phpstan-param array<string, string> $fieldsOperator
     * @phpstan-param array<string, bool> $fieldsIgnoreCase
     */
    #[Override]
    public function handleRequest(object $request, array $fieldsOperator = [], array $fieldsIgnoreCase = []): static
    {
        /** @phpstan-var static<T> $ds */
        $ds = static::applyRequestTo($this, $request, $fieldsOperator, $fieldsIgnoreCase);

        return $ds;
    }

    /**
     * @phpstan-param J $target
     * @phpstan-param array<string, string> $fieldsOperator
     * @phpstan-param array<string, bool> $fieldsIgnoreCase
     *
     * @phpstan-return J
     *
     * @phpstan-template J of ReadDataProviderCompositionInterface<object|array<string, mixed>>
     */
    public static function applyRequestTo(ReadDataProviderCompositionInterface $target, object $request, array $fieldsOperator = [], array $fieldsIgnoreCase = []): ReadDataProviderCompositionInterface
    {
        if (class_exists(SymfonyRequest::class) && $request instanceof SymfonyRequest) {
            if (! class_exists(Psr17Factory::class)) {
                throw new InvalidArgumentException('You need to install "nyholm/psr7" and "symfony/psr-http-message-bridge" in order to handle Symfony requests!');
            }

            $psr17Factory   = new Psr17Factory();
            $psrHttpFactory = new PsrHttpFactory($psr17Factory, $psr17Factory, $psr17Factory, $psr17Factory);
            $request        = $psrHttpFactory->createRequest($request);
        }

        if ($request instanceof RequestInterface) {
            parse_str($request->getUri()->getQuery(), $input);

            /**
             * @phpstan-var J $result
             * @phpstan-ignore argument.type
             */
            $result = $target->handleInput($input, $fieldsOperator, $fieldsIgnoreCase);

            return $result;
        }

        throw new RuntimeException(sprintf('Unsupported request type: %s', $request::class));
    }

    public function __clone()
    {
        $this->query           = null;
        $this->paginator       = null;
        $this->cursorPaginator = null;
    }
}
