<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\AbstractQuery;
use Doctrine\ORM\NativeQuery;
use Doctrine\ORM\Query as ORMQuery;
use Doctrine\ORM\QueryBuilder;
use Doctrine\ORM\Tools\Pagination\Paginator;
use Kraz\ReadModel\BasicReadDataProvider;
use Kraz\ReadModel\Pagination\PaginatorInterface;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\Query\QueryRequest;
use Kraz\ReadModel\ReadDataProviderInterface;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadResponse;
use Kraz\ReadModelDoctrine\Pagination\DoctrinePaginator;
use Kraz\ReadModelDoctrine\Pagination\RawSqlPaginator;
use Kraz\ReadModelDoctrine\Query\AbstractRawQuery;
use Kraz\ReadModelDoctrine\Query\QueryExpressionProvider;
use Kraz\ReadModelDoctrine\Query\RawNativeQuery;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;
use Kraz\ReadModelDoctrine\Tools\ParametersCollection;
use Kraz\ReadModelDoctrine\Tools\QueryParts;
use Nyholm\Psr7\Factory\Psr17Factory;
use Override;
use Psr\Http\Message\RequestInterface;
use RuntimeException;
use Symfony\Bridge\PsrHttpMessage\Factory\PsrHttpFactory;
use Symfony\Component\HttpFoundation\Request as SymfonyRequest;
use Traversable;
use Webmozart\Assert\Assert;

use function array_key_exists;
use function array_replace_recursive;
use function call_user_func;
use function class_exists;
use function count;
use function get_class;
use function gettype;
use function implode;
use function is_callable;
use function is_object;
use function is_string;
use function iterator_to_array;
use function parse_str;
use function sprintf;

/**
 * @phpstan-type DataSourceOptions = array{
 *     connection?: Connection|null,
 *     hydrator?: 1|2|3|4|5|6|string,
 *     root_identifier?: string,
 *     root_alias?: string|string[],
 *     quoteTableAlias?: bool,
 *     quoteFieldNames?: bool,
 *     quoteFieldNamesChar?: string,
 *     read_model_descriptor?: ReadModelDescriptor|string|null,
 *     item_normalizer?: callable|null,
 *     query?: array{
 *         item_normalizer?: callable|null,
 *     },
 *     paginator?: array{
 *         fetchJoinCollection?: bool,
 *         useOutputWalkers?: bool,
 *     },
 *     normalizer?: callable|null,
 *     denormalizer?: callable|null,
 *     field_map?: array<string, string>|null
 * }
 * @phpstan-type DataSourceOptionsWrapper = DataSourceOptions|array<never, never>
 * @template T of object|array<string, mixed>
 */
class DataSource implements ReadDataProviderInterface
{
    use BasicReadDataProvider;

    public const int DEFAULT_HYDRATOR = AbstractQuery::HYDRATE_ARRAY;

    /** @phpstan-var DataSourceOptions */
    private array $options;
    private QueryBuilder|AbstractRawQuery $dataSet;

    private int|null $page         = null;
    private int|null $itemsPerPage = null;
    /** @var QueryExpression[] */
    private array $queryExpression = [];

    private ORMQuery|AbstractRawQuery|null $query = null;
    private PaginatorInterface|null $paginator    = null;
    private QueryExpressionProviderInterface $queryExpressionProvider;
    /** @var callable[] */
    private array $queryPartsModifier = [];

    /** @phpstan-param DataSourceOptionsWrapper $options */
    public function __construct(string|RawQuery|RawNativeQuery|RawQueryBuilder|NativeQuery|QueryBuilder $data, QueryExpressionProviderInterface|null $queryExpressionProvider = null, array $options = [])
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

        if (is_string($data)) {
            if (($this->options['connection'] ?? null) === null) {
                throw new RuntimeException('You must specify a doctrine "connection" into the DataSource options when using the DataSource with plain (RAW) SQL!');
            }

            if (! $this->options['connection'] instanceof Connection) {
                throw new RuntimeException(sprintf('Invalid DataSource connection. Expected instance of "%s", but got "%s"', Connection::class, is_object($this->options['connection']) ? get_class($this->options['connection']) : gettype($this->options['connection'])));
            }

            $query = new RawQuery($this->options['connection'], $this->options);
            $query->setSql($data);
            $data = $query;
        }

        if ($data instanceof NativeQuery) {
            $data = new RawNativeQuery($data, $this->options);
        }

        if ($data instanceof RawQueryBuilder) {
            $data = $data->getQuery();
        }

        if (! $data instanceof QueryBuilder && ! $data instanceof AbstractRawQuery) {
            throw new RuntimeException(sprintf('Invalid DataSource data. Expected string or instance of one of the following classes: %s, but got "%s"', '"' . implode('", "', [RawQuery::class, RawNativeQuery::class, RawQueryBuilder::class, NativeQuery::class, QueryBuilder::class]) . '"', is_object($data) ? $data::class : gettype($data)));
        }

        if ($data instanceof AbstractRawQuery) {
            $data = clone $data;
            $data->setOptions($this->options);
        }

        if ($queryExpressionProvider === null) {
            $queryExpressionProvider = new QueryExpressionProvider(new ReadModelDescriptorFactory());
        }

        $this->dataSet                 = $data;
        $this->queryExpressionProvider = $queryExpressionProvider;
    }

    /** @return AbstractRawQuery<T>|ORMQuery */
    public function getQuery(): AbstractRawQuery|ORMQuery
    {
        if ($this->query !== null) {
            return $this->query;
        }

        $preparedDataSet = clone $this->dataSet;

        foreach ($this->queryExpression as $item) {
            $preparedDataSet = $this->queryExpressionProvider
                ->apply($preparedDataSet, $item, null, $this->options);
        }

        $queryParts = null;

        if (count($this->queryPartsModifier) > 0) {
            $queryParams = new ParametersCollection();
            $queryParts  = new QueryParts();
            foreach ($this->queryPartsModifier as $modifier) {
                call_user_func($modifier, $queryParts, $queryParams);
            }

            foreach ($queryParams->getParameters() as $param) {
                $preparedDataSet->setParameter($param->getName(), $param->getValue(), $param->getType());
            }
        }

        if ($this->page !== null && $this->itemsPerPage !== null) {
            $firstResult = ($this->page - 1) * $this->itemsPerPage;
            $maxResults  = $this->itemsPerPage;
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
        Assert::isInstanceOf($query, AbstractRawQuery::class);

        return $query;
    }

    /** @return PaginatorInterface<T>|null */
    #[Override]
    public function paginator(): PaginatorInterface|null
    {
        if ($this->paginator) {
            return $this->paginator;
        }

        if ($this->page === null || $this->itemsPerPage === null) {
            return null;
        }

        $query = $this->getQuery();

        $paginator = null;
        if ($query instanceof ORMQuery) {
            $paginator = new Paginator($query, $this->options['paginator']['fetchJoinCollection'] ?? true);
            if (array_key_exists('useOutputWalkers', $this->options['paginator'] ?? [])) {
                $paginator->setUseOutputWalkers($this->options['paginator']['useOutputWalkers']);
            }

            $paginator = new DoctrinePaginator($paginator);
        }

        if ($query instanceof AbstractRawQuery) {
            $paginator = new RawSqlPaginator($query);
        }

        if (! $paginator instanceof PaginatorInterface) {
            throw new RuntimeException(sprintf('Invalid paginator. Expected instance of "%s", but got "%s"', PaginatorInterface::class, is_object($paginator) ? $paginator::class : gettype($paginator)));
        }

        $this->paginator = $paginator;

        return $this->paginator;
    }

    /** @return Traversable<array-key, T> */
    #[Override]
    public function getIterator(): Traversable
    {
        $query    = $this->getQuery();
        $iterator = $this->paginator();
        if ($iterator === null) {
            $iterator = $query->toIterable();
        }

        if ($query instanceof AbstractQuery) {
            $itemNormalizer = $this->options['item_normalizer'] ?? null;
            if (is_callable($itemNormalizer)) {
                foreach ($iterator as $item) {
                    $item = $itemNormalizer($item);

                    yield $item;
                }

                return;
            }
        }

        yield from $iterator;
    }

    #[Override]
    public function data(): array
    {
        return iterator_to_array($this->getIterator());
    }

    #[Override]
    public function getResult(): array|ReadResponse
    {
        $data = $this->data();

        if ($this->isValue()) {
            return $data;
        }

        $page  = $this->isPaginated() ? ($this->paginator()?->getCurrentPage() ?? 1) : 1;
        $total = $this->totalCount();

        return ReadResponse::create($data, $page, $total);
    }

    #[Override]
    public function count(): int
    {
        if ($this->isPaginated()) {
            return $this->paginator()->count();
        }

        return $this->totalCount();
    }

    #[Override]
    public function totalCount(): int
    {
        $query = $this->getQuery();
        if ($query instanceof AbstractRawQuery) {
            return $query->getCount();
        }

        if ($this->isPaginated()) {
            return $this->paginator()->getTotalItems();
        }

        // Fetching total count from ORM Query is easier via pagination
        return $this->withPagination(1, 1)->paginator()?->getTotalItems() ?? 0;
    }

    #[Override]
    public function isEmpty(): bool
    {
        return $this->getQuery()->getCount() === 0;
    }

    #[Override]
    public function isPaginated(): bool
    {
        return $this->page > 0 && $this->itemsPerPage > 0;
    }

    #[Override]
    public function queryExpressions(): array
    {
        return $this->queryExpression;
    }

    /** @return DataSource<T> */
    #[Override]
    public function withQueryExpression(QueryExpression $queryExpression): static
    {
        $cloned                    = clone $this;
        $cloned->queryExpression[] = $queryExpression;

        return $cloned;
    }

    /** @return DataSource<T> */
    #[Override]
    public function withoutQueryExpression(): static
    {
        $cloned                  = clone $this;
        $cloned->queryExpression = [];

        return $cloned;
    }

    /** @return DataSource<T> */
    #[Override]
    public function withPagination(int $page, int $itemsPerPage): static
    {
        if ($itemsPerPage <= 0) {
            return $this->withoutPagination();
        }

        Assert::positiveInteger($page);
        Assert::positiveInteger($itemsPerPage);

        $cloned               = clone $this;
        $cloned->page         = $page;
        $cloned->itemsPerPage = $itemsPerPage;

        return $cloned;
    }

    /** @return DataSource<T> */
    #[Override]
    public function withoutPagination(): static
    {
        $cloned               = clone $this;
        $cloned->page         = null;
        $cloned->itemsPerPage = null;

        return $cloned;
    }

    #[Override]
    public function withQueryRequest(QueryRequest $queryRequest): static
    {
        $clone = clone $this;
        if ($queryRequest->getQuery() !== null) {
            $clone = $clone->withQueryExpression($queryRequest->getQuery());
        }

        if ($queryRequest->getPage() !== null && $queryRequest->getItemsPerPage() !== null) {
            $clone = $clone->withPagination($queryRequest->getPage(), $queryRequest->getItemsPerPage());
        }

        return $clone;
    }

    #[Override]
    public function withQueryModifier(callable $modifier): static
    {
        $cloned                       = clone $this;
        $cloned->queryPartsModifier[] = $modifier;

        return $cloned;
    }

    #[Override]
    public function withoutQueryModifier(): static
    {
        $cloned                     = clone $this;
        $cloned->queryPartsModifier = [];

        return $cloned;
    }

    /**
     * @phpstan-param array<string, string> $fieldsOperator
     * @phpstan-param array<string, bool> $fieldsIgnoreCase
     *
     * @return $this
     */
    #[Override]
    public function handleRequest(object $request, array $fieldsOperator = [], array $fieldsIgnoreCase = []): static
    {
        if (class_exists(SymfonyRequest::class) && $request instanceof SymfonyRequest) {
            Assert::classExists(Psr17Factory::class, 'You need to install "nyholm/psr7" and "symfony/psr-http-message-bridge" in order to handle Symfony requests!');
            $psr17Factory   = new Psr17Factory();
            $psrHttpFactory = new PsrHttpFactory($psr17Factory, $psr17Factory, $psr17Factory, $psr17Factory);
            $request        = $psrHttpFactory->createRequest($request);
        }

        if ($request instanceof RequestInterface) {
            parse_str($request->getUri()->getQuery(), $input);

            return $this->handleInput($input, $fieldsOperator, $fieldsIgnoreCase);
        }

        throw new RuntimeException(sprintf('Unsupported request type: %s', $request::class));
    }

    public function __clone()
    {
        $this->query     = null;
        $this->paginator = null;
    }
}
