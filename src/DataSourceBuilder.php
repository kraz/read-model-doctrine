<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\NativeQuery;
use Doctrine\ORM\QueryBuilder;
use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\Query\QueryRequest;
use Kraz\ReadModel\Query\SortExpression;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModelDoctrine\Query\QueryExpressionProvider;
use Kraz\ReadModelDoctrine\Query\RawNativeQuery;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;
use Webmozart\Assert\Assert;

use function is_callable;

/**
 * @phpstan-import-type DataSourceOptionsWrapper from DataSource
 * @template T of object
 */
class DataSourceBuilder
{
    private string|RawQuery|RawNativeQuery|RawQueryBuilder|NativeQuery|QueryBuilder|null $data = null;
    private QueryExpression|null $queryExpression                                              = null;
    private QueryRequest|null $queryRequest                                                    = null;
    private string|null $rootAlias                                                             = null;
    private string|null $rootIdentifier                                                        = null;
    private mixed $itemNormalizer                                                              = null;
    private ReadModelDescriptor|null $readModelDescriptor                                      = null;
    /** @phpstan-var list<callable> */
    private array $queryModifier = [];
    private mixed $denormalizer  = null;
    /** @var array<string, string>|null */
    private array|null $queryExpressionFieldMapping = null;

    public function __construct(
        private ReadModelDescriptorFactoryInterface|null $descriptorFactory = null,
        private QueryExpressionProviderInterface|null $queryExpressionProvider = null,
    ) {
        $this->descriptorFactory       ??= new ReadModelDescriptorFactory();
        $this->queryExpressionProvider ??= new QueryExpressionProvider($this->descriptorFactory);
    }

    public function qry(): QueryExpression
    {
        return QueryExpression::create();
    }

    public function expr(): FilterExpression
    {
        return FilterExpression::create();
    }

    /** @return DataSourceBuilder<T> */
    public function andWhere(FilterExpression ...$expr): static
    {
        $this->queryExpression ??= QueryExpression::create();
        $this->queryExpression   = $this->queryExpression->andWhere(...$expr);

        return $this;
    }

    /** @return DataSourceBuilder<T> */
    public function orWhere(FilterExpression ...$expr): static
    {
        $this->queryExpression ??= QueryExpression::create();
        $this->queryExpression   = $this->queryExpression->orWhere(...$expr);

        return $this;
    }

    /** @return DataSourceBuilder<T> */
    public function sortBy(string $field, string $dir = SortExpression::DIR_ASC): static
    {
        $this->queryExpression ??= QueryExpression::create();
        $this->queryExpression   = $this->queryExpression->sortBy($field, $dir);

        return $this;
    }

    /** @return DataSourceBuilder<T> */
    public function withData(string|RawQuery|RawNativeQuery|RawQueryBuilder|NativeQuery|QueryBuilder $data): static
    {
        $clone       = clone $this;
        $clone->data = $data;

        return $clone;
    }

    /** @return DataSourceBuilder<T> */
    public function withQueryExpression(QueryExpression $queryExpression): static
    {
        $clone                  = clone $this;
        $clone->queryExpression = $queryExpression;

        return $clone;
    }

    /**
     * @phpstan-param array<string, string> $queryExpressionFieldMapping
     *
     * @return DataSourceBuilder<T>
     */
    public function withQueryExpressionFieldMapping(array $queryExpressionFieldMapping): static
    {
        $clone                              = clone $this;
        $clone->queryExpressionFieldMapping = $queryExpressionFieldMapping;

        return $clone;
    }

    /** @return DataSourceBuilder<T> */
    public function withQueryModifier(callable $modifier): static
    {
        $clone                  = clone $this;
        $clone->queryModifier[] = $modifier;

        return $clone;
    }

    public function withDenormalizer(callable $callback): static
    {
        $clone               = clone $this;
        $clone->denormalizer = $callback;

        return $clone;
    }

    /** @return DataSourceBuilder<T> */
    public function withQueryRequest(QueryRequest $queryRequest): static
    {
        $clone               = clone $this;
        $clone->queryRequest = $queryRequest;

        return $clone;
    }

    public function withRootAlias(string $rootAlias): static
    {
        $clone            = clone $this;
        $clone->rootAlias = $rootAlias;

        return $clone;
    }

    public function withRootIdentifier(string $rootIdentifier): static
    {
        $clone                 = clone $this;
        $clone->rootIdentifier = $rootIdentifier;

        return $clone;
    }

    public function withItemNormalizer(callable $itemNormalizer): static
    {
        $clone                 = clone $this;
        $clone->itemNormalizer = $itemNormalizer;

        return $clone;
    }

    public function withReadModel(object|string $model): static
    {
        $clone                      = clone $this;
        $clone->readModelDescriptor = $this->descriptorFactory->createReadModelDescriptorFrom($model);

        return $clone;
    }

    public function withReadModelDescriptor(ReadModelDescriptor $readModelDescriptor): static
    {
        $clone                      = clone $this;
        $clone->readModelDescriptor = $readModelDescriptor;

        return $clone;
    }

    /**
     * @phpstan-param DataSourceOptionsWrapper $options
     *
     * @return DataSource<T>
     */
    public function create(Connection $connection, array $options = []): DataSource
    {
        Assert::notNull($this->data);
        $options['connection'] = $connection;
        if ($this->rootAlias !== null) {
            $options['root_alias'] ??= $this->rootAlias;
        }

        if ($this->rootIdentifier !== null) {
            $options['root_identifier'] ??= $this->rootIdentifier;
        }

        if ($this->denormalizer !== null) {
            $options['denormalizer'] ??= $this->denormalizer;
        }

        if ($this->itemNormalizer !== null) {
            $options['item_normalizer'] ??= $this->itemNormalizer;
        }

        if ($this->readModelDescriptor !== null) {
            $options['read_model_descriptor'] ??= $this->readModelDescriptor;
        }

        if ($this->queryExpressionFieldMapping !== null) {
            $options['field_map'] = $this->queryExpressionFieldMapping;
        }

        $dataSource = new DataSource($this->data, $this->queryExpressionProvider, $options);

        if ($this->queryRequest !== null) {
            $dataSource = $dataSource->withQueryRequest($this->queryRequest);
        }

        if ($this->queryExpression !== null) {
            $dataSource = $dataSource->withQueryExpression($this->queryExpression);
        }

        foreach ($this->queryModifier as $modifier) {
            if (! is_callable($modifier)) {
                continue;
            }

            $dataSource = $dataSource->withQueryModifier($modifier);
        }

        return $dataSource;
    }
}
