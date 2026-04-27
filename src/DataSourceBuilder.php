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
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModel\Query\SortExpression;
use Kraz\ReadModelDoctrine\Query\QueryExpressionProvider;
use Kraz\ReadModelDoctrine\Query\RawNativeQuery;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;
use Webmozart\Assert\Assert;

/**
 * @psalm-import-type DataSourceOptionsWrapper from DataSource
 *
 * @template T of object
 */
class DataSourceBuilder
{
    private string|RawQuery|RawNativeQuery|RawQueryBuilder|NativeQuery|QueryBuilder|null $data = null;
    private ?QueryExpression $queryExpression = null;
    private ?QueryRequest $queryRequest = null;
    private ?string $rootAlias = null;
    private ?string $rootIdentifier = null;
    private mixed $itemNormalizer = null;
    private ?ReadModelDescriptor $readModelDescriptor = null;
    private array $queryModifier = [];
    private mixed $denormalizer = null;
    /**
     * @var array<string, string>|null
     */
    private ?array $queryExpressionFieldMapping = null;

    public function __construct(
        private ?ReadModelDescriptorFactoryInterface $descriptorFactory = null,
        private ?QueryExpressionProviderInterface $queryExpressionProvider = null,
    ) {
        $this->descriptorFactory ??= new ReadModelDescriptorFactory();
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

    /**
     * @return DataSourceBuilder<T>
     */
    public function andWhere(FilterExpression ...$expr): static
    {
        $this->queryExpression ??= QueryExpression::create();
        $this->queryExpression = $this->queryExpression->andWhere(...$expr);

        return $this;
    }

    /**
     * @return DataSourceBuilder<T>
     */
    public function orWhere(FilterExpression ...$expr): static
    {
        $this->queryExpression ??= QueryExpression::create();
        $this->queryExpression = $this->queryExpression->orWhere(...$expr);

        return $this;
    }

    /**
     * @return DataSourceBuilder<T>
     */
    public function sortBy(string $field, string $dir = SortExpression::DIR_ASC): static
    {
        $this->queryExpression ??= QueryExpression::create();
        $this->queryExpression = $this->queryExpression->sortBy($field, $dir);

        return $this;
    }

    /**
     * @return DataSourceBuilder<T>
     */
    public function withData(string|RawQuery|RawNativeQuery|RawQueryBuilder|NativeQuery|QueryBuilder $data): static
    {
        $clone = clone $this;
        $clone->data = $data;

        return $clone;
    }

    /**
     * @return DataSourceBuilder<T>
     */
    public function withQueryExpression(QueryExpression $queryExpression): static
    {
        $clone = clone $this;
        $clone->queryExpression = $queryExpression;

        return $clone;
    }

    /**
     * @param array<string, string> $queryExpressionFieldMapping
     *
     * @return DataSourceBuilder<T>
     */
    public function withQueryExpressionFieldMapping(array $queryExpressionFieldMapping): static
    {
        $clone = clone $this;
        $clone->queryExpressionFieldMapping = $queryExpressionFieldMapping;

        return $clone;
    }

    /**
     * @return DataSourceBuilder<T>
     */
    public function withQueryModifier(callable $modifier): static
    {
        $clone = clone $this;
        $clone->queryModifier[] = $modifier;

        return $clone;
    }

    public function withDenormalizer(callable $callback): static
    {
        $clone = clone $this;
        $clone->denormalizer = $callback;

        return $clone;
    }

    /**
     * @return DataSourceBuilder<T>
     */
    public function withQueryRequest(QueryRequest $queryRequest): static
    {
        $clone = clone $this;
        $clone->queryRequest = $queryRequest;

        return $clone;
    }

    public function withRootAlias(string $rootAlias): static
    {
        $clone = clone $this;
        $clone->rootAlias = $rootAlias;

        return $clone;
    }

    public function withRootIdentifier(string $rootIdentifier): static
    {
        $clone = clone $this;
        $clone->rootIdentifier = $rootIdentifier;

        return $clone;
    }

    public function withItemNormalizer(callable $itemNormalizer): static
    {
        $clone = clone $this;
        $clone->itemNormalizer = $itemNormalizer;

        return $clone;
    }

    public function withReadModel(object|string $model): static
    {
        $clone = clone $this;
        $clone->readModelDescriptor = $this->descriptorFactory->createReadModelDescriptorFrom($model);

        return $clone;
    }

    public function withReadModelDescriptor(ReadModelDescriptor $readModelDescriptor): static
    {
        $clone = clone $this;
        $clone->readModelDescriptor = $readModelDescriptor;

        return $clone;
    }

    /**
     * @psalm-param DataSourceOptionsWrapper $options
     *
     * @return DataSource<T>
     */
    public function create(Connection $connection, array $options = []): DataSource
    {
        Assert::notNull($this->data);
        $options['connection'] = $connection;
        if (null !== $this->rootAlias) {
            $options['root_alias'] = $options['root_alias'] ?? $this->rootAlias;
        }
        if (null !== $this->rootIdentifier) {
            $options['root_identifier'] = $options['root_identifier'] ?? $this->rootIdentifier;
        }
        if (null !== $this->denormalizer) {
            $options['denormalizer'] = $options['denormalizer'] ?? $this->denormalizer;
        }
        if (null !== $this->itemNormalizer) {
            $options['item_normalizer'] = $options['item_normalizer'] ?? $this->itemNormalizer;
        }
        if (null !== $this->readModelDescriptor) {
            $options['read_model_descriptor'] = $options['read_model_descriptor'] ?? $this->readModelDescriptor;
        }
        if (null !== $this->queryExpressionFieldMapping) {
            $options['field_map'] = $this->queryExpressionFieldMapping;
        }
        $dataSource = new DataSource($this->data, $this->queryExpressionProvider, $options);

        if (null !== $this->queryRequest) {
            $dataSource = $dataSource->withQueryRequest($this->queryRequest);
        }

        if (null !== $this->queryExpression) {
            $dataSource = $dataSource->withQueryExpression($this->queryExpression);
        }

        foreach ($this->queryModifier as $modifier) {
            if (\is_callable($modifier)) {
                $dataSource = $dataSource->withQueryModifier($modifier);
            }
        }

        return $dataSource;
    }
}
