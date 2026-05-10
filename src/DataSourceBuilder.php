<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\NativeQuery;
use Doctrine\ORM\QueryBuilder;
use InvalidArgumentException;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\ReadDataProviderBuilder;
use Kraz\ReadModel\ReadDataProviderBuilderInterface;
use Kraz\ReadModel\ReadDataProviderCompositionInterface;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModelDoctrine\Query\QueryExpressionProvider;
use Kraz\ReadModelDoctrine\Query\RawNativeQuery;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;
use LogicException;
use Override;

/**
 * @phpstan-import-type DataSourceOptionsWrapper from DataSource
 * @phpstan-template-covariant T of object|array<string, mixed> = array<string, mixed>
 * @implements ReadDataProviderCompositionInterface<T>
 * @implements ReadDataProviderBuilderInterface<T>
 */
class DataSourceBuilder implements ReadDataProviderCompositionInterface, ReadDataProviderBuilderInterface
{
    /** @use ReadDataProviderBuilder<T> */
    use ReadDataProviderBuilder;

    private mixed $data         = null;
    private mixed $denormalizer = null;

    protected function createDefaultDescriptorFactory(): ReadModelDescriptorFactoryInterface
    {
        return new ReadModelDescriptorFactory();
    }

    protected function createDefaultQueryExpressionProvider(ReadModelDescriptorFactoryInterface $factory): QueryExpressionProviderInterface
    {
        return new QueryExpressionProvider($factory);
    }

    /**
     * @phpstan-param string|RawQuery<J>|RawNativeQuery<J>|RawQueryBuilder<J>|NativeQuery|QueryBuilder $data
     *
     * @phpstan-return static<J>
     *
     * @phpstan-template J of object|array<string, mixed> = array<string, mixed>
     */
    public function withData(string|RawQuery|RawNativeQuery|RawQueryBuilder|NativeQuery|QueryBuilder $data): static
    {
        /** @phpstan-var static<J> $clone */
        $clone       = clone $this;
        $clone->data = $data;

        return $clone;
    }

    public function withDenormalizer(callable $callback): static
    {
        /** @phpstan-var static<T> $clone */
        $clone               = clone $this;
        $clone->denormalizer = $callback;

        return $clone;
    }

    /**
     * @phpstan-param DataSourceOptionsWrapper $options
     * @phpstan-param string|RawQuery<J>|RawNativeQuery<J>|RawQueryBuilder<J>|NativeQuery|QueryBuilder|null $data
     *
     * @return ($data is null ? DataSource<object|array<string, mixed>> : DataSource<J>)
     *
     * @phpstan-template J of object|array<string, mixed>
     */
    public function create(Connection $connection, array $options = [], string|RawQuery|RawNativeQuery|RawQueryBuilder|NativeQuery|QueryBuilder|null $data = null): DataSource
    {
        $data ??= $this->data;
        if ($data === null) {
            throw new InvalidArgumentException('The data source has no data assigned! Expected a value other than null.');
        }

        $options['connection'] = $connection;
        if ($this->rootAlias !== null) {
            $options['root_alias'] = $this->rootAlias;
        }

        if ($this->rootIdentifier !== null) {
            $options['root_identifier'] = $this->rootIdentifier;
        }

        if ($this->denormalizer !== null) {
            $options['denormalizer'] = $this->denormalizer;
        }

        if ($this->itemNormalizer !== null) {
            $options['item_normalizer'] = $this->itemNormalizer;
        }

        if ($this->readModelDescriptor !== null) {
            $options['read_model_descriptor'] = $this->readModelDescriptor;
        }

        if ($this->fieldMapping !== null) {
            $options['field_map'] = $this->fieldMapping;
        }

        /** @phpstan-var DataSource<J> $dataSource */
        $dataSource = new DataSource($data, $this->queryExpressionProvider, $options);

        return $this->apply($dataSource);
    }

    #[Override]
    public function handleRequest(object $request, array $fieldsOperator = [], array $fieldsIgnoreCase = []): static
    {
        throw new LogicException('Unsupported operation. The data source builder can not handle requests.');
    }
}
