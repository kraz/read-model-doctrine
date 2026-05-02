<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine;

use Kraz\ReadModel\DataSourceReadDataProvider;
use Kraz\ReadModelDoctrine\Query\AbstractRawQuery;
use LogicException;

/** @phpstan-template-covariant T of object|array<string, mixed> */
trait DoctrineReadDataProvider
{
    /** @use DataSourceReadDataProvider<T> */
    use DataSourceReadDataProvider;

    /** @phpstan-return DataSource<T> */
    abstract protected function createDataSource(): DataSource;

    /** @phpstan-return AbstractRawQuery<T> */
    public function getRawQuery(): AbstractRawQuery
    {
        $dataSource = $this->dataSource();
        if (! $dataSource instanceof DataSource) {
            throw new LogicException('Unsupported data source!');
        }

        /** @phpstan-var AbstractRawQuery<T> $query */
        $query = $dataSource->getRawQuery();

        return $query;
    }
}
