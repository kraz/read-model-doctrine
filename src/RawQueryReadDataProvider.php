<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine;

use Doctrine\DBAL\Connection;
use Kraz\ReadModelDoctrine\Query\RawQuery;
use Kraz\ReadModelDoctrine\Tools\ParametersCollection;

use function is_string;

/**
 * @phpstan-import-type DataSourceOptionsWrapper from DataSource
 * @phpstan-template-covariant T of object|array<string, mixed>
 */
trait RawQueryReadDataProvider
{
    /** @use DoctrineReadDataProvider<T> */
    use DoctrineReadDataProvider;

    /**
     * @phpstan-param string|RawQuery<T> $query
     * @phpstan-param DataSourceOptionsWrapper $options
     *
     * @phpstan-return DataSource<T>
     */
    protected function rawQuery(Connection $connection, string|RawQuery $query, ParametersCollection|null $parameters = null, array $options = []): DataSource
    {
        if (is_string($query)) {
            $query = new RawQuery($connection)->setSql($query);
        }

        if ($parameters !== null) {
            foreach ($parameters->getParameters() as $parameter) {
                $query->setParameter($parameter->getName(), $parameter->getValue(), $parameter->getType());
            }
        }

        $builder = new DataSourceBuilder()->withData($query);

        /** @var DataSource<T> $ds */
        $ds = $builder->create($connection, $options);

        return $ds;
    }
}
