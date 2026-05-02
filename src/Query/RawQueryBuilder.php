<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;

/**
 * @phpstan-import-type WrapperParameterTypeArray from AbstractRawQuery
 * @phpstan-template-covariant T of object|array<string, mixed>
 */
class RawQueryBuilder extends QueryBuilder
{
    public function __construct(private Connection $connection)
    {
        parent::__construct($connection);
    }

    /** @phpstan-return RawQuery<T>  */
    public function getQuery(): RawQuery
    {
        $maxResults  = $this->getMaxResults();
        $firstResult = $this->getFirstResult();
        /** @phpstan-var RawQuery<T> $query */
        $query = new RawQuery($this->connection);
        try {
            $this->setFirstResult(0);
            $this->setMaxResults(null);
            $query
                ->setSql($this->getSQL())
                ->setParameters($this->getParameters(), $this->getParameterTypes())
                ->setMaxResults($maxResults)
                ->setFirstResult($firstResult);
        } finally {
            $this->setMaxResults($maxResults);
            $this->setFirstResult($firstResult);
        }

        return $query;
    }

    /** @phpstan-return WrapperParameterTypeArray */
    public function getParameterTypes(): array
    {
        /** @phpstan-var WrapperParameterTypeArray $types */
        $types = parent::getParameterTypes();

        return $types;
    }
}
