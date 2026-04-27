<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;

class RawQueryBuilder extends QueryBuilder
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        parent::__construct($connection);
        $this->connection = $connection;
    }

    public function getQuery(): RawQuery
    {
        $maxResults = $this->getMaxResults();
        $firstResult = $this->getFirstResult();
        $query = new RawQuery($this->connection);
        try {
            $this->setFirstResult(0);
            $this->setMaxResults(null);
            $query
                ->setSql($this->getSQL())
                ->setParameters($this->getParameters(), $this->getParameterTypes())
                ->setMaxResults($maxResults)
                ->setFirstResult($firstResult)
            ;
        } finally {
            $this->setMaxResults($maxResults);
            $this->setFirstResult($firstResult);
        }

        return $query;
    }
}
