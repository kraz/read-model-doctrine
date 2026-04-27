<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Pagination;

use Kraz\ReadModel\Pagination\PaginatorInterface;
use Kraz\ReadModelDoctrine\Query\AbstractRawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;

/**
 * @template T of object
 *
 * @implements PaginatorInterface<T>
 */
final readonly class RawSqlPaginator implements PaginatorInterface
{
    private AbstractRawQuery $query;
    private int $firstResult;
    private int $maxResults;

    public function __construct(RawQueryBuilder|AbstractRawQuery $query)
    {
        if ($query instanceof RawQueryBuilder) {
            $query = $query->getQuery();
        }

        $firstResult = $query->getFirstResult();
        if (null === $firstResult) {
            throw new \InvalidArgumentException('Missing "firstResult" from the query.');
        }
        $maxResults = $query->getMaxResults();
        if (null === $maxResults) {
            throw new \InvalidArgumentException('Missing "maxResults" from the query.');
        }
        $this->firstResult = $firstResult;
        $this->maxResults = $maxResults;

        $this->query = $query;
    }

    public function getItemsPerPage(): int
    {
        return $this->maxResults;
    }

    public function getCurrentPage(): int
    {
        if (0 >= $this->maxResults) {
            return 1;
        }

        return 1 + (int) floor($this->firstResult / $this->maxResults);
    }

    public function getLastPage(): int
    {
        if (0 >= $this->maxResults) {
            return 1;
        }

        return (int) (ceil($this->getTotalItems() / $this->maxResults) ?: 1);
    }

    public function getTotalItems(): int
    {
        return $this->query->getCount();
    }

    public function count(): int
    {
        return iterator_count($this->getIterator());
    }

    /**
     * @return \Traversable<array-key, T>
     */
    #[\ReturnTypeWillChange]
    public function getIterator(): \Traversable
    {
        return $this->query->toIterable();
    }
}
