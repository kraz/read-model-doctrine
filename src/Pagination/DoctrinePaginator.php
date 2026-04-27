<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Pagination;

use Doctrine\ORM\Tools\Pagination\Paginator;
use Kraz\ReadModel\Pagination\PaginatorInterface;

/**
 * @template T of object
 *
 * @implements PaginatorInterface<T>
 */
final readonly class DoctrinePaginator implements PaginatorInterface
{
    private int $firstResult;
    private int $maxResults;

    /**
     * @param Paginator<T> $paginator
     */
    public function __construct(
        private Paginator $paginator,
    ) {
        $firstResult = $paginator->getQuery()->getFirstResult();
        $maxResults = $paginator->getQuery()->getMaxResults();
        if (null === $maxResults) {
            throw new \InvalidArgumentException('Missing maxResults from the query.');
        }
        $this->firstResult = $firstResult;
        $this->maxResults = $maxResults;
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
        return \count($this->paginator);
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
        return $this->paginator->getIterator();
    }
}
