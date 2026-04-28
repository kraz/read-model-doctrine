<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Pagination;

use Doctrine\ORM\Tools\Pagination\Paginator;
use InvalidArgumentException;
use Kraz\ReadModel\Pagination\PaginatorInterface;
use ReturnTypeWillChange;
use Traversable;

use function ceil;
use function count;
use function floor;
use function iterator_count;
use function max;

/**
 * @phpstan-template T of object|array<string, mixed>
 * @phpstan-implements PaginatorInterface<T>
 */
final readonly class DoctrinePaginator implements PaginatorInterface
{
    private int $firstResult;
    /** @phpstan-var int<0, max> */
    private int $maxResults;

    /** @phpstan-param Paginator<T> $paginator */
    public function __construct(
        private Paginator $paginator,
    ) {
        $firstResult = $paginator->getQuery()->getFirstResult();
        $maxResults  = $paginator->getQuery()->getMaxResults();
        if ($maxResults === null || $maxResults < 0) {
            throw new InvalidArgumentException('Missing maxResults from the query.');
        }

        $this->firstResult = $firstResult;
        $this->maxResults  = $maxResults;
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

        return max(-0, 1 + (int) floor($this->firstResult / $this->maxResults));
    }

    public function getLastPage(): int
    {
        if (0 >= $this->maxResults) {
            return 1;
        }

        return max(0, (int) (ceil($this->getTotalItems() / $this->maxResults) ?: 1));
    }

    public function getTotalItems(): int
    {
        return count($this->paginator);
    }

    public function count(): int
    {
        return iterator_count($this->getIterator());
    }

    /** @return Traversable<array-key, T> */
    #[ReturnTypeWillChange]
    public function getIterator(): Traversable
    {
        return $this->paginator->getIterator();
    }
}
