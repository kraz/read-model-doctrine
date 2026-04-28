<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Pagination;

use InvalidArgumentException;
use Kraz\ReadModel\Pagination\PaginatorInterface;
use Kraz\ReadModelDoctrine\Query\AbstractRawQuery;
use Kraz\ReadModelDoctrine\Query\RawQueryBuilder;
use ReturnTypeWillChange;
use Traversable;

use function ceil;
use function floor;
use function iterator_count;
use function max;

/**
 * @phpstan-template-covariant T of object|array<string, mixed>
 * @phpstan-implements PaginatorInterface<T>
 */
final readonly class RawSqlPaginator implements PaginatorInterface
{
    /** @phpstan-var AbstractRawQuery<T> */
    private AbstractRawQuery $query;
    private int $firstResult;
    /** @phpstan-var int<0, max> */
    private int $maxResults;

    /** @phpstan-param RawQueryBuilder<T>|AbstractRawQuery<T> $query */
    public function __construct(RawQueryBuilder|AbstractRawQuery $query)
    {
        if ($query instanceof RawQueryBuilder) {
            $query = $query->getQuery();
        }

        $firstResult = $query->getFirstResult();
        if ($firstResult === null) {
            throw new InvalidArgumentException('Missing "firstResult" from the query.');
        }

        $maxResults = $query->getMaxResults();
        if ($maxResults === null || $maxResults < 0) {
            throw new InvalidArgumentException('Missing "maxResults" from the query.');
        }

        $this->firstResult = $firstResult;
        $this->maxResults  = $maxResults;

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

        return max(0, 1 + (int) floor($this->firstResult / $this->maxResults));
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
        return $this->query->getCount();
    }

    public function count(): int
    {
        return iterator_count($this->getIterator());
    }

    /** @return Traversable<array-key, T> */
    #[ReturnTypeWillChange]
    public function getIterator(): Traversable
    {
        return $this->query->toIterable();
    }
}
