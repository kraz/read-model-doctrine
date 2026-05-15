<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Pagination;

use ArrayIterator;
use Closure;
use Kraz\ReadModel\Exception\InvalidCursorException;
use Kraz\ReadModel\Pagination\Cursor\Cursor;
use Kraz\ReadModel\Pagination\Cursor\CursorCodecInterface;
use Kraz\ReadModel\Pagination\Cursor\CursorPaginatorInterface;
use Kraz\ReadModel\Pagination\Cursor\Direction;
use Kraz\ReadModel\Query\SortExpression;
use LogicException;
use Override;
use ReturnTypeWillChange;
use Traversable;

use function array_reverse;
use function array_slice;
use function array_values;
use function count;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_object;
use function is_string;
use function method_exists;
use function property_exists;
use function ucfirst;

/**
 * Cursor paginator over Doctrine-fetched results.
 *
 * Receives the rows already retrieved by Doctrine — fetched with the keyset predicate
 * pre-applied and `LIMIT (n+1)` set — and produces the bounded window plus the opaque
 * next/previous cursor tokens. The (n+1)-th row is used purely to detect whether more
 * data exists in the traversal direction; it never reaches the caller.
 *
 * For backward traversal, the query is executed against an inverted sort, which gives
 * the database the chance to use an index in the natural direction; this paginator
 * reverses the returned rows back to the caller-visible order.
 *
 * @phpstan-template-covariant T of object|array<string, mixed>
 * @phpstan-implements CursorPaginatorInterface<T>
 */
final class DoctrineCursorPaginator implements CursorPaginatorInterface
{
    /** @phpstan-var list<T> */
    private array $window;

    private bool $hasNext;

    private bool $hasPrevious;

    private string|null $nextCursor = null;

    private string|null $previousCursor = null;

    /** @phpstan-var Closure(mixed, string): mixed */
    private Closure $fieldAccessor;

    /**
     * @phpstan-param list<T>                                   $fetched      Doctrine result, up to limit+1 rows.
     * @phpstan-param int<1, max>                               $limit
     * @phpstan-param int<0, max>|null                          $totalItems
     * @phpstan-param (Closure(mixed, string): mixed)|null      $fieldAccessor
     */
    public function __construct(
        array $fetched,
        private readonly SortExpression $effectiveSort,
        private readonly Direction $direction,
        private readonly int $limit,
        private readonly CursorCodecInterface $codec,
        bool $cameFromCursor,
        private readonly int|null $totalItems = null,
        Closure|null $fieldAccessor = null,
    ) {
        if ($limit < 1) {
            throw new LogicException('Cursor limit must be a positive integer.');
        }

        if ($effectiveSort->isSortEmpty()) {
            throw new LogicException('Cursor pagination requires a non-empty sort expression.');
        }

        $fieldAccessor     ??= self::defaultFieldAccessor(...);
        $this->fieldAccessor = $fieldAccessor;

        $hasMore = count($fetched) > $limit;
        $window  = $hasMore ? array_slice($fetched, 0, $limit) : $fetched;

        if ($direction === Direction::BACKWARD) {
            // The query was executed under inverted sort to walk backwards efficiently;
            // restore the caller-visible order before exposing the window.
            $window = array_reverse($window);
        }

        $this->window = array_values($window);

        if ($direction === Direction::FORWARD) {
            $this->hasNext     = $hasMore;
            $this->hasPrevious = $cameFromCursor;
        } else {
            $this->hasPrevious = $hasMore;
            // The caller arrived here by going BACKWARD from somewhere — that somewhere
            // is always "forward" relative to the current window.
            $this->hasNext = true;
        }

        $windowCount = count($this->window);
        if ($windowCount === 0) {
            return;
        }

        $signature = Cursor::signatureFor($this->effectiveSort);
        $sortItems = array_values($this->effectiveSort->items());

        if ($this->hasNext) {
            $last             = $this->window[$windowCount - 1];
            $this->nextCursor = $this->codec->encode(new Cursor(
                Direction::FORWARD,
                $this->extractPosition($last, $sortItems),
                $signature,
            ));
        }

        if (! $this->hasPrevious) {
            return;
        }

        $first                = $this->window[0];
        $this->previousCursor = $this->codec->encode(new Cursor(
            Direction::BACKWARD,
            $this->extractPosition($first, $sortItems),
            $signature,
        ));
    }

    #[Override]
    public function getLimit(): int
    {
        return $this->limit;
    }

    #[Override]
    public function getDirection(): Direction
    {
        return $this->direction;
    }

    #[Override]
    public function hasNext(): bool
    {
        return $this->hasNext;
    }

    #[Override]
    public function hasPrevious(): bool
    {
        return $this->hasPrevious;
    }

    #[Override]
    public function getNextCursor(): string|null
    {
        return $this->nextCursor;
    }

    #[Override]
    public function getPreviousCursor(): string|null
    {
        return $this->previousCursor;
    }

    #[Override]
    public function getTotalItems(): int|null
    {
        return $this->totalItems;
    }

    /** @return Traversable<array-key, T> */
    #[ReturnTypeWillChange]
    #[Override]
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->window);
    }

    #[Override]
    public function count(): int
    {
        return count($this->window);
    }

    /**
     * @phpstan-param T                                        $item
     * @phpstan-param list<array{field: string, dir: string}>  $sortItems
     *
     * @phpstan-return list<array{field: string, value: scalar|null}>
     */
    private function extractPosition(mixed $item, array $sortItems): array
    {
        $position = [];
        foreach ($sortItems as $sortItem) {
            /** @phpstan-var mixed $value */
            $value = ($this->fieldAccessor)($item, $sortItem['field']);
            if ($value !== null && ! is_int($value) && ! is_float($value) && ! is_string($value) && ! is_bool($value)) {
                throw new InvalidCursorException('Field "' . $sortItem['field'] . '" produced a non-scalar value when extracting the cursor position.');
            }

            $position[] = ['field' => $sortItem['field'], 'value' => $value];
        }

        return $position;
    }

    private static function defaultFieldAccessor(mixed $item, string $field): mixed
    {
        if (is_object($item)) {
            if (property_exists($item, $field)) {
                return $item->{$field};
            }

            $getter = 'get' . ucfirst($field);
            if (method_exists($item, $getter)) {
                /** @phpstan-var mixed $value */
                $value = $item->{$getter}();

                return $value;
            }

            return null;
        }

        if (is_array($item)) {
            return $item[$field] ?? null;
        }

        return null;
    }
}
