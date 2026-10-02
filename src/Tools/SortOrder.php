<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tools;

use Doctrine\ORM\Query\Expr;
use ReflectionMethod;
use ReflectionNamedType;
use ReflectionUnionType;
use SortDirection;

use function enum_exists;
use function strtoupper;
use function trim;

/**
 * Resolves the sort order in the form expected by the installed doctrine/orm version.
 *
 * Newer doctrine/orm versions deprecate passing the sort order as string in favor of the SortDirection enum,
 * while the older ones accept only strings. The string form is kept as long as the installed version does
 * not know about the enum.
 *
 * @internal
 */
final class SortOrder
{
    private static bool|null $sortDirectionSupported = null;

    /**
     * A string which is not a plain sort direction (like "DESC NULLS LAST") is returned as it is.
     */
    public static function resolve(SortDirection|string|null $order): SortDirection|string|null
    {
        if ($order instanceof SortDirection || ! self::isSortDirectionSupported()) {
            return $order;
        }

        return match (strtoupper(trim($order ?? 'ASC'))) {
            'ASC' => SortDirection::Ascending,
            'DESC' => SortDirection::Descending,
            default => $order,
        };
    }

    public static function createOrderBy(string $sort, SortDirection|string|null $order = null): Expr\OrderBy
    {
        return new Expr\OrderBy($sort, self::resolve($order));
    }

    private static function isSortDirectionSupported(): bool
    {
        return self::$sortDirectionSupported ??= self::detectSortDirectionSupport();
    }

    private static function detectSortDirectionSupport(): bool
    {
        if (! enum_exists(SortDirection::class)) {
            return false;
        }

        $type = new ReflectionMethod(Expr\OrderBy::class, 'add')->getParameters()[1]->getType();
        if (! $type instanceof ReflectionUnionType) {
            return false;
        }

        foreach ($type->getTypes() as $candidate) {
            if ($candidate instanceof ReflectionNamedType && $candidate->getName() === SortDirection::class) {
                return true;
            }
        }

        return false;
    }
}
