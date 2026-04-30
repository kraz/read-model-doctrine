<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Fixtures\Specifications;

use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Specification\AbstractSpecification;

use function intval;
use function is_array;

/** @phpstan-extends AbstractSpecification<array<string, mixed>> */
final class AgeAboveSpecification extends AbstractSpecification
{
    public function __construct(private readonly int $minAge)
    {
    }

    /** @phpstan-param array<string, mixed> $item */
    public function isSatisfiedBy(object|array $item): bool
    {
        $satisfies = is_array($item) && intval($item['age']) > $this->minAge;

        return $this->inverted() ? ! $satisfies : $satisfies;
    }

    protected function buildQueryExpression(): QueryExpression
    {
        return QueryExpression::create()->andWhere(
            FilterExpression::create()->greaterThan('age', $this->minAge),
        );
    }
}
