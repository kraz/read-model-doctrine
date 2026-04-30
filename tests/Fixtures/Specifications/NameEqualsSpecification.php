<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Fixtures\Specifications;

use Kraz\ReadModel\Specification\AbstractSpecification;

use function is_array;

/** @phpstan-extends AbstractSpecification<array<string, mixed>> */
final class NameEqualsSpecification extends AbstractSpecification
{
    public function __construct(private readonly string $name)
    {
    }

    /** @phpstan-param array<string, mixed> $item */
    public function isSatisfiedBy(object|array $item): bool
    {
        $satisfies = is_array($item) && $item['name'] === $this->name;

        return $this->inverted() ? ! $satisfies : $satisfies;
    }
}
