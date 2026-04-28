<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\SQLite3\Driver;
use Traversable;

/**
 * @phpstan-template-covariant T of array<never, never>
 * @phpstan-extends AbstractRawQuery<T>
 */
class EmptyRawQuery extends AbstractRawQuery
{
    public function __construct()
    {
        parent::__construct(new Connection([], new Driver()));
    }

    public function close(): static
    {
        return parent::closeStatement();
    }

    /** @phpstan-return Traversable<array-key, T> */
    public function toIterable(): Traversable
    {
        yield from [];
    }
}
