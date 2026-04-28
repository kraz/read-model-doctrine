<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine;

use Kraz\ReadModel\ReadDataProviderInterface;
use Kraz\ReadModelDoctrine\Query\EmptyRawQuery;

/** @phpstan-implements ReadDataProviderInterface<array<never, never>> */
class EmptyReadModel implements ReadDataProviderInterface
{
    /** @use DoctrineReadDataProvider<array<never, never>> */
    use DoctrineReadDataProvider;

    protected function createDataSource(): DataSource
    {
        return new DataSource(new EmptyRawQuery());
    }
}
