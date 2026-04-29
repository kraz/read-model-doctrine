<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Fixtures;

use Doctrine\DBAL\Connection;
use Kraz\ReadModelDoctrine\DataSource;
use Kraz\ReadModelDoctrine\DataSourceBuilder;

// Mirrors the production read model pattern: SQL with WHERE/ORDERBY placeholders,
// a subquery alias, and FIELD_* constants for consumer field name references.
final class UserReadModelFixture
{
    public const string FIELD_ID         = 'id';
    public const string FIELD_NAME       = 'name';
    public const string FIELD_DEPARTMENT = 'department';
    public const string FIELD_AGE        = 'age';
    public const string FIELD_ACTIVE     = 'active';

    /** @phpstan-return DataSource<array<string, mixed>> */
    public function createDataSource(Connection $connection): DataSource
    {
        /** @phpstan-var DataSource<array<string, mixed>> $ds */
        $ds = new DataSourceBuilder()
            ->withData(<<<'SQL'
                SELECT r.* FROM (
                    SELECT
                        t.id AS id,
                        t.name AS name,
                        t.department AS department,
                        t.age AS age,
                        t.active AS active
                    FROM test_entity t
                ) r
                /*#WHERE#*/
                /*#ORDERBY#*/
                ORDER BY r.id ASC
            SQL)
            ->create($connection);

        return $ds;
    }
}
