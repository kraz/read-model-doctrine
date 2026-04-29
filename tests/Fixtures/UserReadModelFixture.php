<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Fixtures;

use Doctrine\DBAL\Connection;
use Kraz\ReadModel\ReadDataProviderInterface;
use Kraz\ReadModelDoctrine\DataSource;
use Kraz\ReadModelDoctrine\DataSourceBuilder;
use Kraz\ReadModelDoctrine\DoctrineReadDataProvider;

/**
 * Mirrors the production read model pattern: SQL with WHERE/ORDERBY placeholders,
 * a subquery alias, and FIELD_* constants for consumer field name references.
 *
 * Implementing the read mode like this also gives us the flexibility to use
 * dependency inversion, for example: In-Memory or Doctrine implementation (like this one).
 *
 * @phpstan-type UserReadModelItem = array{
 *     id: string,
 *     name: string,
 *     department: string,
 *     age: int,
 *     active: int,
 * }
 * @implements ReadDataProviderInterface<UserReadModelItem>
 */
final class UserReadModelFixture implements ReadDataProviderInterface
{
    /** @use DoctrineReadDataProvider<UserReadModelItem> */
    use DoctrineReadDataProvider;

    public const string FIELD_ID         = 'id';
    public const string FIELD_NAME       = 'name';
    public const string FIELD_DEPARTMENT = 'department';
    public const string FIELD_AGE        = 'age';
    public const string FIELD_ACTIVE     = 'active';

    public function __construct(
        private readonly Connection $connection,
    ) {
    }

    protected function createDataSource(): DataSource
    {
        /** @phpstan-var DataSource<UserReadModelItem> $ds */
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
                /*#ORDERBY_B#*/ORDER BY r.id ASC/*#ORDERBY_E#*/
            SQL)
            ->create($this->connection);

        return $ds;
    }
}
