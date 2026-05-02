<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Fixtures;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Types\Types as DBType;
use Kraz\ReadModel\ReadDataProviderInterface;
use Kraz\ReadModelDoctrine\DataSource;
use Kraz\ReadModelDoctrine\RawQueryReadDataProvider;
use Kraz\ReadModelDoctrine\Tools\ParametersCollection;
use Kraz\ReadModelDoctrine\Tools\QueryParts;

/**
 * Mirrors the production read model pattern: SQL with WHERE/ORDERBY placeholders,
 * a subquery alias, and FIELD_* constants for consumer field name references.
 *
 * Implementing the read mode like this also gives us the flexibility to use
 * dependency inversion, for example: In-Memory or Doctrine implementation (like this one).
 *
 * Note: This is the simplified version when only having the SQL is enough
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
final class UserSQLReadModelFixture implements ReadDataProviderInterface
{
    /** @use RawQueryReadDataProvider<UserReadModelItem> */
    use RawQueryReadDataProvider;

    public const string FIELD_ID         = 'id';
    public const string FIELD_NAME       = 'name';
    public const string FIELD_DEPARTMENT = 'department';
    public const string FIELD_AGE        = 'age';
    public const string FIELD_ACTIVE     = 'active';

    public function __construct(
        private readonly Connection $connection,
    ) {
    }

    /** @phpstan-return DataSource<UserReadModelItem> */
    protected function createDataSource(): DataSource
    {
        return $this->rawQuery($this->connection, <<<'SQL'
            SELECT r.* FROM (
                SELECT
                    t.id AS id,
                    t.name AS name,
                    t.department AS department,
                    t.age AS age,
                    t.active AS active
                FROM test_entity t
            ) r
            WHERE r.age > :p_age
            /*#WHERE_B#*/AND 0=0/*#WHERE_E#*/
            /*#ORDERBY_B#*/ORDER BY r.id ASC/*#ORDERBY_E#*/
        SQL, new ParametersCollection()
            ->setParameter('p_age', 0, DBType::INTEGER));
    }

    public function olderThan25(): self
    {
        return $this->withQueryModifier(static function (QueryParts $qp, ParametersCollection $params): void {
            $params->setParameter('p_age', 25, DBType::INTEGER);
        });
    }
}
