<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform as AbstractDatabasePlatform;
use Doctrine\DBAL\Result as DBALResult;
use Kraz\ReadModelDoctrine\Exception;
use Kraz\ReadModelDoctrine\Tools;

/**
 * @psalm-type AbstractRawQueryOptions = array{
 *     database_platform_class: array<string, class-string<AbstractDatabasePlatform>>,
 *     sql_formatter: array,
 *     use_count_cache: bool,
 *     item_normalizer: callable|null,
 *  }
 * @psalm-type AbstractRawQueryOptionsWrapper = AbstractRawQueryOptions|array<never, never>
 *
 * @template-covariant T of object
 */
abstract class AbstractRawQuery
{
    private string $sql = '';
    private Tools\QueryParts $sqlEx;
    private array $params = [];
    private array $paramTypes = [];
    private Connection $connection;
    private ?DBALResult $statement = null;
    private ?int $firstResult = null;
    private ?int $maxResults = null;
    private ?string $countSql = null;
    private ?int $count = null;
    private ?string $countHash = null;
    private ?AbstractDatabasePlatform $platform = null;

    /**
     * @psalm-var AbstractRawQueryOptions
     */
    private array $options;

    /**
     * @psalm-param AbstractRawQueryOptionsWrapper $options
     */
    public function __construct(Connection $connection, array $options = [])
    {
        $this->connection = $connection;

        $this->sqlEx = new Tools\QueryParts();

        /** @psalm-var AbstractRawQueryOptions $options */
        $options = array_replace_recursive($this->getDefaultOptions(), $options);
        $this->options = $options;
    }

    /**
     * @psalm-return AbstractRawQueryOptions
     */
    public function getDefaultOptions(): array
    {
        return [
            'database_platform_class' => [
                //                'oracle' => Platforms\OraclePlatform::class,
            ],
            'sql_formatter' => [
            ],
            'use_count_cache' => true,
            'item_normalizer' => null,
        ];
    }

    /**
     * @psalm-param AbstractRawQueryOptionsWrapper $options
     */
    public function setOptions(array $options): static
    {
        $this->platform = null;

        /** @psalm-var AbstractRawQueryOptions $options */
        $options = array_replace_recursive($this->getDefaultOptions(), $options);
        $this->options = $options;

        return $this;
    }

    /**
     * @psalm-return AbstractRawQueryOptions
     */
    public function getOptions(): array
    {
        return $this->options;
    }

    protected function getItemNormalizer(): ?callable
    {
        $itemNormalizer = $this->options['item_normalizer'] ?? null;
        if (\is_callable($itemNormalizer)) {
            return $itemNormalizer;
        }

        return null;
    }

    protected function getDatabasePlatform(): AbstractDatabasePlatform
    {
        if ($this->platform) {
            return $this->platform;
        }

        $opt = $this->getOptions();
        $platform = $this->getConnection()->getDatabasePlatform();

        /** @var AbstractDatabasePlatform|class-string<AbstractDatabasePlatform>|null $platformOverride */
        $platformOverride = $opt['database_platform_class'][$platform::class] ?? null;

        if (null !== $platformOverride) {
            if (\is_string($platformOverride)) {
                $platformOverride = new $platformOverride();
            }
            if ($platformOverride instanceof AbstractDatabasePlatform) {
                $platform = $platformOverride;
            } else {
                throw new \RuntimeException(\sprintf('Invalid database platform. Expected instance of "%s", but got "%s"', AbstractDatabasePlatform::class, \is_object($platformOverride) ? $platformOverride::class : \gettype($platformOverride)));
            }
        }

        $this->platform = $platform;

        return $this->platform;
    }

    protected function resetCountCache(): static
    {
        $this->count = null;
        $this->countHash = null;

        return $this;
    }

    protected function closeStatement(): static
    {
        if (null !== $this->statement) {
            if (method_exists($this->statement, 'closeCursor')) {
                $this->statement->closeCursor();
            }
            $this->statement = null;
        }

        return $this;
    }

    protected function hasStatement(): bool
    {
        return null !== $this->statement;
    }

    public function getConnection(): Connection
    {
        return $this->connection;
    }

    public function setSql(string $sql): static
    {
        if (0 !== strcmp($this->sql, $sql)) {
            $this->close();
        }

        $this->sql = $sql;

        return $this;
    }

    public function getSql(): string
    {
        return $this->sql;
    }

    public function sql(): Tools\QueryParts
    {
        return $this->sqlEx;
    }

    /**
     * Set count SQL text. Optimized version of the original SQL which shares the same parameters.
     */
    public function setCountSql(?string $sql): static
    {
        $this->countSql = $sql;

        return $this;
    }

    public function getCountSql(): ?string
    {
        $countSql = $this->countSql;
        if (null === $countSql) {
            $sql = $this->getExtendedSql();
            if ($sql) {
                $countSql = 'SELECT COUNT(*) FROM ('.\PHP_EOL.$sql.\PHP_EOL.') raw_cnt_query';
            }
        }

        return $countSql;
    }

    /**
     * Get an extended version of the query SQL, after applying the rules from the sql() parts.
     */
    public function getExtendedSql(): string
    {
        $sql = $this->getSql();

        $opt = $this->getOptions();
        $formatter = new Tools\SqlFormatter($opt['sql_formatter'] ?? []);

        return $formatter->formatSqlParts($sql, $this->sqlEx);
    }

    protected function getExecuteSql(): string
    {
        $sql = $this->getExtendedSql();
        if (null !== $this->maxResults) {
            $sql = $this->getDatabasePlatform()->modifyLimitQuery(
                $sql, $this->maxResults, $this->firstResult ?? 0
            );
        }

        return $sql;
    }

    public function setParameter(int|string $key, mixed $value, ParameterType|ArrayParameterType|string|int|null $type = null): static
    {
        if (null !== $type) {
            $this->paramTypes[$key] = $type;
        }

        $this->params[$key] = $value;

        return $this;
    }

    public function setParameters(array $params, array $types = []): static
    {
        $this->paramTypes = $types;
        $this->params = $params;

        return $this;
    }

    public function getParameters(): array
    {
        return $this->params;
    }

    public function getParameter(string|int $key): mixed
    {
        return $this->params[$key] ?? null;
    }

    public function getParameterTypes(): array
    {
        return $this->paramTypes;
    }

    public function getParameterType(string|int $key): mixed
    {
        return $this->paramTypes[$key] ?? null;
    }

    public function setFirstResult(?int $firstResult): static
    {
        $this->closeStatement();
        $this->firstResult = $firstResult;

        return $this;
    }

    public function getFirstResult(): ?int
    {
        return $this->firstResult;
    }

    public function setMaxResults(?int $maxResults): static
    {
        $this->closeStatement();
        $this->maxResults = $maxResults;

        return $this;
    }

    public function getMaxResults(): ?int
    {
        return $this->maxResults;
    }

    protected function doExecute(string $sql, ?array $params = null, ?array $types = null): ?DBALResult
    {
        if (!\is_array($params)) {
            $params = $this->params;
        }
        if (!\is_array($types)) {
            $types = $this->paramTypes;
        }

        // Override parameters
        $params = array_replace($this->params, $params);
        $types = array_replace($this->paramTypes, $types);

        if (\count($params) !== \count($this->params)) {
            $this->close();
        } else {
            $diffKeys = array_merge(
                array_diff_key($params, $this->params),
                array_diff_key($this->params, $params)
            );
            if (\count($diffKeys) > 0) {
                $this->close();
            } else {
                foreach (array_keys($params) as $k) {
                    if ($params[$k] !== $this->params[$k]) {
                        $this->close();
                        break;
                    }
                }
            }
        }

        if (!$this->statement) {
            $this->statement = $this->connection->executeQuery($sql, $params, $types);
        }

        return $this->statement;
    }

    /**
     * @return \Traversable<array-key, T>
     */
    abstract public function toIterable(): \Traversable;

    /**
     * @return T[]
     */
    public function getArrayResult(): array
    {
        $itemNormalizer = $this->getItemNormalizer();
        $this->options['item_normalizer'] = null;
        try {
            $result = iterator_to_array($this->toIterable());
        } finally {
            $this->options['item_normalizer'] = $itemNormalizer;
        }

        return $result;
    }

    /**
     * @return T[]
     */
    public function getResult(): array
    {
        return iterator_to_array($this->toIterable());
    }

    /**
     * Gets the single result of the query.
     *
     * Enforces the presence as well as the uniqueness of the result.
     *
     * If the result is not unique, a NonUniqueResultException is thrown.
     * If there is no result, a NoResultException is thrown.
     *
     * @throws Exception\NonUniqueResultException if the query result is not unique
     * @throws Exception\NoResultException        if the query returned no result
     *
     * @psalm-return T
     */
    public function getSingleResult(): mixed
    {
        $result = $this->getResult();

        if (!$result) {
            throw new Exception\NoResultException();
        }

        if (\count($result) > 1) {
            throw new Exception\NonUniqueResultException();
        }

        return array_shift($result);
    }

    /**
     * Get exactly one result or null.
     *
     * @throws Exception\NonUniqueResultException
     *
     * @psalm-return T|null
     */
    public function getOneOrNullResult(): mixed
    {
        $result = $this->getResult();

        if (!$result) {
            return null;
        }

        if (\count($result) > 1) {
            throw new Exception\NonUniqueResultException();
        }

        return array_shift($result);
    }

    /**
     * Get the query TOTAL row count. No limits or offsets used.
     */
    public function getCount(): int
    {
        $countSql = $this->getCountSql();
        if (null === $countSql || '' === $countSql) {
            return 0;
        }

        $useCountCache = $this->options['use_count_cache'] ?? true;

        if ($useCountCache) {
            $countHash = sha1($countSql);
            if (null !== $this->count && null !== $this->countHash && '' !== $this->countHash && 0 === strcasecmp($countHash, $this->countHash)) {
                // The count has been already fetched and the SQL was not modified
                return $this->count;
            }
            $this->resetCountCache();
            $this->countHash = $countHash;
        }

        $maxResults = $this->getMaxResults();
        $firstResult = $this->getFirstResult();
        try {
            $this
                ->setMaxResults(null)
                ->setFirstResult(null)
            ;
            $count = (int) array_sum(array_map('current', $this->doExecute($countSql)?->fetchAllAssociative() ?? []));
            $this->closeStatement();
        } finally {
            $this
                ->setMaxResults($maxResults)
                ->setFirstResult($firstResult)
            ;
        }
        $this->count = $count;

        return $this->count;
    }

    public function close(): static
    {
        $this->closeStatement();
        $this->resetCountCache();

        return $this;
    }

    public function __clone()
    {
        $this->sqlEx = clone $this->sqlEx;
        $this->statement = null;
        $this->countHash = null;
    }
}
