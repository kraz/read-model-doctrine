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
use RuntimeException;
use Traversable;

use function array_diff_key;
use function array_keys;
use function array_map;
use function array_merge;
use function array_replace;
use function array_replace_recursive;
use function array_shift;
use function array_sum;
use function count;
use function gettype;
use function is_array;
use function is_callable;
use function is_object;
use function is_string;
use function iterator_to_array;
use function max;
use function method_exists;
use function sha1;
use function sprintf;
use function strcasecmp;
use function strcmp;

use const PHP_EOL;

/**
 * @phpstan-import-type SqlFormatterOptions from Tools\SqlFormatter
 * @phpstan-type WrapperParameterType = ParameterType|ArrayParameterType|string|int
 * @phpstan-type WrapperParameterTypeArray = array<int|string, WrapperParameterType>
 * @phpstan-type AbstractRawQueryOptions = array{
 *     database_platform_class: array<string, class-string<AbstractDatabasePlatform>>,
 *     sql_formatter: SqlFormatterOptions,
 *     use_count_cache: bool,
 *     item_normalizer: callable|null,
 *  }
 * @phpstan-type AbstractRawQueryOptionsWrapper = AbstractRawQueryOptions|array<never, never>
 * @phpstan-template-covariant T of object|array<string, mixed>
 */
abstract class AbstractRawQuery
{
    private string $sql = '';
    private Tools\QueryParts $sqlEx;
    /** @phpstan-var array<int|string, mixed> */
    private array $params = [];
    /** @phpstan-var WrapperParameterTypeArray */
    private array $paramTypes          = [];
    private DBALResult|null $statement = null;
    private int|null $firstResult      = null;
    private int|null $maxResults       = null;
    private string|null $countSql      = null;
    /** @phpstan-var int<0, max> */
    private int|null $count                         = null;
    private string|null $countHash                  = null;
    private AbstractDatabasePlatform|null $platform = null;

    /** @phpstan-var AbstractRawQueryOptions */
    private array $options;

    /** @phpstan-param AbstractRawQueryOptionsWrapper $options */
    public function __construct(private Connection $connection, array $options = [])
    {
        $this->sqlEx = new Tools\QueryParts();

        /** @phpstan-var AbstractRawQueryOptions $options */
        $options       = array_replace_recursive($this->getDefaultOptions(), $options);
        $this->options = $options;
    }

    /** @phpstan-return AbstractRawQueryOptions */
    public function getDefaultOptions(): array
    {
        return [
            'database_platform_class' => [/* 'oracle' => Platforms\OraclePlatform::class, */],
            'sql_formatter' => [],
            'use_count_cache' => true,
            'item_normalizer' => null,
        ];
    }

    /** @phpstan-param AbstractRawQueryOptionsWrapper $options */
    public function setOptions(array $options): static
    {
        $this->platform = null;

        /** @phpstan-var AbstractRawQueryOptions $options */
        $options       = array_replace_recursive($this->getDefaultOptions(), $options);
        $this->options = $options;

        return $this;
    }

    /** @phpstan-return AbstractRawQueryOptions */
    public function getOptions(): array
    {
        return $this->options;
    }

    protected function getItemNormalizer(): callable|null
    {
        $itemNormalizer = $this->options['item_normalizer'] ?? null;
        if (is_callable($itemNormalizer)) {
            return $itemNormalizer;
        }

        return null;
    }

    protected function getDatabasePlatform(): AbstractDatabasePlatform
    {
        if ($this->platform) {
            return $this->platform;
        }

        $opt      = $this->getOptions();
        $platform = $this->getConnection()->getDatabasePlatform();

        /** @var AbstractDatabasePlatform|class-string<AbstractDatabasePlatform>|null $platformOverride */
        $platformOverride = $opt['database_platform_class'][$platform::class] ?? null;

        if ($platformOverride !== null) {
            if (is_string($platformOverride)) {
                $platformOverride = new $platformOverride();
            }

            if (! ($platformOverride instanceof AbstractDatabasePlatform)) {
                throw new RuntimeException(sprintf('Invalid database platform. Expected instance of "%s", but got "%s"', AbstractDatabasePlatform::class, is_object($platformOverride) ? $platformOverride::class : gettype($platformOverride)));
            }

            $platform = $platformOverride;
        }

        $this->platform = $platform;

        return $this->platform;
    }

    protected function resetCountCache(): static
    {
        $this->count     = null;
        $this->countHash = null;

        return $this;
    }

    protected function closeStatement(): static
    {
        if ($this->statement !== null) {
            if (method_exists($this->statement, 'closeCursor')) {
                $this->statement->closeCursor();
            }

            $this->statement = null;
        }

        return $this;
    }

    protected function hasStatement(): bool
    {
        return $this->statement !== null;
    }

    public function getConnection(): Connection
    {
        return $this->connection;
    }

    public function setSql(string $sql): static
    {
        if (strcmp($this->sql, $sql) !== 0) {
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
    public function setCountSql(string|null $sql): static
    {
        $this->countSql = $sql;

        return $this;
    }

    public function getCountSql(): string|null
    {
        $countSql = $this->countSql;
        if ($countSql === null) {
            $sql = $this->getExtendedSql();
            if ($sql) {
                $countSql = 'SELECT COUNT(*) FROM (' . PHP_EOL . $sql . PHP_EOL . ') raw_cnt_query';
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

        $opt       = $this->getOptions();
        $formatter = new Tools\SqlFormatter($opt['sql_formatter'] ?? []);

        return $formatter->formatSqlParts($sql, $this->sqlEx);
    }

    protected function getExecuteSql(): string
    {
        $sql = $this->getExtendedSql();
        if ($this->maxResults !== null) {
            $sql = $this->getDatabasePlatform()->modifyLimitQuery(
                $sql,
                $this->maxResults,
                $this->firstResult ?? 0,
            );
        }

        return $sql;
    }

    public function setParameter(int|string $key, mixed $value, ParameterType|ArrayParameterType|string|int|null $type = null): static
    {
        if ($type !== null) {
            $this->paramTypes[$key] = $type;
        }

        $this->params[$key] = $value;

        return $this;
    }

    /**
     * @phpstan-param array<int|string, mixed>  $params
     * @phpstan-param WrapperParameterTypeArray $types
     */
    public function setParameters(array $params, array $types = []): static
    {
        $this->paramTypes = $types;
        $this->params     = $params;

        return $this;
    }

    /** @phpstan-return array<int|string, mixed> */
    public function getParameters(): array
    {
        return $this->params;
    }

    public function getParameter(string|int $key): mixed
    {
        return $this->params[$key] ?? null;
    }

    /** @phpstan-return WrapperParameterTypeArray */
    public function getParameterTypes(): array
    {
        return $this->paramTypes;
    }

    public function getParameterType(string|int $key): mixed
    {
        return $this->paramTypes[$key] ?? null;
    }

    public function setFirstResult(int|null $firstResult): static
    {
        $this->closeStatement();
        $this->firstResult = $firstResult;

        return $this;
    }

    public function getFirstResult(): int|null
    {
        return $this->firstResult;
    }

    public function setMaxResults(int|null $maxResults): static
    {
        $this->closeStatement();
        $this->maxResults = $maxResults;

        return $this;
    }

    public function getMaxResults(): int|null
    {
        return $this->maxResults;
    }

    /**
     * @phpstan-param array<int|string, mixed>|null $params
     * @phpstan-param WrapperParameterTypeArray|null $types
     */
    protected function doExecute(string $sql, array|null $params = null, array|null $types = null): DBALResult|null
    {
        if (! is_array($params)) {
            $params = $this->params;
        }

        if (! is_array($types)) {
            $types = $this->paramTypes;
        }

        // Override parameters
        $params = array_replace($this->params, $params);
        $types  = array_replace($this->paramTypes, $types);

        if (count($params) !== count($this->params)) {
            $this->close();
        } else {
            $diffKeys = array_merge(
                array_diff_key($params, $this->params),
                array_diff_key($this->params, $params),
            );
            if (count($diffKeys) > 0) {
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

        if (! $this->statement) {
            /** @phpstan-ignore argument.type,argument.type */
            $this->statement = $this->connection->executeQuery($sql, $params, $types);
        }

        return $this->statement;
    }

    /** @return Traversable<array-key, T> */
    abstract public function toIterable(): Traversable;

    /** @return T[] */
    public function getArrayResult(): array
    {
        $itemNormalizer                   = $this->getItemNormalizer();
        $this->options['item_normalizer'] = null;
        try {
            $result = iterator_to_array($this->toIterable());
        } finally {
            $this->options['item_normalizer'] = $itemNormalizer;
        }

        return $result;
    }

    /** @return T[] */
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
     * @phpstan-return T
     *
     * @throws Exception\NonUniqueResultException if the query result is not unique.
     * @throws Exception\NoResultException        if the query returned no result.
     */
    public function getSingleResult(): mixed
    {
        $result = $this->getResult();

        if (! $result) {
            throw new Exception\NoResultException();
        }

        if (count($result) > 1) {
            throw new Exception\NonUniqueResultException();
        }

        return array_shift($result);
    }

    /**
     * Get exactly one result or null.
     *
     * @phpstan-return T|null
     *
     * @throws Exception\NonUniqueResultException
     */
    public function getOneOrNullResult(): mixed
    {
        $result = $this->getResult();

        if (! $result) {
            return null;
        }

        if (count($result) > 1) {
            throw new Exception\NonUniqueResultException();
        }

        return array_shift($result);
    }

    /**
     * Get the query TOTAL row count. No limits or offsets used.
     *
     * @phpstan-return int<0, max>
     */
    public function getCount(): int
    {
        $countSql = $this->getCountSql();
        if ($countSql === null || $countSql === '') {
            return 0;
        }

        $useCountCache = $this->options['use_count_cache'] ?? true;

        if ($useCountCache) {
            $countHash = sha1($countSql);
            if ($this->count !== null && $this->countHash !== null && $this->countHash !== '' && strcasecmp($countHash, $this->countHash) === 0) {
                // The count has been already fetched and the SQL was not modified
                return $this->count;
            }

            $this->resetCountCache();
            $this->countHash = $countHash;
        }

        $maxResults  = $this->getMaxResults();
        $firstResult = $this->getFirstResult();
        try {
            $this
                ->setMaxResults(null)
                ->setFirstResult(null);
            $count = (int) array_sum(array_map('current', $this->doExecute($countSql)?->fetchAllAssociative() ?? []));
            $this->closeStatement();
        } finally {
            $this
                ->setMaxResults($maxResults)
                ->setFirstResult($firstResult);
        }

        $this->count = max(0, $count);

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
        $this->sqlEx     = clone $this->sqlEx;
        $this->statement = null;
        $this->countHash = null;
    }
}
