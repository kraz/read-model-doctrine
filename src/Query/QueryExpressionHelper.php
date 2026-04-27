<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\ORM\Query\Expr;
use Doctrine\ORM\Query\Expr\Andx;
use Doctrine\ORM\Query\Expr\Comparison;
use Doctrine\ORM\Query\Expr\Func;
use Doctrine\ORM\Query\Expr\Orx;
use Doctrine\ORM\QueryBuilder;
use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModelDoctrine\Tools\QueryParts;
use RuntimeException;
use Webmozart\Assert\Assert;

use function array_key_exists;
use function array_map;
use function array_shift;
use function array_unshift;
use function count;
use function explode;
use function implode;
use function in_array;
use function is_array;
use function is_scalar;
use function is_string;
use function mb_strlen;
use function mb_strrpos;
use function mb_strtolower;
use function mb_strtoupper;
use function mb_trim;
use function reset;
use function sprintf;
use function str_contains;
use function str_replace;
use function str_starts_with;

/**
 * @phpstan-import-type FilterCompositeArrayItems from FilterExpression
 * @phpstan-type QueryExpressionHelperOptions = array{
 *     root_identifier?: string,
 *     root_alias?: string|string[],
 *     quoteTableAlias?: bool,
 *     quoteFieldNames?: bool,
 *     quoteFieldNamesChar?: string,
 *     read_model_descriptor?: ReadModelDescriptor|class-string|null,
 *     field_map?: array<string, string>,
 *     expressions?: array<string, array{exp?: mixed}>,
 *     groups?: array<string, array{
 *         logic?: string,
 *         fields?: array<array-key, string>,
 *         filter?: array<string, mixed>,
 *         filters?: array<string, mixed>,
 *     }>
 * }
 * @template T of object|array<string, mixed>
 */
final class QueryExpressionHelper
{
    /** @phpstan-var list<string> */
    private static array $operatorsNoValue = [
        'isnull',
        'isnotnull',
        'isempty',
        'isnullorempty',
        'isnotempty',
        'isnotnullorempty',
    ];

    /**
     * @phpstan-param QueryBuilder|AbstractRawQuery<T> $data
     * @phpstan-param QueryExpressionHelperOptions $options
     */
    private function __construct(
        private readonly QueryBuilder|AbstractRawQuery $data,
        private ReadModelDescriptor|null $descriptor,
        private array $options = [],
    ) {
        if ($this->descriptor !== null) {
            return;
        }

        $readModelDescriptor = $this->options['read_model_descriptor'] ?? null;
        if (! ($readModelDescriptor instanceof ReadModelDescriptor)) {
            return;
        }

        $this->descriptor = $readModelDescriptor;
    }

    /** @phpstan-return array<string, string> */
    private function getFieldMap(): array
    {
        return $this->options['field_map'] ?? $this->descriptor->fieldMap ?? [];
    }

    /** @return string[] */
    private function getRootAliasList(): array
    {
        if ($this->data instanceof QueryBuilder) {
            return $this->data->getRootAliases();
        }

        $rootAlias = $this->options['root_alias'] ?? null;

        return is_string($rootAlias) ? [$rootAlias] : [];
    }

    private function getRootIdentifier(): string
    {
        $rootIdentifier = $this->options['root_identifier'] ?? null;
        if (is_string($rootIdentifier)) {
            $rootIdentifier = [$rootIdentifier];
        }

        if (! is_array($rootIdentifier) && $this->data instanceof QueryBuilder) {
            $rootEntity = $this->data->getRootEntities();
            $rootEntity = reset($rootEntity);
            Assert::stringNotEmpty($rootEntity);
            Assert::classExists($rootEntity);
            $rootMetaData   = $this->data->getEntityManager()->getClassMetadata($rootEntity);
            $rootIdentifier = $rootMetaData->getIdentifierFieldNames();
        }

        if (! is_array($rootIdentifier)) {
            throw new RuntimeException('Can not determine the root identifier. Did you missed the "root_identifier" option?');
        }

        if (count($rootIdentifier) > 1) {
            throw new RuntimeException('Composite root identifiers are not supported.');
        }

        $rootIdentifier = reset($rootIdentifier);

        Assert::stringNotEmpty($rootIdentifier, 'Can not determine the "root_identifier"!');

        if (str_contains($rootIdentifier, '.')) {
            throw new RuntimeException('The "root_identifier" option must not contain "." symbol. Please use "root_alias" to specify the alias of the table which holds the identifier column!');
        }

        return $rootIdentifier;
    }

    private function isQuoted(string $str): bool
    {
        $quoteChar = $this->options['quoteFieldNamesChar'] ?? '"';
        if (! is_string($quoteChar) || mb_strlen($quoteChar) !== 1) {
            return false;
        }

        $s = mb_trim($str);

        return str_starts_with($s, $quoteChar) && mb_strrpos($s, $quoteChar, -1) !== false;
    }

    private function quote(string $str): string
    {
        $quoteChar = $this->options['quoteFieldNamesChar'] ?? '"';
        if (! is_string($quoteChar) || mb_strlen($quoteChar) !== 1) {
            return $str;
        }

        return $quoteChar . str_replace($quoteChar, $quoteChar . $quoteChar, $str) . $quoteChar;
    }

    private function mapField(string $field): string
    {
        $fieldMap = $this->getFieldMap();

        $field = $fieldMap[$field] ?? $field;

        $rootAliasList = $this->getRootAliasList();

        $alias  = null;
        $column = $field;
        if (str_contains($column, '.')) {
            $parts = explode('.', $column);
            $alias = array_shift($parts);
            if (! in_array($alias, $rootAliasList, true)) {
                array_unshift($parts, $alias);
                $alias = null;
            }

            $column = implode('.', $parts);
        }

        if ($alias === null) {
            $alias = $rootAliasList[0] ?? null;
        }

        $quoteFieldNames = ($this->options['quoteFieldNames'] ?? null) === true;
        if ($quoteFieldNames && ! $this->isQuoted($column)) {
            $column = $this->quote($column);
        }

        $quoteTableAlias = ($this->options['quoteTableAlias'] ?? null) === true;
        if ($quoteTableAlias && $alias !== null) {
            $alias = $this->quote($alias);
        }

        return ($alias !== null ? $alias . '.' : '') . $column;
    }

    private function getDatabasePlatform(): AbstractPlatform
    {
        if ($this->data instanceof QueryBuilder) {
            return $this->data->getEntityManager()->getConnection()->getDatabasePlatform();
        }

        return $this->data->getConnection()->getDatabasePlatform();
    }

    /**
     * @phpstan-param FilterExpression|FilterCompositeArrayItems $filter
     * @phpstan-param array<string, mixed> $params
     */
    private function createQueryFilterExpression(Expr $expr, FilterExpression|array $filter, array &$params): string|Comparison|Andx|Orx|Func
    {
        if ($filter instanceof FilterExpression) {
            $filter = $filter->toArray();
        }

        static $paramId = 0;

        $ignoreCaseDefault = ! isset($filter['ignoreCase']);
        $ignoreCase        = ! isset($filter['ignoreCase']) || (bool) $filter['ignoreCase'];

        if (! isset($filter['field'])) {
            throw new RuntimeException('Missing filter filed');
        }

        $field = $this->mapField((string) $filter['field']);

        if (! isset($filter['operator'])) {
            throw new RuntimeException('Missing filter operator');
        }

        $operator = mb_strtolower((string) $filter['operator']);

        $paramName       = 'pvalue' . ($paramId++);
        $paramValue      = null;
        $paramValueUpper = null;

        if (! in_array($operator, self::$operatorsNoValue, true)) {
            if (! isset($filter['value'])) {
                throw new RuntimeException('Missing filter value');
            }

            $paramValue      = $filter['value'];
            $paramValueUpper = $ignoreCase && is_scalar($paramValue) ? mb_strtoupper((string) $paramValue, 'UTF-8') : $paramValue;
        }

        $fieldEx = $field;
        if (array_key_exists($field, $this->options['expressions'] ?? [])) {
            $fieldEx = $this->options['expressions'][$field]['exp'] ?? $field;
        }

        switch ($operator) {
            case 'eq':
                if (! $ignoreCaseDefault && $ignoreCase) {
                    $params[$paramName] = $paramValueUpper;

                    return $expr->eq($expr->upper($fieldEx), ':' . $paramName);
                }

                $params[$paramName] = $paramValue;

                return $expr->eq($fieldEx, ':' . $paramName);

            case 'neq':
                if (! $ignoreCaseDefault && $ignoreCase) {
                    $params[$paramName] = $paramValueUpper;

                    return $expr->neq($expr->upper($fieldEx), ':' . $paramName);
                }

                $params[$paramName] = $paramValue;

                return $expr->neq($fieldEx, ':' . $paramName);

            case 'isnull':
                return $expr->isNull($fieldEx);

            case 'isnotnull':
                return $expr->isNotNull($fieldEx);

            case 'lt':
                $params[$paramName] = $paramValue;

                return $expr->lt($fieldEx, ':' . $paramName);

            case 'lte':
                $params[$paramName] = $paramValue;

                return $expr->lte($fieldEx, ':' . $paramName);

            case 'gt':
                $params[$paramName] = $paramValue;

                return $expr->gt($fieldEx, ':' . $paramName);

            case 'gte':
                $params[$paramName] = $paramValue;

                return $expr->gte($fieldEx, ':' . $paramName);

            case 'startswith':
            case 'notstartswith':
            case 'doesnotstartwith':
                if ($ignoreCase) {
                    $params[$paramName] = $paramValueUpper . '%';

                    return match ($operator) {
                        'startswith' => $expr->like((string) $expr->upper($fieldEx), ':' . $paramName),
                        'notstartswith', 'doesnotstartwith' => $expr->notLike((string) $expr->upper($fieldEx), ':' . $paramName),
                    };
                }

                $params[$paramName] = $paramValue . '%';

                return match ($operator) {
                    'startswith' => $expr->like($fieldEx, ':' . $paramName),
                    'notstartswith', 'doesnotstartwith' => $expr->notLike($fieldEx, ':' . $paramName),
                };

            case 'endswith':
            case 'notendswith':
            case 'doesnotendwith':
                if ($ignoreCase) {
                    $params[$paramName] = '%' . $paramValueUpper;

                    return match ($operator) {
                        'endswith' => $expr->like((string) $expr->upper($fieldEx), ':' . $paramName),
                        'notendswith', 'doesnotendwith' => $expr->notLike((string) $expr->upper($fieldEx), ':' . $paramName),
                    };
                }

                $params[$paramName] = '%' . $paramValue;

                return match ($operator) {
                    'endswith' => $expr->like($fieldEx, ':' . $paramName),
                    'notendswith', 'doesnotendwith' => $expr->notLike($fieldEx, ':' . $paramName),
                };

            case 'contains':
            case 'notcontains':
            case 'doesnotcontain':
                if ($ignoreCase) {
                    $params[$paramName] = '%' . $paramValueUpper . '%';

                    return match ($operator) {
                        'contains' => $expr->like((string) $expr->upper($fieldEx), ':' . $paramName),
                        'notcontains', 'doesnotcontain' => $expr->notLike((string) $expr->upper($fieldEx), ':' . $paramName),
                    };
                }

                $params[$paramName] = '%' . $paramValue . '%';

                return match ($operator) {
                    'contains' => $expr->like($fieldEx, ':' . $paramName),
                    'notcontains', 'doesnotcontain' => $expr->notLike($fieldEx, ':' . $paramName),
                };

            case 'isempty':
            case 'isnullorempty':
            case 'isnotempty':
            case 'isnotnullorempty':
                $databasePlatform = $this->getDatabasePlatform();
                if ($databasePlatform instanceof OraclePlatform) {
                    return match ($operator) {
                        'isempty', 'isnullorempty' => $expr->isNull($fieldEx),
                        'isnotempty', 'isnotnullorempty' => $expr->isNotNull($fieldEx),
                    };
                }

                return match ($operator) {
                    'isempty', 'isnullorempty' => $expr->orX($expr->isNull($fieldEx), $expr->eq($fieldEx, $expr->literal(''))),
                    'isnotempty', 'isnotnullorempty' => $expr->andX($expr->isNotNull($fieldEx), $expr->neq($fieldEx, $expr->literal(''))),
                };

            case 'inlist':
            case 'notinlist':
                if (is_string($paramValue)) {
                    $paramValue = array_map('trim', explode(',', $paramValue));
                }

                Assert::isArray($paramValue);
                if ($ignoreCase) {
                    $paramValue = array_map(static fn ($v) => mb_strtoupper((string) $v, 'UTF-8'), $paramValue);
                    $fieldEx    = (string) $expr->upper($fieldEx);
                }

                if (count($paramValue) === 1) {
                    $params[$paramName] = reset($paramValue);

                    return match ($operator) {
                        'inlist' => $expr->eq($fieldEx, ':' . $paramName),
                        'notinlist' => $expr->neq($fieldEx, ':' . $paramName),
                    };
                }

                return match ($operator) {
                    'inlist' => $expr->in($fieldEx, $paramValue),
                    'notinlist' => $expr->notIn($fieldEx, $paramValue),
                };

            default:
                throw new RuntimeException(sprintf('Unsupported filter operator: "%s"', $operator));
        }
    }

    /** @phpstan-return QueryBuilder|AbstractRawQuery<T> */
    public function apply(QueryExpression $queryExpression, int $includeData = QueryExpressionProviderInterface::INCLUDE_DATA_ALL): QueryBuilder|AbstractRawQuery
    {
        $data          = clone $this->data;
        $queryParts    = null;
        $params        = [];
        $includeFilter = $includeData & QueryExpressionProviderInterface::INCLUDE_DATA_FILTER;
        $includeSort   = $includeData & QueryExpressionProviderInterface::INCLUDE_DATA_SORT;
        $includeValues = $includeData & QueryExpressionProviderInterface::INCLUDE_DATA_VALUES;

        $filter = $queryExpression->getFilter();
        if ($includeFilter && $filter !== null && ! $filter->isFilterEmpty()) {
            $where = FilterExpression::normalize(new Expr(), $filter, $params, $this->createQueryFilterExpression(...), $this->options);
            if ($where) {
                $queryParts = new QueryParts();
                $queryParts->andWhere($where);
            }
        }

        $sort = $queryExpression->getSort();
        if ($includeSort && $sort !== null && ! $sort->isSortEmpty()) {
            foreach ($sort->items() as $entry) {
                $field = $entry['field'] ?? null;
                if ($field === null || $field === '') {
                    throw new RuntimeException('The sort rule must specify a field');
                }

                if (array_key_exists($field, $this->options['expressions'] ?? [])) {
                    $field = $this->options['expressions'][$field]['exp'] ?? $field;
                } else {
                    $field = $this->mapField($field);
                }

                $dir = $entry['dir'] ?? 'ASC';
                if (! in_array(mb_strtoupper($dir), ['ASC', 'DESC'], true)) {
                    throw new RuntimeException(sprintf('Invalid sort direction: "%s"', $dir));
                }

                $queryParts ??= new QueryParts();
                $queryParts->addOrderBy($field, $dir);
            }
        }

        $values = $queryExpression->getValues();
        if ($includeValues && $values !== null) {
            $field        = $this->getRootIdentifier();
            $field        = $this->mapField($field);
            $queryParts ??= new QueryParts();
            if (count($values) === 1) {
                $params['pValueIdent'] = reset($values);
                $queryParts->andWhere(new Expr()->eq($field, ':pValueIdent'));
            } else {
                $queryParts->andWhere(new Expr()->in($field, $values));
            }
        }

        if ($data instanceof QueryBuilder) {
            if ($queryParts === null) {
                return $data;
            }

            $queryParts->addTo($data);
        }

        if ($data instanceof AbstractRawQuery) {
            if ($queryParts === null) {
                return $data;
            }

            $queryParts->addTo($data->sql());
        }

        foreach ($params as $paramName => $paramValue) {
            $data->setParameter($paramName, $paramValue);
        }

        return $data;
    }

    /**
     * @phpstan-param QueryBuilder|AbstractRawQuery<T> $data
     * @phpstan-param QueryExpressionHelperOptions $options
     *
     * @phpstan-return QueryExpressionHelper<T>
     */
    public static function create(QueryBuilder|AbstractRawQuery $data, ReadModelDescriptor|null $descriptor = null, array $options = []): QueryExpressionHelper
    {
        return new QueryExpressionHelper($data, $descriptor, $options);
    }
}
