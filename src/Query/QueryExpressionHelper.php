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
use Webmozart\Assert\Assert;

/**
 * @psalm-type QueryExpressionHelperOptions = array{
 *     root_identifier?: string,
 *     root_alias?: string|string[],
 *     quoteTableAlias?: bool,
 *     quoteFieldNames?: bool,
 *     quoteFieldNamesChar?: string,
 *     read_model_descriptor: ReadModelDescriptor|string|null,
 *     field_map?: array<string, string>,
 *     expressions?: array<string, array{exp?: mixed}>,
 *     groups?: array<string, array{
 *         logic?: string,
 *         fields?: array<array-key, string>,
 *         filter?: array<string, mixed>,
 *         filters?: array<string, mixed>,
 *     }>
 * }
 */
final class QueryExpressionHelper
{
    /** @noinspection SpellCheckingInspection */
    private static array $operatorsNoValue = [
        'isnull',
        'isnotnull',
        'isempty',
        'isnullorempty',
        'isnotempty',
        'isnotnullorempty',
    ];

    /**
     * @psalm-var QueryExpressionHelperOptions
     */
    private array $options;

    /**
     * @param QueryBuilder|AbstractRawQuery $data
     * @param ReadModelDescriptor|null $descriptor
     * @param QueryExpressionHelperOptions $options
     */
    private function __construct(
        private readonly QueryBuilder|AbstractRawQuery $data,
        private ?ReadModelDescriptor $descriptor,
        array $options = [],
    ) {
        $this->options = $options;
        if (null === $this->descriptor) {
            $readModelDescriptor = $this->options['read_model_descriptor'] ?? null;
            if ($readModelDescriptor instanceof ReadModelDescriptor) {
                $this->descriptor = $readModelDescriptor;
            }
        }
    }

    private function getFieldMap(): array
    {
        return $this->options['field_map'] ?? $this->descriptor?->fieldMap ?? [];
    }

    /**
     * @return string[]
     */
    private function getRootAliasList(): array
    {
        if ($this->data instanceof QueryBuilder) {
            return $this->data->getRootAliases();
        }

        $rootAlias = $this->options['root_alias'] ?? null;

        return \is_string($rootAlias) ? [$rootAlias] : [];
    }

    private function getRootIdentifier(): string
    {
        $rootIdentifier = $this->options['root_identifier'] ?? null;
        if (\is_string($rootIdentifier)) {
            $rootIdentifier = [$rootIdentifier];
        }

        if (!\is_array($rootIdentifier) && $this->data instanceof QueryBuilder) {
            $rootEntity = $this->data->getRootEntities();
            $rootEntity = reset($rootEntity);
            Assert::stringNotEmpty($rootEntity);
            Assert::classExists($rootEntity);
            $rootMetaData = $this->data->getEntityManager()->getClassMetadata($rootEntity);
            $rootIdentifier = $rootMetaData->getIdentifierFieldNames();
        }

        if (!\is_array($rootIdentifier)) {
            throw new \RuntimeException('Can not determine the root identifier. Did you missed the "root_identifier" option?');
        }

        if (\count($rootIdentifier) > 1) {
            throw new \RuntimeException('Composite root identifiers are not supported.');
        }

        $rootIdentifier = reset($rootIdentifier);

        Assert::stringNotEmpty($rootIdentifier, 'Can not determine the "root_identifier"!');

        if (str_contains($rootIdentifier, '.')) {
            throw new \RuntimeException('The "root_identifier" option must not contain "." symbol. Please use "root_alias" to specify the alias of the table which holds the identifier column!');
        }

        return $rootIdentifier;
    }

    private function isQuoted(string $str): bool
    {
        $quoteChar = $this->options['quoteFieldNamesChar'] ?? '"';
        if (!\is_string($quoteChar) || 1 !== mb_strlen($quoteChar)) {
            return false;
        }
        $s = mb_trim($str);

        return str_starts_with($s, $quoteChar) && false !== mb_strrpos($s, $quoteChar, -1);
    }

    private function quote(string $str): string
    {
        $quoteChar = $this->options['quoteFieldNamesChar'] ?? '"';
        if (!\is_string($quoteChar) || 1 !== mb_strlen($quoteChar)) {
            return $str;
        }

        return $quoteChar.str_replace($quoteChar, $quoteChar.$quoteChar, $str).$quoteChar;
    }

    //    private function unquote(string $str): string
    //    {
    //        $quoteChar = $this->options['quoteFieldNamesChar'] ?? '"';
    //        if (!\is_string($quoteChar) || 1 !== mb_strlen($quoteChar) || !$this->isQuoted($str)) {
    //            return $str;
    //        }
    //
    //        return str_replace($quoteChar, '', $str);
    //    }

    private function mapField(string $field): string
    {
        $fieldMap = $this->getFieldMap();

        $field = $fieldMap[$field] ?? $field;

        $rootAliasList = $this->getRootAliasList();

        $alias = null;
        $column = $field;
        if (str_contains($column, '.')) {
            $parts = explode('.', $column);
            $alias = array_shift($parts);
            if (!in_array($alias, $rootAliasList, true)) {
                array_unshift($parts, $alias);
                $alias = null;
            }
            $column = implode('.', $parts);
        }

        if (null === $alias) {
            $alias = $rootAliasList[0] ?? null;
        }

        $quoteFieldNames = true === ($this->options['quoteFieldNames'] ?? null);
        if ($quoteFieldNames && !$this->isQuoted($column)) {
            $column = $this->quote($column);
        }

        $quoteTableAlias = true === ($this->options['quoteTableAlias'] ?? null);
        if ($quoteTableAlias && null !== $alias) {
            $alias = $this->quote($alias);
        }

        return (null !== $alias ? $alias.'.' : '').$column;
    }

    private function getDatabasePlatform(): ?AbstractPlatform
    {
        if ($this->data instanceof QueryBuilder) {
            return $this->data->getEntityManager()->getConnection()->getDatabasePlatform();
        }

        if ($this->data instanceof AbstractRawQuery) {
            return $this->data->getConnection()->getDatabasePlatform();
        }

        return null;
    }

    private function createQueryFilterExpression(Expr $expr, FilterExpression|array $filter, array &$params): string|Comparison|Andx|Orx|Func
    {
        if ($filter instanceof FilterExpression) {
            $filter = $filter->toArray();
        }

        static $paramId = 0;

        $ignoreCaseDefault = !isset($filter['ignoreCase']);
        $ignoreCase = !isset($filter['ignoreCase']) || (bool) $filter['ignoreCase'];

        if (!isset($filter['field'])) {
            throw new \RuntimeException('Missing filter filed');
        }

        $field = $this->mapField((string) $filter['field']);

        if (!isset($filter['operator'])) {
            throw new \RuntimeException('Missing filter operator');
        }
        $operator = mb_strtolower((string) $filter['operator']);

        $paramName = 'pvalue'.($paramId++);
        $paramValue = null;
        $paramValueUpper = null;

        if (!\in_array($operator, self::$operatorsNoValue, true)) {
            if (!isset($filter['value'])) {
                throw new \RuntimeException('Missing filter value');
            }
            $paramValue = $filter['value'];
            $paramValueUpper = $ignoreCase && is_scalar($paramValue) ? mb_strtoupper((string) $paramValue, 'UTF-8') : $paramValue;
        }

        $fieldEx = $field;
        if (\array_key_exists($field, $this->options['expressions'] ?? [])) {
            $fieldEx = $this->options['expressions'][$field]['exp'] ?? $field;
        }

        switch ($operator) {
            case 'eq':
                if (!$ignoreCaseDefault && $ignoreCase) {
                    $params[$paramName] = $paramValueUpper;

                    return $expr->eq($expr->upper($fieldEx), ':'.$paramName);
                }
                $params[$paramName] = $paramValue;

                return $expr->eq($fieldEx, ':'.$paramName);
            case 'neq':
                if (!$ignoreCaseDefault && $ignoreCase) {
                    $params[$paramName] = $paramValueUpper;

                    return $expr->neq($expr->upper($fieldEx), ':'.$paramName);
                }
                $params[$paramName] = $paramValue;

                return $expr->neq($fieldEx, ':'.$paramName);
            case 'isnull':
                return $expr->isNull($fieldEx);
            case 'isnotnull':
                return $expr->isNotNull($fieldEx);
            case 'lt':
                $params[$paramName] = $paramValue;

                return $expr->lt($fieldEx, ':'.$paramName);
            case 'lte':
                $params[$paramName] = $paramValue;

                return $expr->lte($fieldEx, ':'.$paramName);
            case 'gt':
                $params[$paramName] = $paramValue;

                return $expr->gt($fieldEx, ':'.$paramName);
            case 'gte':
                $params[$paramName] = $paramValue;

                return $expr->gte($fieldEx, ':'.$paramName);
            case 'startswith':
            case 'notstartswith':
            case 'doesnotstartwith':
                if ($ignoreCase) {
                    $params[$paramName] = $paramValueUpper.'%';

                    return match ($operator) {
                        'startswith' => $expr->like((string) $expr->upper($fieldEx), ':'.$paramName),
                        'notstartswith', 'doesnotstartwith' => $expr->notLike((string) $expr->upper($fieldEx), ':'.$paramName),
                    };
                }
                $params[$paramName] = $paramValue.'%';

                return match ($operator) {
                    'startswith' => $expr->like($fieldEx, ':' . $paramName),
                    'notstartswith', 'doesnotstartwith' => $expr->notLike($fieldEx, ':' . $paramName),
                };

            case 'endswith':
            case 'notendswith':
            case 'doesnotendwith':
                if ($ignoreCase) {
                    $params[$paramName] = '%'.$paramValueUpper;

                    return match ($operator) {
                        'endswith' => $expr->like((string) $expr->upper($fieldEx), ':'.$paramName),
                        'notendswith', 'doesnotendwith' => $expr->notLike((string) $expr->upper($fieldEx), ':'.$paramName),
                    };
                }
                $params[$paramName] = '%'.$paramValue;

                return match ($operator) {
                    'endswith' => $expr->like($fieldEx, ':'.$paramName),
                    'notendswith', 'doesnotendwith' => $expr->notLike($fieldEx, ':'.$paramName),
                };
            case 'contains':
            case 'notcontains':
            case 'doesnotcontain':
                if ($ignoreCase) {
                    $params[$paramName] = '%'.$paramValueUpper.'%';

                    return match ($operator) {
                        'contains' => $expr->like((string) $expr->upper($fieldEx), ':'.$paramName),
                        'notcontains', 'doesnotcontain' => $expr->notLike((string) $expr->upper($fieldEx), ':'.$paramName),
                    };
                }
                $params[$paramName] = '%'.$paramValue.'%';

                return match ($operator) {
                    'contains' => $expr->like($fieldEx, ':'.$paramName),
                    'notcontains', 'doesnotcontain' => $expr->notLike($fieldEx, ':'.$paramName),
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
                if (\is_string($paramValue)) {
                    $paramValue = array_map('trim', explode(',', $paramValue));
                }
                Assert::isArray($paramValue);
                if ($ignoreCase) {
                    $paramValue = array_map(static fn ($v) => mb_strtoupper((string) $v, 'UTF-8'), $paramValue);
                    $fieldEx = (string) $expr->upper($fieldEx);
                }
                if (1 === \count($paramValue)) {
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
                throw new \RuntimeException(\sprintf('Unsupported filter operator: "%s"', $operator));
        }
    }

    public function apply(QueryExpression $queryExpression, int $includeData = QueryExpressionProviderInterface::INCLUDE_DATA_ALL): QueryBuilder|AbstractRawQuery
    {
        $data = clone $this->data;
        $queryParts = null;
        $params = [];
        $includeFilter = $includeData & QueryExpressionProviderInterface::INCLUDE_DATA_FILTER;
        $includeSort = $includeData & QueryExpressionProviderInterface::INCLUDE_DATA_SORT;
        $includeValues = $includeData & QueryExpressionProviderInterface::INCLUDE_DATA_VALUES;

        $filter = $queryExpression->getFilter();
        if ($includeFilter && null !== $filter && !$filter->isFilterEmpty()) {
            $where = FilterExpression::normalize(new Expr(), $filter, $params, $this->createQueryFilterExpression(...), $this->options);
            if ($where) {
                $queryParts ??= new QueryParts();
                $queryParts->andWhere($where);
            }
        }

        $sort = $queryExpression->getSort();
        if ($includeSort && null !== $sort && !$sort->isSortEmpty()) {
            foreach ($sort->items() as $entry) {
                $field = $entry['field'] ?? null;
                if (null === $field || '' === $field) {
                    throw new \RuntimeException('The sort rule must specify a field');
                }
                if (\array_key_exists($field, $this->options['expressions'] ?? [])) {
                    $field = $this->options['expressions'][$field]['exp'] ?? $field;
                } else {
                    $field = $this->mapField($field);
                }
                $dir = $entry['dir'] ?? 'ASC';
                if (!\in_array(mb_strtoupper($dir), ['ASC', 'DESC'], true)) {
                    throw new \RuntimeException(\sprintf('Invalid sort direction: "%s"', $dir));
                }
                $queryParts ??= new QueryParts();
                $queryParts->addOrderBy($field, $dir);
            }
        }

        $values = $queryExpression->getValues();
        if ($includeValues && null !== $values) {
            $field = $this->getRootIdentifier();
            $field = $this->mapField($field);
            $queryParts ??= new QueryParts();
            if (count($values) === 1) {
                $params['pValueIdent'] = reset($values);
                $queryParts->andWhere(new Expr()->eq($field, ':pValueIdent'));
            } else {
                $queryParts->andWhere(new Expr()->in($field, $values));
            }
        }

        if ($data instanceof QueryBuilder) {
            if (null === $queryParts) {
                return $data;
            }
            $queryParts->addTo($data);
        }

        if ($data instanceof AbstractRawQuery) {
            if (null === $queryParts) {
                return $data;
            }
            $queryParts->addTo($data->sql());
        }

        foreach ($params as $paramName => $paramValue) {
            $data->setParameter($paramName, $paramValue);
        }
        return $data;
    }

    public static function create(QueryBuilder|AbstractRawQuery $data, ?ReadModelDescriptor $descriptor = null, array $options = []): QueryExpressionHelper
    {
        return new QueryExpressionHelper($data, $descriptor, $options);
    }
}
