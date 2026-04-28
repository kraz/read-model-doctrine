<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\ORM\NativeQuery;
use Traversable;

use function array_key_exists;
use function array_replace;
use function count;
use function is_array;
use function json_encode;
use function ksort;
use function sha1;
use function strcasecmp;

/**
 * @phpstan-import-type WrapperParameterTypeArray from Connection
 * @phpstan-import-type AbstractRawQueryOptionsWrapper from AbstractRawQuery
 * @phpstan-template-covariant T of object|array<string, mixed>
 * @phpstan-extends AbstractRawQuery<T>
 */
class RawNativeQuery extends AbstractRawQuery
{
    private string $paramsHash;

    /** @phpstan-param AbstractRawQueryOptionsWrapper $options */
    public function __construct(private NativeQuery $query, array $options = [])
    {
        parent::__construct($query->getEntityManager()->getConnection(), $options);

        $this->setSql($query->getSQL());

        $params = [];
        $types  = [];
        foreach ($query->getParameters() as $param) {
            $params[$param->getName()] = $param->getValue();
            $types[$param->getName()]  = $param->getType();
        }

        $this->paramsHash = $this->createHash($params);
        $this->setParameters($params, $types);
    }

    public function getNativeQuery(): NativeQuery
    {
        return $this->query;
    }

    protected function createHash(mixed $data): string
    {
        if (! $data) {
            return '';
        }

        if (is_array($data)) {
            $data = array_replace([], $data);
            ksort($data);
        }

        return sha1(json_encode($data) ?: '');
    }

    /**
     * @phpstan-param array<int|string, mixed>|null $params
     * @phpstan-param array<string, ParameterType|ArrayParameterType|string|int|null>|null $types
     */
    protected function updateQuery(string|null $sql, array|null $params, array|null $types): static
    {
        if ($sql !== null) {
            $this->query->setSql($sql);
        }

        $nativeParams = [];
        foreach ($this->query->getParameters() as $param) {
            $nativeParams[$param->getName()] = $param->getValue();
        }

        $nativeParamsHash    = $this->createHash($nativeParams);
        $nativeParamsChanged = strcasecmp($nativeParamsHash, $this->paramsHash) !== 0;

        if (! $nativeParamsChanged && is_array($params) && count($params) === 0) {
            $this->query->getParameters()->clear();
        }

        if (is_array($params)) {
            if ($nativeParamsChanged) {
                $params = array_replace($params, $nativeParams);
            }

            $nativeParams = [];
            foreach ($params as $paramName => $paramValue) {
                $paramType = is_array($types) && array_key_exists($paramName, $types) ? $types[$paramName] : null;
                $this->query->setParameter($paramName, $paramValue, $paramType);
                $nativeParams[$paramName] = $paramValue;
            }
        }

        $this->paramsHash = $this->createHash($nativeParams);

        return $this;
    }

    /** @phpstan-return Traversable<array-key, T> */
    public function toIterable(): Traversable
    {
        $sql    = $this->getExecuteSql();
        $params = $this->getParameters();
        /** @phpstan-var array<string, ParameterType|ArrayParameterType|string|int|null> $types */
        $types = $this->getParameterTypes();

        $this->updateQuery($sql, $params, $types);

        $itemNormalizer = $this->getItemNormalizer();

        if ($itemNormalizer) {
            foreach ($this->query->toIterable() as $item) {
                $item = $itemNormalizer($item);

                yield $item;
            }
        } else {
            yield from $this->query->toIterable();
        }
    }
}
