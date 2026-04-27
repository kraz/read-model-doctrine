<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Doctrine\ORM\NativeQuery;

class RawNativeQuery extends AbstractRawQuery
{
    private NativeQuery $query;
    private string $paramsHash;

    public function __construct(NativeQuery $query, array $options = [])
    {
        parent::__construct($query->getEntityManager()->getConnection(), $options);

        $this->query = $query;
        $this->setSql($query->getSQL());

        $params = [];
        $types = [];
        foreach ($query->getParameters() as $param) {
            $params[$param->getName()] = $param->getValue();
            $types[$param->getName()] = $param->getType();
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
        if (!$data) {
            return '';
        }

        if (\is_array($data)) {
            $data = array_replace([], $data);
            ksort($data);
        }

        return sha1(json_encode($data));
    }

    protected function updateQuery(?string $sql, ?array $params, ?array $types): static
    {
        if (null !== $sql) {
            $this->query->setSql($sql);
        }

        $nativeParams = [];
        foreach ($this->query->getParameters() as $param) {
            $nativeParams[$param->getName()] = $param->getValue();
        }

        $nativeParamsHash = $this->createHash($nativeParams);
        $nativeParamsChanged = 0 !== strcasecmp($nativeParamsHash, $this->paramsHash);

        if (!$nativeParamsChanged && \is_array($params) && 0 === \count($params)) {
            $this->query->getParameters()->clear();
        }

        if (\is_array($params)) {
            if ($nativeParamsChanged) {
                $params = array_replace($params, $nativeParams);
            }
            $nativeParams = [];
            foreach ($params as $paramName => $paramValue) {
                $paramType = \is_array($types) && \array_key_exists($paramName, $types) ? $types[$paramName] : null;
                $this->query->setParameter($paramName, $paramValue, $paramType);
                $nativeParams[$paramName] = $paramValue;
            }
        }

        $this->paramsHash = $this->createHash($nativeParams);

        return $this;
    }

    public function toIterable(): \Traversable
    {
        $sql = $this->getExecuteSql();
        $params = $this->getParameters();
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
