<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Doctrine\ORM\QueryBuilder;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;

/**
 * @psalm-import-type QueryExpressionHelperOptions from QueryExpressionHelper
 */
class QueryExpressionProvider implements QueryExpressionProviderInterface
{
    public function __construct(
        private ReadModelDescriptorFactoryInterface $descriptorFactory,
    ) {
    }

    /**
     * @phpstan-param QueryBuilder|AbstractRawQuery $data
     * @phpstan-param QueryExpressionHelperOptions $options
     */
    #[\Override]
    public function apply(mixed $data, QueryExpression $queryExpression, ?ReadModelDescriptor $descriptor = null, array $options = [], int $includeData = self::INCLUDE_DATA_ALL): QueryBuilder|AbstractRawQuery
    {
        $optDescriptor = $options['read_model_descriptor'] ?? null;
        if (null === $descriptor && is_string($optDescriptor)) {
            $optDescriptor = $this->descriptorFactory->createReadModelDescriptorFrom($optDescriptor);
        }
        if (null === $descriptor && $optDescriptor instanceof ReadModelDescriptor) {
            $descriptor = $optDescriptor;
        }
        if (null === $descriptor && $data instanceof QueryBuilder) {
            $descriptor = $this->descriptorFactory->createReadModelDescriptorFrom($data);
        }

        return QueryExpressionHelper::create($data, $descriptor, $options)->apply($queryExpression, $includeData);
    }
}
