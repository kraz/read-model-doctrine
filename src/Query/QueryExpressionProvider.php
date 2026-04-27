<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Doctrine\ORM\QueryBuilder;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Override;

use function is_string;

/** @phpstan-import-type QueryExpressionHelperOptions from QueryExpressionHelper */
class QueryExpressionProvider implements QueryExpressionProviderInterface
{
    public function __construct(
        private ReadModelDescriptorFactoryInterface $descriptorFactory,
    ) {
    }

    /**
     * @phpstan-param QueryBuilder|AbstractRawQuery<T> $data
     * @phpstan-param QueryExpressionHelperOptions $options
     *
     * @phpstan-return ($data is QueryBuilder ? QueryBuilder : AbstractRawQuery<T>)
     *
     * @template T of object|array<string, mixed>
     */
    #[Override]
    public function apply(mixed $data, QueryExpression $queryExpression, ReadModelDescriptor|null $descriptor = null, array $options = [], int $includeData = self::INCLUDE_DATA_ALL): QueryBuilder|AbstractRawQuery
    {
        $optDescriptor = $options['read_model_descriptor'] ?? null;
        if ($descriptor === null && is_string($optDescriptor)) {
            $optDescriptor = $this->descriptorFactory->createReadModelDescriptorFrom($optDescriptor);
        }

        if ($descriptor === null && $optDescriptor instanceof ReadModelDescriptor) {
            $descriptor = $optDescriptor;
        }

        if ($descriptor === null && $data instanceof QueryBuilder) {
            $descriptor = $this->descriptorFactory->createReadModelDescriptorFrom($data);
        }

        /** @phpstan-var QueryExpressionHelper<T> $helper */
        $helper = QueryExpressionHelper::create($data, $descriptor, $options);

        return $helper->apply($queryExpression, $includeData);
    }
}
