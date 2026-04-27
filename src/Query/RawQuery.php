<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Query;

use Traversable;

/**
 * @template-covariant T of object|array<string, mixed>
 * @extends AbstractRawQuery<T>
 */
class RawQuery extends AbstractRawQuery
{
    public function close(): static
    {
        return parent::closeStatement();
    }

    /** @phpstan-return Traversable<array-key, T> */
    public function toIterable(): Traversable
    {
        $itemNormalizer = $this->getItemNormalizer();
        $result         = $this->doExecute($this->getExecuteSql());
        try {
            if ($itemNormalizer !== null) {
                foreach ($result?->iterateAssociative() ?? [] as $item) {
                    $item = $itemNormalizer($item);

                    yield $item;
                }
            } else {
                /** @phpstan-var Traversable<array-key, T> $iterable */
                $iterable = $result?->iterateAssociative() ?? [];

                yield from $iterable;
            }
        } finally {
            $this->close();
        }
    }
}
