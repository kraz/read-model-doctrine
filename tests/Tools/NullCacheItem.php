<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Tools;

use DateInterval;
use DateTimeInterface;
use Psr\Cache\CacheItemInterface;

final class NullCacheItem implements CacheItemInterface
{
    private mixed $value = null;
    private bool $hit    = false;

    public function __construct(private readonly string $key)
    {
    }

    public function getKey(): string
    {
        return $this->key;
    }

    public function get(): mixed
    {
        return $this->value;
    }

    public function isHit(): bool
    {
        return $this->hit;
    }

    public function set(mixed $value): static
    {
        $this->value = $value;
        $this->hit   = true;

        return $this;
    }

    public function expiresAt(DateTimeInterface|null $expiration): static
    {
        return $this;
    }

    public function expiresAfter(int|DateInterval|null $time): static
    {
        return $this;
    }
}
