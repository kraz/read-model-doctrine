<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Fixtures;

use Doctrine\ORM\Mapping as ORM;

#[ORM\Entity]
#[ORM\Table(name: 'composite_key_entity')]
class CompositeKeyEntity
{
    #[ORM\Id]
    #[ORM\Column(type: 'integer')]
    public int $partA = 0;

    #[ORM\Id]
    #[ORM\Column(type: 'integer')]
    public int $partB = 0;

    #[ORM\Column(type: 'string', length: 191, nullable: true)]
    public string|null $name = null;
}
