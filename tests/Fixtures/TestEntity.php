<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Fixtures;

use Doctrine\ORM\Mapping as ORM;

#[ORM\Entity]
#[ORM\Table(name: 'test_entity')]
class TestEntity
{
    #[ORM\Id]
    #[ORM\Column(type: 'integer')]
    public int $id = 0;

    #[ORM\Column(type: 'string', length: 191, nullable: true)]
    public string|null $name = null;

    #[ORM\Column(type: 'string', length: 191, nullable: true)]
    public string|null $email = null;

    #[ORM\Column(type: 'string', length: 64, nullable: true)]
    public string|null $department = null;

    #[ORM\Column(type: 'integer', nullable: true)]
    public int|null $age = null;

    #[ORM\Column(type: 'boolean', nullable: true)]
    public bool|null $active = null;
}
