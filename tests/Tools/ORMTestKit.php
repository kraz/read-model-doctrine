<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Tools;

use Doctrine\DBAL\DriverManager;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\ORMSetup;
use Doctrine\ORM\Tools\SchemaTool;
use Kraz\ReadModelDoctrine\Tests\Fixtures\CompositeKeyEntity;
use Kraz\ReadModelDoctrine\Tests\Fixtures\TestEntity;

final class ORMTestKit
{
    public static function createEntityManager(): EntityManagerInterface
    {
        $config = ORMSetup::createAttributeMetadataConfig(
            paths: [__DIR__ . '/../Fixtures'],
            isDevMode: true,
            cache: new NullCachePool(),
        );
        $config->enableNativeLazyObjects(true);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $config);

        // Make LIKE case-sensitive so case-sensitive operator tests behave consistently.
        $connection->executeStatement('PRAGMA case_sensitive_like = ON');

        $em = new EntityManager($connection, $config);

        $schemaTool = new SchemaTool($em);
        $schemaTool->createSchema([
            $em->getClassMetadata(TestEntity::class),
            $em->getClassMetadata(CompositeKeyEntity::class),
        ]);

        return $em;
    }
}
