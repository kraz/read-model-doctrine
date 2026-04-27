<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine;

use Doctrine\ORM\Mapping as ORM;
use Doctrine\ORM\QueryBuilder;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModel\BasicReadModelDescriptorFactory;
use Webmozart\Assert\Assert;

class ReadModelDescriptorFactory implements ReadModelDescriptorFactoryInterface
{
    use BasicReadModelDescriptorFactory;

    #[\Override]
    public function createReadModelDescriptorFrom(object|string $model): ReadModelDescriptor
    {
        $modelClass = is_string($model) ? $model : get_class($model);
        $ref = new \ReflectionClass($modelClass);
        if (\count($ref->getAttributes(ORM\Entity::class)) > 0) {
            return $this->createReadModelDescriptorFromEntity($modelClass);
        }
        if ($model instanceof QueryBuilder) {
            return $this->createCompositeReadModelDescriptorFromEntities($model->getRootEntities(), $model->getRootAliases());
        }

        return $this->createReadModelDescriptorFromDto($modelClass);
    }

    private function createReadModelDescriptorFromEntity(object|string $entity, ?string $prefix = null): ReadModelDescriptor
    {
        $entityClass = is_string($entity) ? $entity : get_class($entity);
        $key = null !== $prefix ? $entityClass.'@'.$prefix : $entityClass;

        $descriptor = $this->loadReadModelDescriptor($key);
        if (null !==$descriptor) {
            return $descriptor;
        }

        $properties = [];
        $fieldMap = [];
        $ref = new \ReflectionClass($entityClass);
        foreach ($ref->getProperties() as $property) {
            $isEmbedded = count($property->getAttributes(ORM\Embedded::class)) > 0;
            if (!$isEmbedded) {
                continue;
            }
            $propertyName = $property->getName();
            $propertyClass = $property->getType()?->getName();
            if (!$propertyClass || !class_exists($propertyClass)) {
                continue;
            }
            $refProperty = new \ReflectionClass($propertyClass);
            $items = array_filter(array_map(function ($item) {
                return count($item->getAttributes(ORM\Column::class)) > 0 ? $item->getName() : null;
            }, $refProperty->getProperties()));
            if (1 === count($items)) {
                $itemName = reset($items);
                $fieldMap[$propertyName] = $propertyName.'.'.$itemName;
                $properties[] = null !== $prefix ? $prefix.ucwords($propertyName) : $propertyName;
            } else {
                foreach ($items as $itemName) {
                    $fieldMap[$propertyName.ucwords($itemName)] = $propertyName.'.'.$itemName;
                    $properties[] = (null !== $prefix ? $prefix.ucwords($propertyName) : $propertyName).ucwords($itemName);
                }
            }
        }

        $descriptor = new ReadModelDescriptor($properties, [], [], $fieldMap);
        $this->assignReadModelDescriptor($key, $descriptor);

        return $descriptor;
    }

    private function createCompositeReadModelDescriptorFromEntities(array $entities, array $aliases = []): ReadModelDescriptor
    {
        $entityClassIndex = $entityClassList = array_map(static fn ($entity) => is_string($entity) ? $entity : get_class($entity), $entities);
        \sort($entityClassIndex);
        if (\count($aliases) > 1) {
            $aliasesIndex = array_values($aliases);
            Assert::eq(\count($entityClassIndex), \count($aliasesIndex));
            \sort($aliasesIndex);
            $key = sha1(implode('|', $entityClassIndex).'@'.implode('|', $aliasesIndex));
        } else {
            $key = sha1(implode('|', $entityClassIndex));
        }

        $descriptor = $this->loadReadModelDescriptor($key);
        if (null !== $descriptor) {
            return $descriptor;
        }

        $descriptorsList = [];
        $aliases = array_values($aliases);
        foreach (array_values($entityClassList) as $index => $item) {
            $prefix = \count($aliases) > 1 ? $aliases[$index] : null;
            $descriptorsList[] = $this->createReadModelDescriptorFromEntity($item, $index > 0 ? $prefix : null);
        }

        $descriptor = new ReadModelDescriptor([], [], [], [])->merge(...$descriptorsList);
        $this->assignReadModelDescriptor($key, $descriptor);

        return $descriptor;
    }
}
