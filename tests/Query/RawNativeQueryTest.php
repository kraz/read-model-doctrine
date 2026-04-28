<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tests\Query;

use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\Query\ResultSetMapping;
use Kraz\ReadModelDoctrine\Query\RawNativeQuery;
use Kraz\ReadModelDoctrine\Tests\Tools\ORMTestKit;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function array_column;
use function iterator_to_array;

#[CoversClass(RawNativeQuery::class)]
final class RawNativeQueryTest extends TestCase
{
    private EntityManagerInterface $em;

    protected function setUp(): void
    {
        $this->em = ORMTestKit::createEntityManager();
        $this->seed();
    }

    private function seed(): void
    {
        $conn = $this->em->getConnection();
        $rows = [
            [1, 'Alice', 'alice@example.com', 'eng', 30, 1],
            [2, 'Bob', 'bob@example.com', 'eng', 25, 1],
            [3, 'Charlie', 'charlie@example.com', 'sales', 35, 0],
            [4, 'Dave', '', 'sales', 40, 1],
            [5, 'Eve', null, 'support', 28, 1],
        ];
        foreach ($rows as [$id, $name, $email, $dept, $age, $active]) {
            $conn->executeStatement(
                'INSERT INTO test_entity (id, name, email, department, age, active) VALUES (?, ?, ?, ?, ?, ?)',
                [$id, $name, $email, $dept, $age, $active],
            );
        }
    }

    /** @phpstan-return RawNativeQuery<array<string, mixed>|object> */
    private function makeRawNative(string $sql, ResultSetMapping $rsm): RawNativeQuery
    {
        return new RawNativeQuery($this->em->createNativeQuery($sql, $rsm));
    }

    private function makeScalarRsm(string ...$columns): ResultSetMapping
    {
        $rsm = new ResultSetMapping();
        foreach ($columns as $col) {
            $rsm->addScalarResult($col, $col);
        }

        return $rsm;
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public function testConstructorExtractsSqlFromNativeQuery(): void
    {
        $rsm         = $this->makeScalarRsm('id');
        $nativeQuery = $this->em->createNativeQuery('SELECT id FROM test_entity', $rsm);

        $rawNative = new RawNativeQuery($nativeQuery);

        self::assertSame('SELECT id FROM test_entity', $rawNative->getSql());
    }

    public function testConstructorExtractsParametersFromNativeQuery(): void
    {
        $rsm         = $this->makeScalarRsm('id');
        $nativeQuery = $this->em->createNativeQuery(
            'SELECT id FROM test_entity WHERE department = :dept',
            $rsm,
        );
        $nativeQuery->setParameter('dept', 'eng');

        $rawNative = new RawNativeQuery($nativeQuery);

        self::assertSame('eng', $rawNative->getParameter('dept'));
    }

    public function testConstructorWithNoParametersHasEmptyParams(): void
    {
        $rsm         = $this->makeScalarRsm('id');
        $nativeQuery = $this->em->createNativeQuery('SELECT id FROM test_entity', $rsm);

        $rawNative = new RawNativeQuery($nativeQuery);

        self::assertSame([], $rawNative->getParameters());
    }

    // -------------------------------------------------------------------------
    // getNativeQuery
    // -------------------------------------------------------------------------

    public function testGetNativeQueryReturnsOriginalInstance(): void
    {
        $rsm         = $this->makeScalarRsm('id');
        $nativeQuery = $this->em->createNativeQuery('SELECT id FROM test_entity', $rsm);

        $rawNative = new RawNativeQuery($nativeQuery);

        self::assertSame($nativeQuery, $rawNative->getNativeQuery());
    }

    // -------------------------------------------------------------------------
    // toIterable
    // -------------------------------------------------------------------------

    public function testToIterableReturnsAllRows(): void
    {
        $rsm         = $this->makeScalarRsm('id', 'name');
        $nativeQuery = $this->em->createNativeQuery(
            'SELECT id, name FROM test_entity ORDER BY id ASC',
            $rsm,
        );

        $rawNative = new RawNativeQuery($nativeQuery);
        $rows      = iterator_to_array($rawNative->toIterable());

        self::assertCount(5, $rows);
    }

    public function testToIterableReturnsCorrectData(): void
    {
        $rsm       = $this->makeScalarRsm('id', 'name');
        $rawNative = $this->makeRawNative('SELECT id, name FROM test_entity ORDER BY id ASC', $rsm);
        $rows      = iterator_to_array($rawNative->toIterable());

        self::assertIsArray($rows[0]);
        self::assertSame('1', (string) $rows[0]['id']);
        self::assertSame('Alice', $rows[0]['name']);
    }

    public function testToIterableAppliesItemNormalizer(): void
    {
        $rsm         = $this->makeScalarRsm('id', 'name');
        $nativeQuery = $this->em->createNativeQuery(
            'SELECT id, name FROM test_entity ORDER BY id ASC',
            $rsm,
        );

        $rawNative = new RawNativeQuery($nativeQuery, [
            'item_normalizer' => static fn (array $row): string => $row['name'],
        ]);

        $names = iterator_to_array($rawNative->toIterable());

        self::assertSame(['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'], $names);
    }

    public function testToIterableCanBeCalledMultipleTimes(): void
    {
        $rsm         = $this->makeScalarRsm('id');
        $nativeQuery = $this->em->createNativeQuery(
            'SELECT id FROM test_entity ORDER BY id ASC',
            $rsm,
        );

        $rawNative = new RawNativeQuery($nativeQuery);

        $first  = iterator_to_array($rawNative->toIterable());
        $second = iterator_to_array($rawNative->toIterable());

        self::assertCount(5, $first);
        self::assertCount(5, $second);
    }

    // -------------------------------------------------------------------------
    // SQL override via setSql
    // -------------------------------------------------------------------------

    public function testSetSqlUpdatesUnderlyingNativeQueryOnIterable(): void
    {
        $rsm       = $this->makeScalarRsm('id');
        $rawNative = $this->makeRawNative('SELECT id FROM test_entity ORDER BY id ASC', $rsm);
        $rawNative->setSql('SELECT id FROM test_entity WHERE id = 1 ORDER BY id ASC');

        $rows = iterator_to_array($rawNative->toIterable());

        self::assertCount(1, $rows);
        self::assertIsArray($rows[0]);
        self::assertSame('1', (string) $rows[0]['id']);
    }

    // -------------------------------------------------------------------------
    // Parameter sync (AbstractRawQuery → NativeQuery)
    // -------------------------------------------------------------------------

    public function testParameterSetOnWrapperIsUsedDuringExecution(): void
    {
        $rsm         = $this->makeScalarRsm('id', 'name');
        $nativeQuery = $this->em->createNativeQuery(
            'SELECT id, name FROM test_entity WHERE department = :dept ORDER BY id ASC',
            $rsm,
        );
        $nativeQuery->setParameter('dept', 'eng');

        $rawNative = new RawNativeQuery($nativeQuery);
        // Overwrite through the wrapper API
        $rawNative->setParameter('dept', 'sales');

        $rows = iterator_to_array($rawNative->toIterable());

        $departments = array_column($rows, 'name');
        self::assertContains('Charlie', $departments);
        self::assertNotContains('Alice', $departments);
    }

    // -------------------------------------------------------------------------
    // getResult / getSingleResult
    // -------------------------------------------------------------------------

    public function testGetResultReturnsAllRows(): void
    {
        $rsm         = $this->makeScalarRsm('id');
        $nativeQuery = $this->em->createNativeQuery(
            'SELECT id FROM test_entity ORDER BY id ASC',
            $rsm,
        );

        $rawNative = new RawNativeQuery($nativeQuery);

        self::assertCount(5, $rawNative->getResult());
    }

    public function testGetSingleResultReturnsSingleRow(): void
    {
        $rsm       = $this->makeScalarRsm('id', 'name');
        $rawNative = $this->makeRawNative('SELECT id, name FROM test_entity WHERE id = 1', $rsm);
        $row       = $rawNative->getSingleResult();

        self::assertIsArray($row);
        self::assertSame('1', (string) $row['id']);
    }

    // -------------------------------------------------------------------------
    // getCount
    // -------------------------------------------------------------------------

    public function testGetCountReturnsTotalRows(): void
    {
        $rsm         = $this->makeScalarRsm('id');
        $nativeQuery = $this->em->createNativeQuery(
            'SELECT id FROM test_entity ORDER BY id ASC',
            $rsm,
        );

        $rawNative = new RawNativeQuery($nativeQuery);

        self::assertSame(5, $rawNative->getCount());
    }
}
