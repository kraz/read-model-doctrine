<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine;

use Kraz\ReadModel\DataSourceReadDataProvider;
use Kraz\ReadModelDoctrine\Query\AbstractRawQuery;

trait DoctrineReadDataProvider
{
    /**
     * @use DataSourceReadDataProvider<DataSource>
     */
    use DataSourceReadDataProvider;

    abstract protected function createDataSource(): DataSource;

    public function getRawQuery(): AbstractRawQuery
    {
        return $this->dataSource()->getRawQuery();
    }
}
