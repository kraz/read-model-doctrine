<?php

declare(strict_types=1);

namespace Kraz\ReadModelDoctrine\Tools;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\ParameterType;
use Doctrine\ORM\Query\Parameter;
use Kraz\ReadModel\Collections\ArrayCollection;

use function is_array;

class ParametersCollection
{
    /**
     * The query parameters.
     *
     * @phpstan-var ArrayCollection<int, Parameter>
     */
    private ArrayCollection $parameters;

    public function __construct()
    {
        $this->parameters = new ArrayCollection();
    }

    /**
     * Get all defined parameters.
     *
     * @phpstan-return ArrayCollection<int, Parameter>
     */
    public function getParameters(): ArrayCollection
    {
        return $this->parameters;
    }

    /**
     * Gets a query parameter.
     *
     * @param int|string $key the key (index or name) of the bound parameter
     *
     * @return Parameter|null the value of the bound parameter, or NULL if not available
     */
    public function getParameter(int|string $key): Parameter|null
    {
        $key = Parameter::normalizeName($key);

        $filteredParameters = $this->parameters->filter(
            static fn (Parameter $parameter): bool => $parameter->getName() === $key,
        );

        return ! $filteredParameters->isEmpty() ? ($filteredParameters->first() ?: null) : null;
    }

    /**
     * Sets a collection of query parameters.
     *
     * @param ArrayCollection|mixed[] $parameters
     * @phpstan-param ArrayCollection<int, Parameter>|mixed[] $parameters
     *
     * @return $this
     */
    public function setParameters(ArrayCollection|array $parameters): static
    {
        if (is_array($parameters)) {
            /** @phpstan-var ArrayCollection<int, Parameter> $parameterCollection */
            $parameterCollection = new ArrayCollection();

            foreach ($parameters as $key => $value) {
                $parameterCollection->add(new Parameter($key, $value));
            }

            $parameters = $parameterCollection;
        }

        $this->parameters = $parameters;

        return $this;
    }

    /**
     * Sets a query parameter.
     *
     * @param string|int                                       $key   the parameter position or name
     * @param mixed                                            $value the parameter value
     * @param ParameterType|ArrayParameterType|string|int|null $type  The parameter type. If specified, the given value
     *                                                                will be run through the type conversion of this
     *                                                                type. This is usually not needed for strings and
     *                                                                numeric types.
     *
     * @return $this
     */
    public function setParameter(string|int $key, mixed $value, ParameterType|ArrayParameterType|string|int|null $type = null): static
    {
        $existingParameter = $this->getParameter($key);

        if ($existingParameter !== null) {
            $existingParameter->setValue($value, $type);

            return $this;
        }

        $this->parameters->add(new Parameter($key, $value, $type));

        return $this;
    }
}
