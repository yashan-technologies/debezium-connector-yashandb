/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link YashanDBSchemaFactory}.
 */
class YashanDBSchemaFactoryTest {

    @Test
    void shouldReturnSingletonInstance() {
        YashanDBSchemaFactory factory1 = YashanDBSchemaFactory.get();
        YashanDBSchemaFactory factory2 = YashanDBSchemaFactory.get();
        assertThat(factory1).isSameAs(factory2);
    }

    @Test
    void shouldCreateNewInstance() {
        YashanDBSchemaFactory factory = new YashanDBSchemaFactory();
        assertThat(factory).isNotNull();
    }

    @Test
    void shouldBeInstanceOfSchemaFactory() {
        YashanDBSchemaFactory factory = YashanDBSchemaFactory.get();
        assertThat(factory).isInstanceOf(io.debezium.schema.SchemaFactory.class);
    }
}
