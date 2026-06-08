/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link YashanDBTableIdParser}.
 */
class YashanDBTableIdParserTest {

    @Test
    void shouldParseTableIdWithCatalogAndSchema() {
        // YashanDB format with 3 parts: catalog.schema.table
        TableId tableId = YashanDBTableIdParser.parse("catalog.schema.table");
        assertThat(tableId).isNotNull();
        assertThat(tableId.catalog()).isEqualTo("catalog");
        assertThat(tableId.schema()).isEqualTo("schema");
        assertThat(tableId.table()).isEqualTo("table");
    }

    @Test
    void shouldParseTableIdWithDomain() {
        // YashanDB: domain.schema.table where domain may have dots
        TableId tableId = YashanDBTableIdParser.parse("domain.sub.schema.table");
        assertThat(tableId).isNotNull();
        assertThat(tableId.catalog()).isEqualTo("domain.sub");
        assertThat(tableId.schema()).isEqualTo("schema");
        assertThat(tableId.table()).isEqualTo("table");
    }

    @Test
    void shouldParseTableIdWithComplexDomain() {
        TableId tableId = YashanDBTableIdParser.parse("a.b.c.schema.table");
        assertThat(tableId).isNotNull();
        assertThat(tableId.catalog()).isEqualTo("a.b.c");
        assertThat(tableId.schema()).isEqualTo("schema");
        assertThat(tableId.table()).isEqualTo("table");
    }

    @Test
    void shouldParseTableIdWithFourParts() {
        // Four parts should be parsed as domain.schema.table (last 2 are schema/table)
        TableId tableId = YashanDBTableIdParser.parse("d1.d2.d3.table");
        assertThat(tableId).isNotNull();
        assertThat(tableId.catalog()).isEqualTo("d1.d2");
        assertThat(tableId.schema()).isEqualTo("d3");
        assertThat(tableId.table()).isEqualTo("table");
    }
}
