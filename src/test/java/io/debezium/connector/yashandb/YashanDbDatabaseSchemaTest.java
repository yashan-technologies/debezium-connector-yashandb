/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;

/**
 * Unit tests for static methods in {@link YashanDBDatabaseSchema}.
 */
class YashanDBDatabaseSchemaTest {

    @Test
    void shouldIdentifyClobColumn() {
        Column clobCol = Column.editor().name("data").jdbcType(com.yashandb.jdbc.YasTypes.CLOB).create();
        assertThat(YashanDBDatabaseSchema.isLobColumn(clobCol)).isTrue();
    }

    @Test
    void shouldIdentifyNclobColumn() {
        Column nclobCol = Column.editor().name("data").jdbcType(com.yashandb.jdbc.YasTypes.NCLOB).create();
        assertThat(YashanDBDatabaseSchema.isLobColumn(nclobCol)).isTrue();
    }

    @Test
    void shouldIdentifyBlobColumn() {
        Column blobCol = Column.editor().name("data").jdbcType(com.yashandb.jdbc.YasTypes.BLOB).create();
        assertThat(YashanDBDatabaseSchema.isLobColumn(blobCol)).isTrue();
    }

    @Test
    void shouldNotIdentifyVarcharAsLob() {
        Column varcharCol = Column.editor().name("name").jdbcType(java.sql.Types.VARCHAR).create();
        assertThat(YashanDBDatabaseSchema.isLobColumn(varcharCol)).isFalse();
    }

    @Test
    void shouldNotIdentifyIntegerAsLob() {
        Column intCol = Column.editor().name("id").jdbcType(java.sql.Types.INTEGER).create();
        assertThat(YashanDBDatabaseSchema.isLobColumn(intCol)).isFalse();
    }

    @Test
    void shouldIdentifyXmlColumn() {
        Column xmlCol = Column.editor().name("data").jdbcType(com.yashandb.jdbc.YasTypes.SQLXML).create();
        assertThat(YashanDBDatabaseSchema.isXmlColumn(xmlCol)).isTrue();
    }

    @Test
    void shouldNotIdentifyVarcharAsXml() {
        Column varcharCol = Column.editor().name("name").jdbcType(java.sql.Types.VARCHAR).create();
        assertThat(YashanDBDatabaseSchema.isXmlColumn(varcharCol)).isFalse();
    }
}
