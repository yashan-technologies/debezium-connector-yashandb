/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.util.Map;

import io.debezium.connector.yashandb.dialect.YashanDbDialect;
import io.debezium.connector.yashandb.dialect.YashanDbMySqlDialect;
import io.debezium.connector.yashandb.dialect.YashanDbNativeDialect;

/** Immutable mode selection shared by JDBC, schema, snapshots and streaming. */
public final class YashanDbModeContext {

    public static final String OFFSET_KEY = "database_mode";

    private final YashanDbDialect dialect;

    public YashanDbModeContext(boolean mysqlMode) {
        dialect = mysqlMode ? new YashanDbMySqlDialect() : new YashanDbNativeDialect();
    }

    public boolean isMySql() {
        return dialect instanceof YashanDbMySqlDialect;
    }

    public String name() {
        return dialect.name();
    }

    public YashanDbDialect dialect() {
        return dialect;
    }

    public void validateOffset(Map<String, ?> offset) {
        Object stored = offset.get(OFFSET_KEY);
        if (stored == null && !isMySql()) {
            return;
        }
        if (!name().equals(stored)) {
            throw new IllegalArgumentException("Stored database_mode '" + stored + "' does not match connector mode '" + name() + "'");
        }
    }
}
