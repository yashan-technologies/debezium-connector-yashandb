/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import io.debezium.DebeziumException;

/** Reads the effective compatibility mode of a YashanDB JDBC session. */
public final class YashanDbModeDetector {

    public static final String QUERY = "SELECT VALUE FROM SYS.V_$PARAMETER WHERE NAME = 'COMPAT_VECTOR'";

    private YashanDbModeDetector() {
    }

    public static YashanDbModeContext detect(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(QUERY);
                ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new DebeziumException("COMPAT_VECTOR query returned no rows; database mode cannot be determined");
            }
            String value = result.getString(1);
            if (result.next()) {
                throw new DebeziumException("COMPAT_VECTOR query returned multiple rows; database mode is ambiguous");
            }
            if (value == null) {
                throw new DebeziumException("COMPAT_VECTOR is null; database mode cannot be determined");
            }
            switch (value.trim().toUpperCase(Locale.ROOT)) {
                case "MYSQL":
                    return new YashanDbModeContext(true);
                case "YASHAN":
                    return new YashanDbModeContext(false);
                default:
                    throw new DebeziumException("Unsupported COMPAT_VECTOR value '" + value + "'; expected MYSQL or YASHAN");
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to determine database mode from COMPAT_VECTOR", e);
        }
    }

}
