/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.connector.yashandb.YashanDbConnector;
import io.debezium.connector.yashandb.YashanDbConnectorConfig;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.snapshot.spi.SnapshotLock;

@ConnectorSpecific(connector = YashanDbConnector.class)
public class SharedSnapshotLock implements SnapshotLock, BeanRegistryAware {

    private BeanRegistry beanRegistry;

    @Override
    public String name() {
        return YashanDbConnectorConfig.SnapshotLockingMode.SHARED.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void injectBeanRegistry(BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        YashanDbConnectorConfig connectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, YashanDbConnectorConfig.class);
        if (connectorConfig.getModeContext().isMySql()) {
            RelationalSnapshotContext<?, ?> snapshotContext = beanRegistry.lookupByName(StandardBeanNames.SNAPSHOT_CONTEXT, RelationalSnapshotContext.class);
            if (snapshotContext == null) {
                return Optional.of("LOCK TABLES " + tableId + " READ");
            }
            if (snapshotContext.capturedTables.isEmpty()) {
                return Optional.empty();
            }
            // Each MySQL LOCK TABLES replaces existing locks, so acquire all snapshot table locks together.
            String tables = snapshotContext.capturedTables.stream()
                    .map(table -> connectorConfig.getModeContext().dialect().qualifiedTableName(table) + " READ")
                    .collect(Collectors.joining(", "));
            return Optional.of("LOCK TABLES " + tables);
        }
        return Optional.of(String.format("LOCK TABLE %s IN SHARE MODE", tableId));
    }
}
