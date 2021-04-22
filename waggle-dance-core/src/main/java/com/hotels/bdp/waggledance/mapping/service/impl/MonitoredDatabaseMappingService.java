/**
 * Copyright (C) 2016-2020 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.waggledance.mapping.service.impl;

import java.io.IOException;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.mapping.model.CatalogMapping;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.metrics.CurrentMonitoredMetaStoreHolder;

public class MonitoredDatabaseMappingService implements MappingEventListener {

  private final MappingEventListener wrapped;

  public MonitoredDatabaseMappingService(MappingEventListener wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public CatalogMapping primaryCatalogMapping() {
    CatalogMapping primaryDatabaseMapping = wrapped.primaryCatalogMapping();
    CurrentMonitoredMetaStoreHolder.monitorMetastore(primaryDatabaseMapping.getMetastoreMappingName());
    return primaryDatabaseMapping;
  }

  @Override
  public CatalogMapping catalogMapping(@NotNull String catalogName) throws NoSuchObjectException {
    CatalogMapping databaseMapping = wrapped.catalogMapping(catalogName);
    CurrentMonitoredMetaStoreHolder.monitorMetastore(databaseMapping.getMetastoreMappingName());
    return databaseMapping;
  }

  @Override
  public void checkTableAllowed(String catalog, String databaseName, String tableName,
      CatalogMapping mapping) throws NoSuchObjectException {
      wrapped.checkTableAllowed(catalog, databaseName, tableName, mapping);
    }

  @Override
  public List<String> filterTables(String catalog, String databaseName, List<String> tableNames, CatalogMapping mapping) {
    return wrapped.filterTables(catalog, databaseName, tableNames, mapping);
  }

  @Override
  public void checkDatabaseAllowed(String catalog, String databaseName, CatalogMapping mapping)
          throws NoSuchObjectException
  {
    //TODO:
  }

  @Override
  public List<String> filterDatabases(String catalog, List<String> databaseName, CatalogMapping mapping)
  {
    //TODO:
    return null;
  }

  @Override
  public PanopticOperationHandler getPanopticOperationHandler() {
    PanopticOperationHandler handler = wrapped.getPanopticOperationHandler();
    CurrentMonitoredMetaStoreHolder.monitorMetastore();
    return handler;
  }

  @Override
  public List<CatalogMapping> getCatalogMappings() {
    return wrapped.getCatalogMappings();
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }

  @Override
  public void onRegister(AbstractMetaStore federatedMetaStore) {
    wrapped.onRegister(federatedMetaStore);
  }

  @Override
  public void onUnregister(AbstractMetaStore federatedMetaStore) {
    wrapped.onUnregister(federatedMetaStore);
  }

  @Override
  public void onUpdate(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    wrapped.onUpdate(oldMetaStore, newMetaStore);
  }
}
