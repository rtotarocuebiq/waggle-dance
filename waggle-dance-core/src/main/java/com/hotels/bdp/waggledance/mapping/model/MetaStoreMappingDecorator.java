/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.waggledance.mapping.model;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.thrift.TException;

public abstract class MetaStoreMappingDecorator implements MetaStoreMapping {

  private final MetaStoreMapping metaStoreMapping;

  public MetaStoreMappingDecorator(MetaStoreMapping metaStoreMapping) {
    this.metaStoreMapping = metaStoreMapping;
  }

  @Override
  public String transformOutboundCatalogName(String databaseName) {
    if (databaseName == null) {
      return null;
    }
    return metaStoreMapping.transformOutboundCatalogName(databaseName);
  }

  @Override
  public List<String> transformOutboundCatalogNameMultiple(String databaseName) {
    if (databaseName == null) {
      return Collections.emptyList();
    }
    return metaStoreMapping.transformOutboundCatalogNameMultiple(databaseName);
  }

  @Override
  public Catalog transformOutboundCatalog(Catalog catalog) {
    return metaStoreMapping.transformOutboundCatalog(catalog);
  }

  @Override
  public String transformInboundCatalogName(String databaseName) {
    if (databaseName == null) {
      return null;
    }
    return metaStoreMapping.transformInboundCatalogName(databaseName);
  }

  @Override
  public void close() throws IOException {
    metaStoreMapping.close();
  }

  @Override
  public Iface getClient() {
    return metaStoreMapping.getClient();
  }

  @Override
  public MetaStoreFilterHook getMetastoreFilter() {
    return metaStoreMapping.getMetastoreFilter();
  }

  @Override
  public String getCatalogPrefix() {
    return metaStoreMapping.getCatalogPrefix();
  }

  @Override
  public String getMetastoreMappingName() {
    return metaStoreMapping.getMetastoreMappingName();
  }

  @Override
  public boolean isAvailable() {
    return metaStoreMapping.isAvailable();
  }

  @Override
  public MetaStoreMapping checkWritePermissions(String catalog, String databaseName) {
    return metaStoreMapping.checkWritePermissions(catalog, databaseName);
  }

  @Override
  public void createDatabase(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    metaStoreMapping.createDatabase(database);
  }

  @Override
  public long getLatency() {
    return metaStoreMapping.getLatency();
  }

}
