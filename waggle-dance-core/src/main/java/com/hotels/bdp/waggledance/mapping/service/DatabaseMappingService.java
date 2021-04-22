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
package com.hotels.bdp.waggledance.mapping.service;

import java.io.Closeable;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

import com.hotels.bdp.waggledance.mapping.model.CatalogMapping;

public interface DatabaseMappingService extends Closeable {

  /**
   * @return the {@link CatalogMapping} that maps to the primary metastore
   */
  CatalogMapping primaryDatabaseMapping();

  /**
   * @param databaseName given database name
   * @return the {@link CatalogMapping} that maps to the given databaseName
   */
  CatalogMapping databaseMapping(@NotNull String databaseName) throws NoSuchObjectException;

  /**
   * Checks that the table from the specified database is allowed and throws a {@link NoSuchObjectException} if not.
   */
  void checkTableAllowed(String catalog, String databaseName, String tableName,
      CatalogMapping mapping) throws NoSuchObjectException;

  /**
   * Filters out the tables which are not allowed and returns the rest.
   */
  List<String> filterTables(String catalog, String databaseName, List<String> tableNames, CatalogMapping mapping);

  PanopticOperationHandler getPanopticOperationHandler();

  List<CatalogMapping> getDatabaseMappings();
}
