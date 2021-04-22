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

import static java.util.stream.Collectors.toList;

import static com.hotels.bdp.waggledance.api.model.FederationType.PRIMARY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import javax.validation.constraints.NotNull;

import com.hotels.bdp.waggledance.api.model.MappedDbs;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederationType;
import com.hotels.bdp.waggledance.api.model.MappedTables;
import com.hotels.bdp.waggledance.mapping.model.CatalogMapping;
import com.hotels.bdp.waggledance.mapping.model.CatalogMappingImpl;
import com.hotels.bdp.waggledance.mapping.model.MetaStoreMapping;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PanopticConcurrentOperationExecutor;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationExecutor;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.server.NoPrimaryMetastoreException;
import com.hotels.bdp.waggledance.util.AllowList;

public class StaticDatabaseMappingService implements MappingEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(StaticDatabaseMappingService.class);

  private static final String PRIMARY_KEY = "";
  private final MetaStoreMappingFactory metaStoreMappingFactory;
  private final LoadingCache<String, List<String>> primaryCatalogsCache;
  private final Map<String, CatalogMapping> mappingsByMetaStoreName;
  private final Map<String, CatalogMapping> mappingsByCatalogName;
  private final Map<String, List<String>> catalogMappingToCatalogAllowList;
  private final Map<String, Map<String,AllowList>>catalogToDatabaseAllowList;
  private final Map<String, Map<String,Map<String,AllowList>>> databaseToTableAllowList;

  private CatalogMapping primaryCatalogMapping;
  private final QueryMapping queryMapping;

  public StaticDatabaseMappingService(
      MetaStoreMappingFactory metaStoreMappingFactory,
      List<AbstractMetaStore> initialMetastores,
      QueryMapping queryMapping) {
    this.metaStoreMappingFactory = metaStoreMappingFactory;
    this.queryMapping = queryMapping;
    primaryCatalogsCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .maximumSize(1)
        .build(new CacheLoader<String, List<String>>() {

          @Override
          public List<String> load(String key) throws Exception {
            if (primaryCatalogMapping != null) {
              return primaryCatalogMapping.getClient().get_all_databases();
            } else {
              return Lists.newArrayList();
            }
          }
        });

    mappingsByMetaStoreName = Collections.synchronizedMap(new LinkedHashMap<>());
    mappingsByCatalogName = Collections.synchronizedMap(new LinkedHashMap<>());

    catalogMappingToCatalogAllowList = Collections.synchronizedMap(new LinkedHashMap<>());
    catalogToDatabaseAllowList = new ConcurrentHashMap<>();
    databaseToTableAllowList = new ConcurrentHashMap<>();
    for (AbstractMetaStore federatedMetaStore : initialMetastores) {
      add(federatedMetaStore);
    }
  }

  private void add(AbstractMetaStore metaStore) {


    MetaStoreMapping metaStoreMapping = metaStoreMappingFactory.newInstance(metaStore);

    if (metaStoreMapping.isAvailable()) {
      try {
        List<String> mappedCatalogs = metaStoreMapping.getClient().get_catalogs().getNames();

//        catalogMappingToCatalogAllowList.put(metaStore.getCatalogPrefix(),new AllowList(mappedCatalogs));
        List<MappedDbs> mappedDatabases = metaStore.getMappedDatabases();
        if(mappedCatalogs != null) {
          Map<String, AllowList> mappedDbByCatalog = new HashMap<>();
          for (MappedDbs mappedDb : mappedDatabases) {
            AllowList catalogAllowList = new AllowList(mappedDb.getMappedDBs());
            mappedDbByCatalog.put(mappedDb.getCatalog(), catalogAllowList);
          }
          catalogToDatabaseAllowList.put(metaStoreMapping.getCatalogPrefix(),mappedDbByCatalog);
        }

        CatalogMapping catalogMapping = createCatalogMapping(metaStoreMapping);
        List<String> trasformedCatalogs = mappedCatalogs
                .stream()
                .flatMap(n -> catalogMapping.transformOutboundCatalogNameMultiple(n).stream())
                .collect(toList());
        validateMappableCatalogs(trasformedCatalogs, metaStore);

        if (metaStore.getFederationType() == PRIMARY) {
          validatePrimaryMetastoreCatalogs(trasformedCatalogs);
          primaryCatalogMapping = catalogMapping;
          primaryCatalogsCache.invalidateAll();
        } else {
          validateFederatedMetastoreCatalogs(trasformedCatalogs, metaStoreMapping);
        }

        mappingsByMetaStoreName.put(metaStoreMapping.getMetastoreMappingName(), catalogMapping);
        addDatabaseMappings(trasformedCatalogs, catalogMapping);
        catalogMappingToCatalogAllowList.put(catalogMapping.getMetastoreMappingName(), trasformedCatalogs);
        addTableMappings(metaStore);
      }catch (TException e) {
        LOG.error("Could not get databases for metastore {}", metaStore.getRemoteMetaStoreUris(), e);
      }





    }
  }

  private void validateMappableCatalogs(List<String> catalogs, AbstractMetaStore metaStore) {
    int uniqueMappableDatabasesSize = new HashSet<>(catalogs).size();
    if (uniqueMappableDatabasesSize != catalogs.size()) {
      throw new WaggleDanceException(
          "Database clash, found duplicate database names after applying all the mappings. Check the configuration for metastore '"
              + metaStore.getName()
              + "', mappableDatabases are: '"
              + catalogs.toString());
    }
  }

  private void validatePrimaryMetastoreCatalogs(List<String> databases) {
    for (String database : databases) {
      if (mappingsByCatalogName.containsKey(database)) {
        throw new WaggleDanceException("Database clash, found '"
            + database
            + "' in primary that was already mapped to a federated metastore '"
            + mappingsByCatalogName.get(database).getMetastoreMappingName()
            + "', please remove the database from the federated metastore list it can't be"
            + " accessed via Waggle Dance");
      }
    }
  }

  private void validateFederatedMetastoreCatalogs(List<String> mappableDatabases, MetaStoreMapping metaStoreMapping) {
    try {
      Set<String> allPrimaryDatabases = Sets.newHashSet(primaryCatalogsCache.get(PRIMARY_KEY));
      for (String database : mappableDatabases) {
        if (allPrimaryDatabases.contains(database.toLowerCase(Locale.ROOT))) {
          throw new WaggleDanceException("Database clash, found '"
              + database
              + "' to be mapped for the federated metastore '"
              + metaStoreMapping.getMetastoreMappingName()
              + "' already present in the primary database, please remove the database from the list it can't be"
              + " accessed via Waggle Dance");
        }
        if (mappingsByCatalogName.containsKey(database.toLowerCase(Locale.ROOT))) {
          throw new WaggleDanceException("Database clash, found '"
              + database
              + "' to be mapped for the federated metastore '"
              + metaStoreMapping.getMetastoreMappingName()
              + "' already present in another federated database, please remove the database from the list it can't"
              + " be accessed via Waggle Dance");
        }
      }
    } catch (ExecutionException e) {
      throw new WaggleDanceException("Can't validate database clashes", e.getCause());
    }
  }

  private List<String> applyAllowList(List<String> allDatabases, AllowList allowList) {
    List<String> matchedDatabases = new ArrayList<>();

    for (String database : allDatabases) {
      if (allowList.contains(database)) {
        matchedDatabases.add(database);
      }
    }
    return matchedDatabases;
  }

  private void addDatabaseMappings(List<String> databases, CatalogMapping databaseMapping) {
    for (String databaseName : databases) {
      mappingsByCatalogName.put(databaseName, databaseMapping);
    }
  }

  private void addTableMappings(AbstractMetaStore metaStore) {

    List<MappedTables> mappedTables = metaStore.getMappedTables();
    if(mappedTables != null) {
      Map<String, AllowList> mappedTablesbyDbsAndCatalog = new HashMap<>();
      for (MappedTables mappedTable : mappedTables) {
        AllowList tableAllowList = new AllowList(mappedTable.getMappedTables());
        Map<String, AllowList> databaseToTables = databaseToTableAllowList.get(metaStore.getCatalogPrefix()).get(mappedTable.getCatalog());
        if(databaseToTables==null)
        {
          databaseToTables = databaseToTableAllowList.get(metaStore.getCatalogPrefix()).put(mappedTable.getCatalog(),new HashMap<>());
        }
        databaseToTables.put(mappedTable.getCatalog(),tableAllowList);
      }
    }
  }

  private CatalogMapping createCatalogMapping(MetaStoreMapping metaStoreMapping) {
    return new CatalogMappingImpl(metaStoreMapping, queryMapping);
  }

  private void remove(AbstractMetaStore metaStore) {
    if (metaStore.getFederationType() == PRIMARY) {
      primaryCatalogMapping = null;
      primaryCatalogsCache.invalidateAll();
    }

    CatalogMapping removed = mappingsByMetaStoreName.remove(metaStore.getName());
    String mappingName = removed.getMetastoreMappingName();
    List<String> catalogToRemove = catalogMappingToCatalogAllowList.get(mappingName);
    List<String> catalogsToRemove = catalogMappingToCatalogAllowList.remove(mappingName);
    for (String catalog : catalogsToRemove) {
      mappingsByCatalogName.remove(catalog);
    }

    IOUtils.closeQuietly(removed);
  }

  @Override
  public void onRegister(AbstractMetaStore metaStore) {
    // Synchronizing on the mappingsByMetaStoreName map field so we ensure the implemented FederationEventListener
    // methods are processed sequentially
    synchronized (mappingsByMetaStoreName) {
      if (mappingsByMetaStoreName.containsKey(metaStore.getName())) {
        throw new WaggleDanceException(
            "Metastore with name '" + metaStore.getName() + "' already registered, remove old one first or update");
      }
      if ((metaStore.getFederationType() == FederationType.PRIMARY) && (primaryCatalogMapping != null)) {
        throw new WaggleDanceException("Primary metastore already registered, remove old one first or update");
      }
      add(metaStore);
    }
  }

  @Override
  public void onUpdate(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    // Synchronizing on the mappingsByMetaStoreName map field so we ensure the implemented FederationEventListener
    // methods are processed sequentially
    synchronized (mappingsByMetaStoreName) {
      remove(oldMetaStore);
      add(newMetaStore);
    }
  }

  @Override
  public void onUnregister(AbstractMetaStore metaStore) {
    // Synchronizing on the mappingsByMetaStoreName map field so we ensure the implemented FederationEventListener
    // methods are processed sequentially
    synchronized (mappingsByMetaStoreName) {
      remove(metaStore);
    }
  }

  @Override
  public CatalogMapping primaryCatalogMapping() {
    if (primaryCatalogMapping == null) {
      throw new NoPrimaryMetastoreException("Waggle Dance error no primary database mapping available");
    }
    return primaryCatalogMapping;
  }

  private boolean includeInResults(MetaStoreMapping metaStoreMapping) {
    return (metaStoreMapping != null) && metaStoreMapping.isAvailable();
  }

  @Override
  public CatalogMapping catalogMapping(@NotNull String databaseName) throws NoSuchObjectException {
    CatalogMapping databaseMapping = mappingsByCatalogName.get(databaseName.toLowerCase(Locale.ROOT));
    if (databaseMapping != null) {
      LOG
          .debug("Database Name `{}` maps to metastore with name '{}'", databaseName,
              databaseMapping.getMetastoreMappingName());
      if (includeInResults(databaseMapping)) {
        return databaseMapping;
      }
    }
    LOG.debug("Database Name `{}` not mapped", databaseName);
    throw new NoSuchObjectException("Primary metastore does not have database " + databaseName);
  }

  @Override
  public void checkTableAllowed(String catalog, String databaseName, String tableName,
      CatalogMapping mapping) throws NoSuchObjectException {
    if (!isTableAllowed(mapping.getMetastoreMappingName(),catalog, databaseName, tableName)) {
      throw new NoSuchObjectException(String.format("%s.%s table not found in any mappings", databaseName, tableName));
    }
  }

  @Override
  public List<String> filterTables(String catalog, String databaseName, List<String> tableNames, CatalogMapping mapping) {
    List<String> allowedTables = new ArrayList<>();
    String db = databaseName.toLowerCase(Locale.ROOT);
    for (String table: tableNames)
      if (isTableAllowed(mapping.getMetastoreMappingName(),catalog, db, table)) {
        allowedTables.add(table);
      }
    return allowedTables;
  }

  @Override
  public void checkDatabaseAllowed(String catalog, String databaseName, CatalogMapping mapping)
          throws NoSuchObjectException
  {
    //TODO
  }

  @Override
  public List<String> filterDatabases(String catalog, List<String> databaseName, CatalogMapping mapping)
  {
    //TODO
    return null;
  }

  private boolean isTableAllowed(String mapping, String catalog, String database, String table) {
    AllowList tblAllowList = databaseToTableAllowList.get(mapping).get(catalog).get(database);
    if (tblAllowList == null) {
      // Accept everything
      return true;
    }
    return tblAllowList.contains(table);
  }

  @Override
  public List<CatalogMapping> getCatalogMappings() {
    Builder<CatalogMapping> builder = ImmutableList.builder();
    synchronized (mappingsByMetaStoreName) {
      for (CatalogMapping databaseMapping : mappingsByMetaStoreName.values()) {
        if (includeInResults(databaseMapping)) {
          builder.add(databaseMapping);
        }
      }
    }
    return builder.build();
  }

  private boolean databaseAndTableAllowed(String catalog, String database, String table, CatalogMapping mapping) {
    boolean isPrimary = mapping.equals(primaryCatalogMapping);
    boolean isMapped = mappingsByCatalogName.containsKey(database);
    boolean databaseAllowed = isPrimary || isMapped;
    boolean tableAllowed = isTableAllowed(mapping.getMetastoreMappingName(),catalog,database, table);
    return databaseAllowed && tableAllowed;
  }

  @Override
  public PanopticOperationHandler getPanopticOperationHandler() {
    return new PanopticOperationHandler() {

      @Override
      public List<TableMeta> getTableMeta(String catalog, String db_patterns, String tbl_patterns, List<String> tbl_types) {

        BiFunction<TableMeta, CatalogMapping, Boolean> filter = (tableMeta, mapping) ->
            databaseAndTableAllowed(tableMeta.getCatName(),tableMeta.getDbName(), tableMeta.getTableName(), mapping);

        Map<CatalogMapping, String> mappingsForPattern = new LinkedHashMap<>();
        for (CatalogMapping mapping : getCatalogMappings()) {
          mappingsForPattern.put(mapping, db_patterns);
        }
        return super.getTableMeta(db_patterns, tbl_patterns, tbl_types, mappingsForPattern, filter);
      }

      @Override
      public List<String> getAllCatalogs(String pattern) {
        BiFunction<String, CatalogMapping, Boolean> filter = (database, mapping) -> mappingsByCatalogName
            .containsKey(database);

        Map<CatalogMapping, String> mappingsForPattern = new LinkedHashMap<>();
        for (CatalogMapping mapping : getCatalogMappings()) {
          mappingsForPattern.put(mapping, pattern);
        }

        return super.getAllCatalogs(mappingsForPattern, filter);
      }

      @Override
      public List<String> getAllCatalogs() {
        return new ArrayList<>(mappingsByCatalogName.keySet());
      }

      @Override
      protected PanopticOperationExecutor getPanopticOperationExecutor() {
        return new PanopticConcurrentOperationExecutor();
      }
    };
  }

  @Override
  public void close() throws IOException {
    if (mappingsByMetaStoreName != null) {
      for (MetaStoreMapping metaStoreMapping : mappingsByMetaStoreName.values()) {
        metaStoreMapping.close();
      }
    }
  }

}
