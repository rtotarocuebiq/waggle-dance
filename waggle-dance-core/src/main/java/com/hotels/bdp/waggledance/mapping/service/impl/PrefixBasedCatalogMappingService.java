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
package com.hotels.bdp.waggledance.mapping.service.impl;

import static com.hotels.bdp.waggledance.api.model.FederationType.PRIMARY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import javax.validation.constraints.NotNull;

import com.hotels.bdp.waggledance.api.model.MappedDbs;
import com.hotels.bdp.waggledance.mapping.model.CatalogMapping;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederationType;
import com.hotels.bdp.waggledance.api.model.MappedTables;
import com.hotels.bdp.waggledance.mapping.model.CatalogMapping;
import com.hotels.bdp.waggledance.mapping.model.CatalogMappingImpl;
import com.hotels.bdp.waggledance.mapping.model.MetaStoreMapping;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.GrammarUtils;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PanopticConcurrentOperationExecutor;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationExecutor;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.mapping.service.requests.GetAllDatabasesRequest;
import com.hotels.bdp.waggledance.server.NoPrimaryMetastoreException;
import com.hotels.bdp.waggledance.util.AllowList;

public class PrefixBasedCatalogMappingService
        implements MappingEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixBasedCatalogMappingService.class);

  private static final String EMPTY_PREFIX = "";
  private final MetaStoreMappingFactory metaStoreMappingFactory;
  private final QueryMapping queryMapping;
  private final Map<String, CatalogMapping> mappingsByPrefix;
  private final Map<String, AllowList> mappedCatalogByPrefix;
  private final Map<String, Map<String, AllowList>> mappedDbByPrefix;
  private final Map<String, Map <String,Map<String, AllowList>>> mappedTblByPrefix;

  private CatalogMapping primaryCatalogMapping;

  public PrefixBasedCatalogMappingService(
      MetaStoreMappingFactory metaStoreMappingFactory,
      List<AbstractMetaStore> initialMetastores,
      QueryMapping queryMapping) {
    this.metaStoreMappingFactory = metaStoreMappingFactory;
    this.queryMapping = queryMapping;
    mappingsByPrefix = Collections.synchronizedMap(new LinkedHashMap<>());
    mappedCatalogByPrefix = new ConcurrentHashMap<>();
    mappedDbByPrefix = new ConcurrentHashMap<>();
    mappedTblByPrefix = new ConcurrentHashMap<>();
    for (AbstractMetaStore abstractMetaStore : initialMetastores) {
      add(abstractMetaStore);
    }
  }

  private void add(AbstractMetaStore metaStore) {
    MetaStoreMapping metaStoreMapping = metaStoreMappingFactory.newInstance(metaStore);
    CatalogMapping catalogMapping = createCatalogMapping(metaStoreMapping);

    if (metaStore.getFederationType() == PRIMARY) {
      primaryCatalogMapping = catalogMapping;
    }
    mappingsByPrefix.put(metaStoreMapping.getCatalogPrefix(), catalogMapping);

    List<String> mappedCatalogs = metaStore.getMappedCatalogs();
    mappedCatalogByPrefix.put(metaStoreMapping.getCatalogPrefix(), new AllowList(mappedCatalogs));

    List<MappedDbs> mappedDatabases = metaStore.getMappedDatabases();
    if(mappedCatalogs != null) {
      Map<String, AllowList> mappedDbByCatalog = new HashMap<>();
      for (MappedDbs mappedDb : mappedDatabases) {
        AllowList catalogAllowList = new AllowList(mappedDb.getMappedDBs());
        mappedDbByCatalog.put(mappedDb.getCatalog(), catalogAllowList);
      }
      mappedDbByPrefix.put(metaStoreMapping.getCatalogPrefix(),mappedDbByCatalog);
    }

    List<MappedTables> mappedTables = metaStore.getMappedTables();
    if(mappedTables != null) {
      Map<String, AllowList> mappedTablesbyDbsAndCatalog = new HashMap<>();
      for (MappedTables mappedTable : mappedTables) {
        AllowList tableAllowList = new AllowList(mappedTable.getMappedTables());
        Map<String, AllowList> catalogToCatalog = mappedTblByPrefix.get(metaStoreMapping.getCatalogPrefix()).get(mappedTable.getCatalog());
        if(catalogToCatalog==null)
        {
          catalogToCatalog = mappedTblByPrefix.get(metaStoreMapping.getCatalogPrefix()).put(mappedTable.getCatalog(),new HashMap<>());
        }
        catalogToCatalog.put(mappedTable.getCatalog(),tableAllowList);
      }
      mappedDbByPrefix.put(metaStoreMapping.getCatalogPrefix(),mappedTablesbyDbsAndCatalog);
    }
  }
  private CatalogMapping createCatalogMapping(MetaStoreMapping metaStoreMapping) {
    return new CatalogMappingImpl(metaStoreMapping, queryMapping);
  }

  private void remove(AbstractMetaStore metaStore) {
    if (metaStore.getFederationType() == PRIMARY) {
      primaryCatalogMapping = null;
    }
    CatalogMapping removed = mappingsByPrefix.remove(metaStoreMappingFactory.prefixNameFor(metaStore));
    IOUtils.closeQuietly(removed);
  }

  @Override
  public void onRegister(AbstractMetaStore metaStore) {
    // Synchronizing on the mappingsByPrefix map field so we ensure the implemented FederationEventListener methods are
    // processes sequentially
    synchronized (mappingsByPrefix) {
      if (mappingsByPrefix.containsKey(metaStore.getCatalogPrefix())) {
        throw new WaggleDanceException("MetaStore with prefix '"
            + metaStore.getCatalogPrefix()
            + "' already registered, remove old one first or update");
      }
      if (isPrimaryMetaStoreRegistered(metaStore)) {
        throw new WaggleDanceException("Primary metastore already registered, remove old one first or update");
      }
      add(metaStore);
    }
  }

  private boolean isPrimaryMetaStoreRegistered(AbstractMetaStore metaStore) {
    return (metaStore.getFederationType() == FederationType.PRIMARY) && (primaryCatalogMapping != null);
  }

  @Override
  public void onUpdate(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    // Synchronizing on the mappingsByPrefix map field so we ensure the implemented FederationEventListener methods are
    // processes sequentially
    synchronized (mappingsByPrefix) {
      remove(oldMetaStore);
      add(newMetaStore);
    }
  }

  @Override
  public void onUnregister(AbstractMetaStore metaStore) {
    // Synchronizing on the mappingsByPrefix map field so we ensure the implemented FederationEventListener methods are
    // processes sequentially
    synchronized (mappingsByPrefix) {
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

  private boolean includeInResults(MetaStoreMapping metaStoreMapping, String prefixedDatabaseName) {
    return includeInResults(metaStoreMapping)
        && isDbAllowed(metaStoreMapping.getCatalogPrefix(),
            metaStoreMapping.transformInboundCatalogName(prefixedDatabaseName));
  }

  @Override
  public CatalogMapping databaseMapping(@NotNull String databaseName) throws NoSuchObjectException {
    // Find a Metastore with a prefix
    synchronized (mappingsByPrefix) {
      for (Entry<String, CatalogMapping> entry : mappingsByPrefix.entrySet()) {
        String metastorePrefix = entry.getKey();
        if (Strings.isNotBlank(metastorePrefix) && databaseName.startsWith(metastorePrefix)) {
          CatalogMapping databaseMapping = entry.getValue();
          LOG.debug("Database Name `{}` maps to metastore with prefix `{}`", databaseName, metastorePrefix);
          if (includeInResults(databaseMapping, databaseName)) {
            return databaseMapping;
          }
        }
      }
    }
    // Find a Metastore that has an empty prefix
    CatalogMapping databaseMapping = mappingsByPrefix.get(EMPTY_PREFIX);
    if (databaseMapping != null) {
      LOG.debug("Database Name `{}` maps to metastore with EMPTY_PREFIX", databaseName);
      if (includeInResults(databaseMapping, databaseName)) {
        return databaseMapping;
      }
    }
    if (primaryCatalogMapping != null) {
      // If none found we fall back to primary one
      if (includeInResults(primaryCatalogMapping, databaseName)) {
        LOG.debug("Database Name `{}` maps to 'primary' metastore", databaseName);
        return primaryCatalogMapping;
      }

      throw new NoSuchObjectException("Primary metastore does not have database " + databaseName);
    }
    LOG.debug("Database Name `{}` not mapped", databaseName);
    throw new NoPrimaryMetastoreException(
        "Waggle Dance error no database mapping available tried to map database '" + databaseName + "'");
  }

  @Override
  public void checkTableAllowed(String catalogName, String databaseName, String tableName, CatalogMapping mapping)
    throws NoSuchObjectException {
    String databasePrefix = mapping.getCatalogPrefix();
    String transformedCatalogName = mapping.transformInboundCatalogName(catalogName);
    if (!isTableAllowed(databasePrefix, transformedCatalogName,databaseName, tableName)) {
      throw new NoSuchObjectException(String.format("%s.%s table not found in any mappings", databaseName, tableName));
    }
  }

  @Override
  public List<String> filterTables(String catalogName, String databaseName, List<String> tableNames, CatalogMapping mapping) {
    List<String> allowedTables = new ArrayList<>();
    String catalogPrefix = mapping.getCatalogPrefix();
    String transformedCatalog = mapping.transformInboundCatalogName(catalogName);
    for (String table : tableNames) {
      if (isTableAllowed(catalogPrefix, transformedCatalog,databaseName, table)) {
        allowedTables.add(table);
      }
    }
    return allowedTables;
  }

  private boolean isTableAllowed(String catalogPrefix,String catalogName, String database, String table) {
    Map<String, AllowList> dbToTblAllowList = mappedTblByPrefix.get(catalogPrefix).get(catalogName);
    if (dbToTblAllowList == null) {
      // Accept everything
      return true;
    }
    AllowList tblAllowList = dbToTblAllowList.get(database);
    if (tblAllowList == null) {
      // Accept everything
      return true;
    }
    return tblAllowList.contains(table);
  }

  @Override
  public List<CatalogMapping> getCatalogMappings() {
    Builder<CatalogMapping> builder = ImmutableList.builder();
    synchronized (mappingsByPrefix) {
      for (CatalogMapping catalogMapping : mappingsByPrefix.values()) {
        if (includeInResults(catalogMapping)) {
          builder.add(catalogMapping);
        }
      }
    }
    return builder.build();
  }

  private Map<CatalogMapping, String> databaseMappingsByDbPattern(@NotNull String databasePatterns) {
    Map<CatalogMapping, String> mappings = new LinkedHashMap<>();
    Map<String, String> matchingPrefixes = GrammarUtils
        .selectMatchingPrefixes(mappingsByPrefix.keySet(), databasePatterns);
    for (Entry<String, String> prefixWithPattern : matchingPrefixes.entrySet()) {
      CatalogMapping mapping = mappingsByPrefix.get(prefixWithPattern.getKey());
      if (mapping == null) {
        continue;
      }
      if (includeInResults(mapping)) {
        mappings.put(mapping, prefixWithPattern.getValue());
      }
    }
    return mappings;
  }

  private List<String> getMappedAllowedCatalogs(List<String> catalogs, CatalogMapping mapping) {
    List<String> mappedDatabases = new ArrayList<>();
    for (String catalog : catalogs) {
      if (isDbAllowed(mapping.getCatalogPrefix(), catalog)) {
        mappedDatabases.addAll(mapping.transformOutboundCatalogNameMultiple(database));
      }
    }
    return mappedDatabases;
  }

  private boolean isDbAllowed(String databasePrefix, String database) {
    AllowList allowList = mappedDbByPrefix.get(databasePrefix);
    if (allowList == null) {
      // Accept everything
      return true;
    }
    return allowList.contains(database);
  }

  private boolean databaseAndTableAllowed(String database, String table, CatalogMapping mapping) {
    String dbPrefix = mapping.getCatalogPrefix();
    boolean databaseAllowed = isDbAllowed(dbPrefix, database);
    boolean tableAllowed = isTableAllowed(dbPrefix, database, table);
    return databaseAllowed && tableAllowed;
  }

  @Override
  public PanopticOperationHandler getPanopticOperationHandler() {
    return new PanopticOperationHandler() {

      @Override
      public List<TableMeta> getTableMeta(String db_patterns, String tbl_patterns, List<String> tbl_types) {
        Map<CatalogMapping, String> databaseMappingsForPattern = databaseMappingsByDbPattern(db_patterns);

        BiFunction<TableMeta, CatalogMapping, Boolean> filter = (tableMeta, mapping) -> databaseAndTableAllowed(
            tableMeta.getDbName(), tableMeta.getTableName(), mapping);

        return super.getTableMeta(tbl_patterns, tbl_types, databaseMappingsForPattern, filter);
      }

      @Override
      public List<String> getAllDatabases(String databasePattern) {
        Map<CatalogMapping, String> databaseMappingsForPattern = databaseMappingsByDbPattern(databasePattern);

        BiFunction<String, CatalogMapping, Boolean> filter = (database, mapping) -> isDbAllowed(
            mapping.getCatalogPrefix(), database);

        return super.getAllDatabases(databaseMappingsForPattern, filter);
      }

      @Override
      public List<String> getAllDatabases() {
        List<CatalogMapping> databaseMappings = getCatalogMappings();
        List<GetAllDatabasesRequest> allRequests = new ArrayList<>();

        BiFunction<List<String>, CatalogMapping, List<String>> filter = (
            databases,
            mapping) -> getMappedAllowedDatabases(databases, mapping);

        for (CatalogMapping mapping : databaseMappings) {
          GetAllDatabasesRequest allDatabasesRequest = new GetAllDatabasesRequest(mapping, filter);
          allRequests.add(allDatabasesRequest);
        }
        return getPanopticOperationExecutor()
            .executeRequests(allRequests, GET_DATABASES_TIMEOUT, "Can't fetch databases: {}");
      }

      @Override
      public GetAllFunctionsResponse getAllFunctions(List<CatalogMapping> databaseMappings) {
        GetAllFunctionsResponse allFunctions = super.getAllFunctions(databaseMappings);
        addNonPrefixedPrimaryMetastoreFunctions(allFunctions);
        return allFunctions;
      }

      /*
       * This is done to ensure we can fallback to un-prefixed UDFs (for primary Metastore only).
       */
      private void addNonPrefixedPrimaryMetastoreFunctions(GetAllFunctionsResponse allFunctions) {
        List<Function> newFunctions = new ArrayList<>();
        String primaryPrefix = primaryCatalogMapping().getCatalogPrefix();
        if (!"".equals(primaryPrefix)) {
          if (allFunctions.isSetFunctions()) {
            for (Function function : allFunctions.getFunctions()) {
              newFunctions.add(function);
              if (function.getDbName().startsWith(primaryPrefix)) {
                Function unprefixed = new Function(function);
                // strip off the prefix
                primaryCatalogMapping.transformInboundFunction(unprefixed);
                newFunctions.add(unprefixed);
              }
            }
            allFunctions.setFunctions(newFunctions);
          }
        }
      }

      @Override
      protected PanopticOperationExecutor getPanopticOperationExecutor() {
        return new PanopticConcurrentOperationExecutor();
      }
    };
  }

  @Override
  public void close() throws IOException {
    if (mappingsByPrefix != null) {
      for (MetaStoreMapping metaStoreMapping : mappingsByPrefix.values()) {
        metaStoreMapping.close();
      }
    }
  }

}
