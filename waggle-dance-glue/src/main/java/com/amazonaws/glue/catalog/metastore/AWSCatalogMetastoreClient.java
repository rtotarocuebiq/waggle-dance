package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.glue.catalog.converters.HiveToCatalogConverter;
import com.amazonaws.glue.catalog.util.BatchDeletePartitionsHelper;
import com.amazonaws.glue.catalog.util.ExpressionHelper;
import com.amazonaws.glue.catalog.util.LoggingHelper;
import com.amazonaws.glue.catalog.util.MetastoreClientUtils;
import com.amazonaws.glue.shims.AwsGlueHiveShims;
import com.amazonaws.glue.shims.ShimsLoader;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddForeignKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AddPrimaryKeyRequest;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprResult;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionListComposingSpecProxy;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.glue.catalog.converters.ConverterUtils.catalogTableToString;
import static com.amazonaws.glue.catalog.converters.ConverterUtils.stringToCatalogTable;
import static com.amazonaws.glue.catalog.metastore.GlueMetastoreClientDelegate.INDEX_PREFIX;
import static com.amazonaws.glue.catalog.util.MetastoreClientUtils.isExternalTable;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_COMMENT;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

public abstract class AWSCatalogMetastoreClient implements CloseableThriftHiveMetastoreIface
{

  // TODO "hook" into Hive logging (hive or hive.metastore)
  private static final Logger logger = Logger.getLogger(AWSCatalogMetastoreClient.class);

  private final HiveConf conf;
  private final AWSGlue glueClient;
  private final Warehouse wh;
  private final GlueMetastoreClientDelegate glueMetastoreClientDelegate;
  private final String catalogId;
  
  private static final int BATCH_DELETE_PARTITIONS_PAGE_SIZE = 25;
  private static final int BATCH_DELETE_PARTITIONS_THREADS_COUNT = 5;
  static final String BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT = "batch-delete-partitions-%d";
  private static final ExecutorService BATCH_DELETE_PARTITIONS_THREAD_POOL = Executors.newFixedThreadPool(
    BATCH_DELETE_PARTITIONS_THREADS_COUNT,
    new ThreadFactoryBuilder()
      .setNameFormat(BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT)
      .setDaemon(true).build()
  );



  private Map<String, String> currentMetaVars;
  private final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();

  public AWSCatalogMetastoreClient(HiveConf conf, HiveMetaHookLoader hook) throws MetaException {
    this.conf = conf;
    glueClient = new AWSGlueClientFactory(this.conf).newClient();

    // TODO preserve existing functionality for HiveMetaHook
    wh = new Warehouse(this.conf);

    AWSGlueMetastore glueMetastore = new AWSGlueMetastoreFactory().newMetastore(conf);
    glueMetastoreClientDelegate = new GlueMetastoreClientDelegate(this.conf, glueMetastore, wh);

    snapshotActiveConf();
    catalogId = MetastoreClientUtils.getCatalogId(conf);
    if (!doesDefaultDBExist()) {
      createDefaultDatabase();
    }
  }

  /**
   * Currently used for unit tests
   */
  public static class Builder {

    private HiveConf conf;
    private Warehouse wh;
    private GlueClientFactory clientFactory;
    private AWSGlueMetastoreFactory metastoreFactory;
    private boolean createDefaults = true;
    private String catalogId;

    public Builder withHiveConf(HiveConf conf) {
      this.conf = conf;
      return this;
    }

    public Builder withClientFactory(GlueClientFactory clientFactory) {
      this.clientFactory = clientFactory;
      return this;
    }

    public Builder withMetastoreFactory(AWSGlueMetastoreFactory metastoreFactory) {
      this.metastoreFactory = metastoreFactory;
      return this;
    }

    public Builder withWarehouse(Warehouse wh) {
      this.wh = wh;
      return this;
    }

    public Builder withCatalogId(String catalogId) {
      this.catalogId = catalogId;
      return this;
    }

    public AWSCatalogMetastoreClient build() throws MetaException {
      return new AWSCatalogMetastoreClientHive3(this);
    }

    public Builder createDefaults(boolean createDefaultDB) {
      this.createDefaults = createDefaultDB;
      return this;
    }
  }



  AWSCatalogMetastoreClient(Builder builder) throws MetaException {
    conf = Objects.firstNonNull(builder.conf, new HiveConf());

    if (builder.wh != null) {
      this.wh = builder.wh;
    } else {
      this.wh = new Warehouse(conf);
    }

    if (builder.catalogId != null) {
    	this.catalogId = builder.catalogId;
    } else {
    	this.catalogId = null;
    }

    GlueClientFactory clientFactory = Objects.firstNonNull(builder.clientFactory, new AWSGlueClientFactory(conf));
    AWSGlueMetastoreFactory metastoreFactory = Objects.firstNonNull(builder.metastoreFactory,
            new AWSGlueMetastoreFactory());

    glueClient = clientFactory.newClient();
    AWSGlueMetastore glueMetastore = metastoreFactory.newMetastore(conf);
    glueMetastoreClientDelegate = new GlueMetastoreClientDelegate(this.conf, glueMetastore, wh);

    /**
     * It seems weird to create databases as part of client construction. This
     * part should probably be moved to the section in hive code right after the
     * metastore client is instantiated. For now, simply copying the
     * functionality in the thrift server
     */
    if(builder.createDefaults && !doesDefaultDBExist()) {
      createDefaultDatabase();
    }
  }

  private boolean doesDefaultDBExist() throws MetaException {
    
    try {
      GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest().withName(DEFAULT_DATABASE_NAME).withCatalogId(
          catalogId);
      glueClient.getDatabase(getDatabaseRequest);
    } catch (EntityNotFoundException e) {
      return false;
    } catch (AmazonServiceException e) {
      String msg = "Unable to verify existence of default database: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
    return true;
  }

  private void createDefaultDatabase() throws MetaException {
    Database defaultDB = new Database();
    defaultDB.setName(DEFAULT_DATABASE_NAME);
    defaultDB.setDescription(DEFAULT_DATABASE_COMMENT);
    defaultDB.setLocationUri(wh.getDefaultDatabasePath(DEFAULT_DATABASE_NAME).toString());

    org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet principalPrivilegeSet
          = new org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet();
    principalPrivilegeSet.setRolePrivileges(Maps.<String, List<org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo>>newHashMap());

    defaultDB.setPrivileges(principalPrivilegeSet);

    /**
     * TODO: Grant access to role PUBLIC after role support is added
     */
    try {
      create_database(defaultDB);
    } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
      logger.warn("database - default already exists. Ignoring..");
    } catch (Exception e) {
      logger.error("Unable to create default database", e);
    }
  }

  @Override
  public void create_database(Database database) throws InvalidObjectException,
        org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    glueMetastoreClientDelegate.createDatabase(database);
  }

  @Override
  public Database get_database(String name) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getDatabase(name);
  }

  @Override
  public List<String> get_databases(String pattern) throws MetaException, TException {
    return glueMetastoreClientDelegate.getDatabases(pattern);
  }

  @Override
  public List<String> get_all_databases() throws MetaException, TException {
    return get_databases(".*");
  }


  @Override
  public void alter_database(String databaseName, Database database) throws NoSuchObjectException, MetaException,
        TException {
    glueMetastoreClientDelegate.alterDatabase(databaseName, database);
  }

  @Override
  public void drop_database(String name, boolean deleteData, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException,
        TException {
    glueMetastoreClientDelegate.dropDatabase(name, deleteData, false, cascade);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition add_partition(org.apache.hadoop.hive.metastore.api.Partition partition)
        throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
        TException {
    glueMetastoreClientDelegate.addPartitions(Lists.newArrayList(partition), false, true);
    return partition;
  }

  @Override
  public int add_partitions(List<org.apache.hadoop.hive.metastore.api.Partition> partitions)
        throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
        TException {
    return glueMetastoreClientDelegate.addPartitions(partitions, false, true).size();
  }

  @Override
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest addPartitionsRequest)
          throws TException
  {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = addPartitionsRequest.getParts();
    boolean ifNotExists = addPartitionsRequest.isIfNotExists();
    boolean needResult = addPartitionsRequest.isNeedResult();
    List<org.apache.hadoop.hive.metastore.api.Partition> addPartitions = glueMetastoreClientDelegate.addPartitions(partitions, ifNotExists, needResult);
    AddPartitionsResult addPartitionsResult = new AddPartitionsResult();
    addPartitionsResult.setPartitions(addPartitions);
    //TODO: check the set of the fields (see getter)
    addPartitionsResult.setFieldValue(AddPartitionsResult._Fields.PARTITIONS,null);
    return addPartitionsResult;
  }

  @Override
  public int add_partitions_pspec(List<PartitionSpec> list)
          throws TException
  {
    PartitionSpecProxy pSpec = PartitionSpecProxy.Factory.get(list);
    return glueMetastoreClientDelegate.addPartitionsSpecProxy(pSpec);
  }

  @Override
  public void alter_function(String dbName, String functionName, org.apache.hadoop.hive.metastore.api.Function newFunction) throws InvalidObjectException,
        MetaException, TException {
    glueMetastoreClientDelegate.alterFunction(dbName, functionName, newFunction); 
  }

  @Override
  public void alter_partition(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Partition partition
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, Lists.newArrayList(partition));
  }

  @Override
  public void alter_partition_with_environment_context(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Partition partition,
      EnvironmentContext environmentContext
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, Lists.newArrayList(partition));
  }


  @Override
  public void alter_partitions(
      String dbName,
      String tblName,
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, partitions);
  }

  @Override
  public void alter_partitions_with_environment_context(
      String dbName,
      String tblName,
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
      EnvironmentContext environmentContext
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, partitions);
  }


  @Override
  public void alter_table(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table table)
      throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterTable(dbName, tblName, table, null);
  }

  @Override
  public void alter_table_with_cascade(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table table, boolean cascade)
      throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterTable(dbName, tblName, table, null);
  }


  @Override
  public void alter_table_with_environment_context(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Table table,
      EnvironmentContext environmentContext
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterTable(dbName, tblName, table, environmentContext);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition append_partition(String dbName, String tblName, List<String> values)
        throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    return glueMetastoreClientDelegate.appendPartition(dbName, tblName, values);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition append_partition_by_name(String dbName, String tblName, String partitionName) throws InvalidObjectException,
        org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    List<String> partVals = partition_name_to_vals(partitionName);
    return glueMetastoreClientDelegate.appendPartition(dbName, tblName, partVals);
  }


  @Override
  public boolean create_role(org.apache.hadoop.hive.metastore.api.Role role) throws MetaException, TException {
    return glueMetastoreClientDelegate.createRole(role);
  }

  @Override
  public boolean drop_role(String roleName) throws MetaException, TException {
    return glueMetastoreClientDelegate.dropRole(roleName);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Role> list_roles(
      String principalName, org.apache.hadoop.hive.metastore.api.PrincipalType principalType
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.listRoles(principalName, principalType);
  }

  @Override
  public List<String> get_role_names() throws MetaException, TException {
    return glueMetastoreClientDelegate.listRoleNames();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse get_principals_in_role(
      org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest request) throws MetaException, TException {
    return glueMetastoreClientDelegate.getPrincipalsInRole(request);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest request) throws MetaException, TException {
    return glueMetastoreClientDelegate.getRoleGrantsForPrincipal(request);
  }

  @Override
  public boolean grant_role(
      String roleName,
      String userName,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      String grantor, org.apache.hadoop.hive.metastore.api.PrincipalType grantorType,
      boolean grantOption
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.grantRole(roleName, userName, principalType, grantor, grantorType, grantOption);
  }

  @Override
  public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request) throws MetaException, TException {
    boolean b = glueMetastoreClientDelegate.revokeRole(request.getRoleName(), request.getPrincipalName(), request.getPrincipalType(), true);
    GrantRevokeRoleResponse response = new GrantRevokeRoleResponse();
    response.setSuccess(b);
    return response;
  }


  @Override
  public void cancel_delegation_token(String tokenStrForm) throws MetaException, TException {
    glueMetastoreClientDelegate.cancelDelegationToken(tokenStrForm);
  }

  @Override
  public boolean add_token(String tokenIdentifier, String delegationToken) throws TException {
    return glueMetastoreClientDelegate.addToken(tokenIdentifier, delegationToken);
  }

  @Override
  public boolean remove_token(String tokenIdentifier) throws TException {
    return glueMetastoreClientDelegate.removeToken(tokenIdentifier);
  }

  @Override
  public String get_token(String tokenIdentifier) throws TException {
    return glueMetastoreClientDelegate.getToken(tokenIdentifier);
  }

  @Override
  public List<String> get_all_token_identifiers()
          throws TException
  {
    return glueMetastoreClientDelegate.getAllTokenIdentifiers();
  }

  @Override
  public int add_master_key(String key) throws MetaException, TException {
    return glueMetastoreClientDelegate.addMasterKey(key);
  }

  @Override
  public void update_master_key(int seqNo, String key) throws NoSuchObjectException, MetaException, TException {
    glueMetastoreClientDelegate.updateMasterKey(seqNo, key);
  }

  @Override
  public boolean remove_master_key(int keySeq) throws TException {
    return glueMetastoreClientDelegate.removeMasterKey(keySeq);
  }

  @Override
  public List<String> get_master_keys()
          throws TException
  {
      return Arrays.asList(glueMetastoreClientDelegate.getMasterKeys());
  }


  @Override
  public LockResponse check_lock(CheckLockRequest checkLockRequest)
          throws TException
  {
    return glueMetastoreClientDelegate.checkLock(checkLockRequest.getLockid());
  }

  @Override
  public void close() {
    currentMetaVars = null;
  }


  @Override
  public void commit_txn(CommitTxnRequest commitTxnRequest)
          throws TException
  {
    glueMetastoreClientDelegate.commitTxn(commitTxnRequest.getTxnid());
  }

  @Override
  public void abort_txns(AbortTxnsRequest abortTxnsRequest)
          throws TException
  {
    glueMetastoreClientDelegate.abortTxns(abortTxnsRequest.getTxn_ids());
  }

  @Deprecated
  public void compact(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType
  ) throws TException {
    glueMetastoreClientDelegate.compact(dbName, tblName, partitionName, compactionType);
  }

  @Deprecated
  public void compact(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType,
      Map<String, String> tblProperties
  ) throws TException {
    glueMetastoreClientDelegate.compact(dbName, tblName, partitionName, compactionType, tblProperties);
  }


  @Override
  public CompactionResponse compact2(CompactionRequest compactionRequest)
          throws TException
  {
    String dbName = compactionRequest.getDbname();
    String tblName = compactionRequest.getTablename();
    String partitionName = compactionRequest.getPartitionname();
    CompactionType compactionType = compactionRequest.getType();
    Map<String,String> tblProperties = compactionRequest.getProperties();
    return glueMetastoreClientDelegate.compact2(dbName, tblName, partitionName, compactionType, tblProperties);
  }


  @Override
  public void create_function(Function function)
          throws TException
  {
    glueMetastoreClientDelegate.createFunction(function);
  }



  @Override
  public void create_table(org.apache.hadoop.hive.metastore.api.Table tbl) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, InvalidObjectException, MetaException,
        NoSuchObjectException, TException {
    glueMetastoreClientDelegate.createTable(tbl);
  }


  @Override
  public boolean delete_partition_column_statistics(
      String dbName, String tableName, String partName, String colName
  ) throws NoSuchObjectException, MetaException, InvalidObjectException,
      TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
  }


  @Override
  public boolean delete_table_column_statistics(
      String dbName, String tableName, String colName
  ) throws NoSuchObjectException, MetaException, InvalidObjectException,
      TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public void drop_function(String dbName, String functionName) throws MetaException, NoSuchObjectException,
        InvalidObjectException, org.apache.hadoop.hive.metastore.api.InvalidInputException, TException {
    glueMetastoreClientDelegate.dropFunction(dbName, functionName);
  }

  private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
    if (depth > 0 && parent != null && wh.isWritable(parent) && wh.isEmpty(parent)) {
      wh.deleteDir(parent, true, mustPurge);
      deleteParentRecursive(parent.getParent(), depth - 1, mustPurge);
    }
  }

  // This logic is taken from HiveMetaStore#isMustPurge
  private boolean isMustPurge(org.apache.hadoop.hive.metastore.api.Table table, boolean ifPurge) {
    return (ifPurge || "true".equalsIgnoreCase(table.getParameters().get("auto.purge")));
  }

  @Override
  public boolean drop_partition(String dbName, String tblName, List<String> values, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
  }


  @Override
  public boolean drop_partition_by_name_with_environment_context(String dbName, String tblName, String part_name, boolean deleteData, EnvironmentContext environmentContext)
          throws TException
  {
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, Arrays.asList(part_name), false /*context.ifExists*/, deleteData, false /*options.purgeData*/);
  }

  @Override
  public boolean drop_partition_with_environment_context(String dbName, String tblName, List<String> part_vals, boolean deleteData, EnvironmentContext environmentContext)
          throws TException
  {

    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, part_vals, false /*context.ifExists*/, deleteData, false /*options.purgeData*/);
  }

  @Override
  public boolean drop_partition_by_name(String dbName, String tblName, String partitionName, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    List<String> values = partition_name_to_vals(partitionName);
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
  }


  @Override
  public DropPartitionsResult drop_partitions_req(DropPartitionsRequest dropPartitionsRequest)
          throws TException
  {
    //TODO:check this method
    String dbName=dropPartitionsRequest.getDbName();
    String tblName = dropPartitionsRequest.getTblName();
    RequestPartsSpec parts = dropPartitionsRequest.getParts();
    List<DropPartitionsExpr> exprs = parts.getExprs();
    List<ObjectPair<Integer, byte[]>> partExprs = exprs.stream().map(
            dropPartitionsExpr ->
                    new ObjectPair(Integer.valueOf(dropPartitionsExpr.getPartArchiveLevel()), dropPartitionsExpr.getExpr())).collect(Collectors.toList());
    boolean deleteData = dropPartitionsRequest.isDeleteData();

    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = dropPartitions_core(dbName, tblName, partExprs, deleteData, false);
    DropPartitionsResult dropPartitionsResult = new DropPartitionsResult();
    dropPartitionsResult.setPartitions(partitions);
    dropPartitionsResult.setFieldValue(DropPartitionsResult._Fields.PARTITIONS,null);
    return dropPartitionsResult;
  }

  private List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions_core(
      String databaseName,
      String tableName,
      List<ObjectPair<Integer, byte[]>> partExprs,
      boolean deleteData,
      boolean purgeData
  ) throws TException {
    List<org.apache.hadoop.hive.metastore.api.Partition> deleted = Lists.newArrayList();
    for (ObjectPair<Integer, byte[]> expr : partExprs) {
      byte[] tmp = expr.getSecond();
      String exprString = ExpressionHelper.convertHiveExpressionToCatalogExpression(tmp);
      List<Partition> catalogPartitionsToDelete = glueMetastoreClientDelegate.getCatalogPartitions(databaseName, tableName, exprString, -1);
      deleted.addAll(batchDeletePartitions(databaseName, tableName, catalogPartitionsToDelete, deleteData, purgeData));
    }
    return deleted;
  }

  /**
   * Delete all partitions in the list provided with BatchDeletePartitions request. It doesn't use transaction,
   * so the call may result in partial failure.
   * @param dbName
   * @param tableName
   * @param partitionsToDelete
   * @return the partitions successfully deleted
   * @throws TException
   */
  private List<org.apache.hadoop.hive.metastore.api.Partition> batchDeletePartitions(
        final String dbName, final String tableName, final List<Partition> partitionsToDelete,
        final boolean deleteData, final boolean purgeData) throws TException {

    List<org.apache.hadoop.hive.metastore.api.Partition> deleted = Lists.newArrayList();
    if (partitionsToDelete == null) {
      return deleted;
    }

    validateBatchDeletePartitionsArguments(dbName, tableName, partitionsToDelete);

    List<Future<BatchDeletePartitionsHelper>> batchDeletePartitionsFutures = Lists.newArrayList();

    int numOfPartitionsToDelete = partitionsToDelete.size();
    for (int i = 0; i < numOfPartitionsToDelete; i += BATCH_DELETE_PARTITIONS_PAGE_SIZE) {
      int j = Math.min(i + BATCH_DELETE_PARTITIONS_PAGE_SIZE, numOfPartitionsToDelete);
      final List<Partition> partitionsOnePage = partitionsToDelete.subList(i, j);

      batchDeletePartitionsFutures.add(BATCH_DELETE_PARTITIONS_THREAD_POOL.submit(new Callable<BatchDeletePartitionsHelper>() {
        @Override
        public BatchDeletePartitionsHelper call() throws Exception {
          return new BatchDeletePartitionsHelper(glueClient, dbName, tableName, catalogId, partitionsOnePage).deletePartitions();
        }
      }));
    }

    TException tException = null;
    for (Future<BatchDeletePartitionsHelper> future : batchDeletePartitionsFutures) {
      try {
        BatchDeletePartitionsHelper batchDeletePartitionsHelper = future.get();
        for (Partition partition : batchDeletePartitionsHelper.getPartitionsDeleted()) {
          org.apache.hadoop.hive.metastore.api.Partition hivePartition =
                CatalogToHiveConverter.convertPartition(partition);
          try {
            performDropPartitionPostProcessing(dbName, tableName, hivePartition, deleteData, purgeData);
          } catch (TException e) {
            logger.error("Drop partition directory failed.", e);
            tException = tException == null ? e : tException;
          }
          deleted.add(hivePartition);
        }
        tException = tException == null ? batchDeletePartitionsHelper.getFirstTException() : tException;
      } catch (Exception e) {
        logger.error("Exception thrown by BatchDeletePartitions thread pool. ", e);
      }
    }

    if (tException != null) {
      throw tException;
    }
    return deleted;
  }

  private void validateBatchDeletePartitionsArguments(final String dbName, final String tableName,
                                                      final List<Partition> partitionsToDelete) {

    Preconditions.checkArgument(dbName != null, "Database name cannot be null");
    Preconditions.checkArgument(tableName != null, "Table name cannot be null");
    for (Partition partition : partitionsToDelete) {
      Preconditions.checkArgument(dbName.equals(partition.getDatabaseName()), "Database name cannot be null");
      Preconditions.checkArgument(tableName.equals(partition.getTableName()), "Table name cannot be null");
      Preconditions.checkArgument(partition.getValues() != null, "Partition values cannot be null");
    }
  }

  // Preserve the logic from Hive metastore
  private void performDropPartitionPostProcessing(String dbName, String tblName,
                                                  org.apache.hadoop.hive.metastore.api.Partition partition, boolean deleteData, boolean ifPurge)
        throws MetaException, NoSuchObjectException, TException {
    if (deleteData && partition.getSd() != null && partition.getSd().getLocation() != null) {
      Path partPath = new Path(partition.getSd().getLocation());
      org.apache.hadoop.hive.metastore.api.Table table = get_table(dbName, tblName);
      if (isExternalTable(table)){
        //Don't delete external table data
        return;
      }
      boolean mustPurge = isMustPurge(table, ifPurge);
      wh.deleteDir(partPath, true, mustPurge);
      try {
        List<String> values = partition.getValues();
        deleteParentRecursive(partPath.getParent(), values.size() - 1, mustPurge);
      } catch (IOException e) {
        throw new MetaException(e.getMessage());
      }
    }
  }



  @Override
  public void drop_table(String dbname, String tableName, boolean deleteData)
          throws TException
  {
    glueMetastoreClientDelegate.dropTable(dbname, tableName, deleteData, true, false);
  }


  @Override
  public void drop_table_with_environment_context(String dbname, String tableName, boolean deleteData, EnvironmentContext environmentContext)
          throws TException
  {
    //TODO: retrieve from context boolean params
    glueMetastoreClientDelegate.dropTable(dbname, tableName, deleteData, true,false);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(
      Map<String, String> partitionSpecs,
      String srcDb,
      String srcTbl,
      String dstDb,
      String dstTbl
  ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return glueMetastoreClientDelegate.exchangePartition(partitionSpecs, srcDb, srcTbl, dstDb, dstTbl);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(
      Map<String, String> partitionSpecs,
      String sourceDb,
      String sourceTbl,
      String destDb,
      String destTbl
  ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return glueMetastoreClientDelegate.exchangePartitions(partitionSpecs, sourceDb, sourceTbl, destDb, destTbl);
  }


  @Override
  public AggrStats get_aggr_stats_for(PartitionsStatsRequest partitionsStatsRequest)
          throws TException
  {
    return glueMetastoreClientDelegate.getAggrColStatsFor(partitionsStatsRequest.getDbName(), partitionsStatsRequest.getTblName(), partitionsStatsRequest.getColNames(), partitionsStatsRequest.getPartNames());
  }


  @Override
  public List<String> get_all_tables(String dbname) throws MetaException, TException, UnknownDBException {
    return get_tables(dbname, ".*");
  }


  @Override
  public String get_config_value(String name, String defaultValue) throws TException, ConfigValSecurityException {
    if(!Pattern.matches("(hive|hdfs|mapred).*", name)) {
      throw new ConfigValSecurityException("For security reasons, the config key " + name + " cannot be accessed");
    }

    return conf.get(name, defaultValue);
  }


  @Override
  public String get_delegation_token(
      String owner, String renewerKerberosPrincipalName
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.getDelegationToken(owner, renewerKerberosPrincipalName);
  }


  @Override
  public List<FieldSchema> get_fields(String db, String tableName) throws MetaException, TException,
        UnknownTableException, UnknownDBException {
    return glueMetastoreClientDelegate.getFields(db, tableName);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Function get_function(String dbName, String functionName) throws MetaException, TException {
    return glueMetastoreClientDelegate.getFunction(dbName, functionName);
  }

  @Override
  public List<String> get_functions(String dbName, String pattern) throws MetaException, TException {
    return glueMetastoreClientDelegate.getFunctions(dbName, pattern);
  }

  @Override
  public GetAllFunctionsResponse get_all_functions() throws MetaException, TException {
    List<String> databaseNames = get_databases(".*");
    List<org.apache.hadoop.hive.metastore.api.Function> result = new ArrayList<>();
    try {
      for (String databaseName : databaseNames) {
        GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest = new GetUserDefinedFunctionsRequest()
            .withDatabaseName(databaseName).withPattern(".*").withCatalogId(catalogId);
        
        List<UserDefinedFunction> catalogFunctions = glueClient.getUserDefinedFunctions(
            getUserDefinedFunctionsRequest)
            .getUserDefinedFunctions();
        for (UserDefinedFunction catalogFunction : catalogFunctions) {
          result.add(CatalogToHiveConverter.convertFunction(databaseName, catalogFunction));
        }
      }

      GetAllFunctionsResponse response = new GetAllFunctionsResponse();
      response.setFunctions(result);
      return response;
    } catch (AmazonServiceException e) {
      logger.error(e);
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get Functions: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }


  @Override
  public String getMetaConf(String key) throws MetaException, TException {
    ConfVars metaConfVar = HiveConf.getMetaConf(key);
    if (metaConfVar == null) {
      throw new MetaException("Invalid configuration key " + key);
    }
    return conf.get(key, metaConfVar.getDefaultValue());
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition get_partition(String dbName, String tblName, List<String> values)
      throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartition(dbName, tblName, values);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition get_partition_by_name(String dbName, String tblName, String partitionName)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.getPartition(dbName, tblName, partitionName);
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(
      String dbName,
      String tableName,
      String partitionNames,
      String columnNames
  ) throws NoSuchObjectException, MetaException, TException
  {
    Map<String, List<ColumnStatisticsObj>> partitionColumnStatistics = glueMetastoreClientDelegate.getPartitionColumnStatistics(dbName, tableName, Arrays.asList(partitionNames), Arrays.asList(columnNames));
    ColumnStatisticsObj columnStatisticsObj = partitionColumnStatistics.get(tableName).get(0);


    ColumnStatistics columnStatistics = new ColumnStatistics();
    columnStatistics.setStatsObj(Arrays.asList(columnStatisticsObj));
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setCatName(DEFAULT_CATALOG_NAME);
    statsDesc.setDbName(dbName);
    statsDesc.setTableName(tableName);
    columnStatistics.setStatsDesc(statsDesc);
    return  columnStatistics;

  }


  @Override
  public org.apache.hadoop.hive.metastore.api.Partition get_partition_with_auth(
        String databaseName, String tableName, List<String> values,
        String userName, List<String> groupNames)
        throws MetaException, UnknownTableException, NoSuchObjectException, TException {

    // TODO move this into the service
    org.apache.hadoop.hive.metastore.api.Partition partition = get_partition(databaseName, tableName, values);
    org.apache.hadoop.hive.metastore.api.Table table = get_table(databaseName, tableName);
    if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
      String partName = Warehouse.makePartName(table.getPartitionKeys(), values);
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(databaseName);
      obj.setObjectName(tableName);
      obj.setPartValues(values);
      org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet privilegeSet =
            this.get_privilege_set(obj, userName, groupNames);
      partition.setPrivileges(privilegeSet);
    }

    return partition;
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> get_partitions_by_names(
        String databaseName, String tableName, List<String> partitionNames)
        throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartitionsByNames(databaseName, tableName, partitionNames);
  }

  @Override
  public List<FieldSchema> get_schema(String db, String tableName) throws MetaException, TException, UnknownTableException,
        UnknownDBException {
    return glueMetastoreClientDelegate.getSchema(db, tableName);
  }

  @Deprecated
  public org.apache.hadoop.hive.metastore.api.Table get_table(String tableName) throws MetaException, TException, NoSuchObjectException {
    //this has been deprecated
    return get_table(DEFAULT_DATABASE_NAME, tableName);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table get_table(String dbName, String tableName)
        throws MetaException, TException, NoSuchObjectException {
    return glueMetastoreClientDelegate.getTable(dbName, tableName);
  }

  @Override
  public ColumnStatistics get_table_column_statistics(String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, TException {
    List<ColumnStatisticsObj> tableColumnStatistics = glueMetastoreClientDelegate.getTableColumnStatistics(dbName, tableName, Arrays.asList(colName));
    ColumnStatistics columnStatistics = new ColumnStatistics();
    columnStatistics.setStatsObj(Arrays.asList(tableColumnStatistics.get(0)));
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setCatName(DEFAULT_CATALOG_NAME);
    statsDesc.setDbName(dbName);
    statsDesc.setTableName(tableName);
    columnStatistics.setStatsDesc(statsDesc);
    return  columnStatistics;

  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Table> get_table_objects_by_name(String dbName, List<String> tableNames) throws MetaException,
        InvalidOperationException, UnknownDBException, TException {
    List<org.apache.hadoop.hive.metastore.api.Table> hiveTables = Lists.newArrayList();
    for(String tableName : tableNames) {
      hiveTables.add(get_table(dbName, tableName));
    }

    return hiveTables;
  }

  @Override
  public List<String> get_tables(String dbname, String tablePattern) throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTables(dbname, tablePattern);
  }

  @Override
  public List<String> get_tables_by_type(String dbname, String tablePattern, String tableType)
      throws MetaException, TException, UnknownDBException {

    return glueMetastoreClientDelegate.getTables(dbname, tablePattern, TableType.valueOf(tableType));
  }

  @Override
  public List<TableMeta> get_table_meta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTableMeta(dbPatterns, tablePatterns, tableTypes);
  }


  public GetOpenTxnsResponse get_open_txns() throws TException
  {
    //FIXME:unsupported
    return null;
  }
  //TODO
//  @Override
//  public ValidTxnList getValidTxns() throws TException {
//    return glueMetastoreClientDelegate.getValidTxns();
//  }
//
//
//  @Override
//  public ValidTxnList getValidTxns(long currentTxn) throws TException {
//    return glueMetastoreClientDelegate.getValidTxns(currentTxn);
//  }

  @Override
  public org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet get_privilege_set(
      HiveObjectRef obj,
      String user, List<String> groups
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.getPrivilegeSet(obj, user, groups);
  }

  @Override
  public boolean grant_privileges(org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges)
      throws MetaException, TException {
    return glueMetastoreClientDelegate.grantPrivileges(privileges);
  }

  @Override
  public GrantRevokePrivilegeResponse grant_revoke_privileges(
          GrantRevokePrivilegeRequest privileges
  ) throws MetaException, TException {
    boolean b = glueMetastoreClientDelegate.revokePrivileges(privileges.getPrivileges(), true);
    GrantRevokePrivilegeResponse grantRevokePrivilegeResponse = new GrantRevokePrivilegeResponse();
    grantRevokePrivilegeResponse.setSuccess(b);
    return grantRevokePrivilegeResponse;
  }

  @Override
  public void heartbeat(HeartbeatRequest request)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    glueMetastoreClientDelegate.heartbeat(request.getTxnid(), request.getTxnid());
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest request) throws TException {
    HeartbeatTxnRangeResponse heartbeatTxnRangeResponse = glueMetastoreClientDelegate.heartbeatTxnRange(request.getMin(), request.getMax());
    return heartbeatTxnRangeResponse;
  }

  private void snapshotActiveConf() {
    currentMetaVars = new HashMap<String, String>(HiveConf.metaVars.length);
    for (ConfVars oneVar : HiveConf.metaVars) {
      currentMetaVars.put(oneVar.varname, conf.get(oneVar.varname, ""));
    }
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partKVs, PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {
    return glueMetastoreClientDelegate.isPartitionMarkedForEvent(dbName, tblName, partKVs, eventType);
  }

  @Override
  public List<String> get_partition_names(String dbName, String tblName, short max)
        throws MetaException, TException {
    try {
      return get_partition_names_ps(dbName, tblName, null, max);
    } catch (NoSuchObjectException e) {
      // For compatibility with Hive 1.0.0
      return Collections.emptyList();
    }
  }

  @Override
  public List<String> get_partition_names_ps(String databaseName, String tableName,
                                         List<String> values, short max)
        throws MetaException, TException, NoSuchObjectException {
    return glueMetastoreClientDelegate.listPartitionNames(databaseName, tableName, values, max);
  }

  @Override
  public int get_num_partitions_by_filter(String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.getNumPartitionsByFilter(dbName, tableName, filter);
  }


  @Override
  public List<PartitionSpec> get_partitions_pspec(String dbName, String tblName, int max) throws TException {
    PartitionSpecProxy partitionSpecProxy = glueMetastoreClientDelegate.listPartitionSpecs(dbName, tblName, max);
    return partitionSpecProxy.toPartitionSpec();
  }

  @Override
  public List<PartitionSpec> get_part_specs_by_filter(String dbName, String tblName, String filter, int max)
      throws MetaException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.listPartitionSpecsByFilter(dbName, tblName, filter, max).toPartitionSpec();
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> get_partitions(String dbName, String tblName, short max)
      throws NoSuchObjectException, MetaException, TException {
    return get_partitions_ps(dbName, tblName, null, max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> get_partitions_ps(
      String databaseName,
      String tableName,
      List<String> values,
      short max
  ) throws NoSuchObjectException, MetaException, TException {
    String expression = null;
    if (values != null) {
      org.apache.hadoop.hive.metastore.api.Table table = get_table(databaseName, tableName);
      expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, values);
    }
    return glueMetastoreClientDelegate.getPartitions(databaseName, tableName, expression, (long) max);
  }

  @Override
  public PartitionsByExprResult get_partitions_by_expr(PartitionsByExprRequest partitionsByExprRequest) throws TException {

    String catalogExpression =  ExpressionHelper.convertHiveExpressionToCatalogExpression(partitionsByExprRequest.getExpr());
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions =
        glueMetastoreClientDelegate.getPartitions(partitionsByExprRequest.getDbName(), partitionsByExprRequest.getTblName(), catalogExpression, (long) partitionsByExprRequest.getMaxParts());
    PartitionsByExprResult partitionsByExprResult = new PartitionsByExprResult();
    partitionsByExprResult.setPartitions(partitions);
    //TODO: check this values
    partitionsByExprResult.setHasUnknownPartitions(false);
//    return false;
    return partitionsByExprResult;
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> get_partitions_by_filter(
      String databaseName,
      String tableName,
      String filter,
      short max
  ) throws MetaException, NoSuchObjectException, TException {
    // we need to replace double quotes with single quotes in the filter expression
    // since server side does not accept double quote expressions.
    if (StringUtils.isNotBlank(filter)) {
      filter = ExpressionHelper.replaceDoubleQuoteWithSingleQuotes(filter);
    }
    return glueMetastoreClientDelegate.getPartitions(databaseName, tableName, filter, (long) max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> get_partitions_with_auth(String database, String table, short maxParts,
                                                                                         String user, List<String> groups)
        throws MetaException, TException, NoSuchObjectException {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = get_partitions(database, table, maxParts);

    for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(database);
      obj.setObjectName(table);
      obj.setPartValues(p.getValues());
      org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet set = this.get_privilege_set(obj, user, groups);
      p.setPrivileges(set);
    }

    return partitions;
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> get_partitions_ps_with_auth(String database, String table,
                                                                                         List<String> partVals, short maxParts,
                                                                                         String user, List<String> groups) throws MetaException, TException, NoSuchObjectException {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = get_partitions_ps(database, table, partVals, maxParts);

    for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(database);
      obj.setObjectName(table);
      obj.setPartValues(p.getValues());
      org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet set;
      try {
        set = get_privilege_set(obj, user, groups);
      } catch (MetaException e) {
        logger.info(String.format("No privileges found for user: %s, "
              + "groups: [%s]", user, LoggingHelper.concatCollectionToStringForLogging(groups, ",")));
        set = new org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet();
      }
      p.setPrivileges(set);
    }

    return partitions;
  }

  @Override
  public List<String> get_table_names_by_filter(String dbName, String filter, short maxTables) throws MetaException,
        TException, InvalidOperationException, UnknownDBException {
    return glueMetastoreClientDelegate.listTableNamesByFilter(dbName, filter, maxTables);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(
      String principal,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      HiveObjectRef objectRef
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.listPrivileges(principal, principalType, objectRef);
  }

  @Override
  public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
    return glueMetastoreClientDelegate.lock(lockRequest);
  }

  @Override
  public void markPartitionForEvent(
      String dbName,
      String tblName,
      Map<String, String> partKVs,
      PartitionEventType eventType
  ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {
    glueMetastoreClientDelegate.markPartitionForEvent(dbName, tblName, partKVs, eventType);
  }

  @Override
  public OpenTxnsResponse open_txns(OpenTxnRequest request) throws TException {
    return glueMetastoreClientDelegate.openTxns(request.getUser(), request.getNum_txns());
  }

  @Override
  public Map<String, String> partition_name_to_spec(String name) throws MetaException, TException {
    // Lifted from HiveMetaStore
    if (name.length() == 0) {
      return new HashMap<String, String>();
    }
    return Warehouse.makeSpecFromName(name);
  }

  @Override
  public List<String> partition_name_to_vals(String name) throws MetaException, TException {
    return glueMetastoreClientDelegate.partitionNameToVals(name);
  }

//  @Override
//  public void reconnect() throws MetaException {
//    // TODO reset active Hive confs for metastore glueClient
//    logger.debug("reconnect() was called.");
//  }

  @Override
  public void rename_partition(String dbName, String tblName, List<String> partitionValues,
                              org.apache.hadoop.hive.metastore.api.Partition newPartition)
        throws InvalidOperationException, MetaException, TException {

    // Set DDL time to now if not specified
    setDDLTime(newPartition);
    org.apache.hadoop.hive.metastore.api.Table tbl;
    org.apache.hadoop.hive.metastore.api.Partition oldPart;

    try {
      tbl = get_table(dbName, tblName);
      oldPart = get_partition(dbName, tblName, partitionValues);
    } catch(NoSuchObjectException e) {
      throw new InvalidOperationException(e.getMessage());
    }

    if(newPartition.getSd() == null || oldPart.getSd() == null ) {
      throw new InvalidOperationException("Storage descriptor cannot be null");
    }

    // if an external partition is renamed, the location should not change
    if (!Strings.isNullOrEmpty(tbl.getTableType()) && tbl.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
      newPartition.getSd().setLocation(oldPart.getSd().getLocation());
      renamePartitionInCatalog(dbName, tblName, partitionValues, newPartition);
    } else {

      Path destPath = getDestinationPathForRename(dbName, tbl, newPartition);
      Path srcPath = new Path(oldPart.getSd().getLocation());
      FileSystem srcFs = wh.getFs(srcPath);
      FileSystem destFs = wh.getFs(destPath);

      verifyDestinationLocation(srcFs, destFs, srcPath, destPath, tbl, newPartition);
      newPartition.getSd().setLocation(destPath.toString());

      renamePartitionInCatalog(dbName, tblName, partitionValues, newPartition);
      boolean success = true;
      try{
        if (srcFs.exists(srcPath)) {
          //if destPath's parent path doesn't exist, we should mkdir it
          Path destParentPath = destPath.getParent();
          if (!wh.mkdirs(destParentPath)) {
            throw new IOException("Unable to create path " + destParentPath);
          }
          wh.renameDir(srcPath, destPath, true);
        }
      } catch (IOException e) {
        success = false;
        throw new InvalidOperationException("Unable to access old location "
              + srcPath + " for partition " + tbl.getDbName() + "."
              + tbl.getTableName() + " " + partitionValues);
      } finally {
        if(!success) {
          // revert metastore operation
          renamePartitionInCatalog(dbName, tblName, newPartition.getValues(), oldPart);
        }
      }
    }
  }

  private void verifyDestinationLocation(FileSystem srcFs, FileSystem destFs, Path srcPath, Path destPath, org.apache.hadoop.hive.metastore.api.Table tbl, org.apache.hadoop.hive.metastore.api.Partition newPartition)
        throws InvalidOperationException {
    String oldPartLoc = srcPath.toString();
    String newPartLoc = destPath.toString();

    // check that src and dest are on the same file system
    if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
      throw new InvalidOperationException("table new location " + destPath
            + " is on a different file system than the old location "
            + srcPath + ". This operation is not supported");
    }
    try {
      srcFs.exists(srcPath); // check that src exists and also checks
      if (newPartLoc.compareTo(oldPartLoc) != 0 && destFs.exists(destPath)) {
        throw new InvalidOperationException("New location for this partition "
              + tbl.getDbName() + "." + tbl.getTableName() + "." + newPartition.getValues()
              + " already exists : " + destPath);
      }
    } catch (IOException e) {
      throw new InvalidOperationException("Unable to access new location "
            + destPath + " for partition " + tbl.getDbName() + "."
            + tbl.getTableName() + " " + newPartition.getValues());
    }
  }

  private Path getDestinationPathForRename(String dbName, org.apache.hadoop.hive.metastore.api.Table tbl, org.apache.hadoop.hive.metastore.api.Partition newPartition)
        throws InvalidOperationException, MetaException, TException {
    try {
      Path destPath = new Path(hiveShims.getDefaultTablePath(get_database(dbName), tbl.getTableName(), wh),
            Warehouse.makePartName(tbl.getPartitionKeys(), newPartition.getValues()));
      return constructRenamedPath(destPath, new Path(newPartition.getSd().getLocation()));
    } catch (NoSuchObjectException e) {
      throw new InvalidOperationException(
            "Unable to change partition or table. Database " + dbName + " does not exist"
                  + " Check metastore logs for detailed stack." + e.getMessage());
    }
  }

  private void setDDLTime(org.apache.hadoop.hive.metastore.api.Partition partition) {
    if (partition.getParameters() == null ||
          partition.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
          Integer.parseInt(partition.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
      partition.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
            .currentTimeMillis() / 1000));
    }
  }

  private void renamePartitionInCatalog(String databaseName, String tableName,
                                        List<String> partitionValues, org.apache.hadoop.hive.metastore.api.Partition newPartition)
        throws InvalidOperationException, MetaException, TException {
    try {
      glueClient.updatePartition(
          new UpdatePartitionRequest()
          .withDatabaseName(databaseName)
          .withTableName(tableName)
          .withPartitionValueList(partitionValues)
          .withPartitionInput(GlueInputConverter.convertToPartitionInput(newPartition)));
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    }
  }

  @Override
  public long renew_delegation_token(String tokenStrForm) throws MetaException, TException {
    return glueMetastoreClientDelegate.renewDelegationToken(tokenStrForm);
  }

  @Override
  public void abort_txn(AbortTxnRequest request) throws NoSuchTxnException, TException {
    glueMetastoreClientDelegate.rollbackTxn(request.getTxnid());
  }

  @Override
  public void setMetaConf(String key, String value) throws MetaException, TException {
    ConfVars confVar = HiveConf.getMetaConf(key);
    if (confVar == null) {
      throw new MetaException("Invalid configuration key " + key);
    }
    String validate = confVar.validate(value);
    if (validate != null) {
      throw new MetaException("Invalid configuration value " + value + " for key " + key + " by " + validate);
    }
    conf.set(key, value);
  }

  @Override
  public void flushCache() {
    //no op
  }

  @Override
  public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest request) throws TException {
    List<Long> fileIds = request.getFileIds();
    Iterable<Map.Entry<Long, ByteBuffer>> fileMetadata = glueMetastoreClientDelegate.getFileMetadata(fileIds);
    GetFileMetadataResult fileMetadataResult = new GetFileMetadataResult();
    fileMetadata.forEach(longByteBufferEntry -> fileMetadataResult.putToMetadata(longByteBufferEntry.getKey(), longByteBufferEntry.getValue()));
    return fileMetadataResult;
  }

  public  GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest request) throws TException{
    Iterable<Map.Entry<Long, MetadataPpdResult>> fileMetadataBySarg =
            glueMetastoreClientDelegate.getFileMetadataBySarg(request.getFileIds(), ByteBuffer.wrap(request.getExpr()), request.isDoGetFooters());
    GetFileMetadataByExprResult result = new GetFileMetadataByExprResult();
    fileMetadataBySarg.forEach(
            longMetadataPpdResultEntry -> result.putToMetadata(longMetadataPpdResultEntry.getKey(),longMetadataPpdResultEntry.getValue()));
    return result;
  }

  @Override
  public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest request) throws TException {
    glueMetastoreClientDelegate.clearFileMetadata(request.getFileIds());
    //TODO:check return values
    return new ClearFileMetadataResult();

  }

  @Override
  public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest request) throws TException {
    glueMetastoreClientDelegate.putFileMetadata(request.getFileIds(), request.getMetadata());
    //TODO:check return values
    return new PutFileMetadataResult();
  }


  @Override
  public CacheFileMetadataResult cache_file_metadata(CacheFileMetadataRequest request) throws TException {
    boolean cacheFileMetadata = glueMetastoreClientDelegate.cacheFileMetadata(request.getDbName(), request.getTblName(), request.getPartName(), request.isIsAllParts());
    CacheFileMetadataResult result = new CacheFileMetadataResult();
    //TODO:check return values
    result.setIsSupported(cacheFileMetadata);
    return result;
  }

  @Override
  public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest primaryKeysRequest) throws MetaException, NoSuchObjectException, TException {
    // PrimaryKeys are currently unsupported
    //return null to allow DESCRIBE (FORMATTED | EXTENDED)
    return null;
  }

  @Override
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest foreignKeysRequest) throws MetaException, NoSuchObjectException, TException {
    // PrimaryKeys are currently unsupported
    //return null to allow DESCRIBE (FORMATTED | EXTENDED)
    return null;
  }

  @Override
  public void create_table_with_constraints(
      org.apache.hadoop.hive.metastore.api.Table table,
      List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint>  sqlNotNullConstraints,
      List<SQLDefaultConstraint> sqlDefaultConstraints,
      List<SQLCheckConstraint> sqlCheckConstraints
  ) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    //TODO:add warning log for unsupported
    glueMetastoreClientDelegate.createTableWithConstraints(table, primaryKeys, foreignKeys);
  }

  @Override
  public void drop_constraint(DropConstraintRequest request) throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.dropConstraint(request.getDbname(),request.getTablename(), request.getConstraintname());
  }

  @Override
  public void add_primary_key(AddPrimaryKeyRequest request)
      throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.addPrimaryKey(request.getPrimaryKeyCols());
  }

  @Override
  public void add_foreign_key(AddForeignKeyRequest request)
      throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.addForeignKey(request.getForeignKeyCols());
  }

  @Override
  public ShowCompactResponse show_compact(ShowCompactRequest request) throws TException {
    return glueMetastoreClientDelegate.showCompactions();
  }

  @Override
  public void add_dynamic_partitions(AddDynamicPartitions request) throws TException {
    glueMetastoreClientDelegate.addDynamicPartitions(request.getTxnid(),request.getDbname(),request.getTablename(),request.getPartitionnames());
  }


  @Override
  public NotificationEventResponse get_next_notification(NotificationEventRequest request) throws TException {
    return glueMetastoreClientDelegate.getNextNotification(request.getLastEvent(), request.getMaxEvents(),null);
  }

  @Override
  public CurrentNotificationEventId get_current_notificationEventId() throws TException {
    return glueMetastoreClientDelegate.getCurrentNotificationEventId();
  }

  @Override
  public FireEventResponse fire_listener_event(FireEventRequest fireEventRequest) throws TException {
    return glueMetastoreClientDelegate.fireListenerEvent(fireEventRequest);
  }

  @Override
  public ShowLocksResponse show_locks(ShowLocksRequest request) throws TException {
    return glueMetastoreClientDelegate.showLocks();
  }

  @Override
  public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
    return glueMetastoreClientDelegate.showTxns();
  }


  @Override
  public void unlock(UnlockRequest request) throws NoSuchLockException, TxnOpenException, TException {
    glueMetastoreClientDelegate.unlock(request.getLockid());
  }

  @Override
  public boolean update_partition_column_statistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.updatePartitionColumnStatistics(columnStatistics);
  }

  @Override
  public boolean update_table_column_statistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.updateTableColumnStatistics(columnStatistics);
  }


  private Path constructRenamedPath(Path defaultNewPath, Path currentPath) {
    URI currentUri = currentPath.toUri();

    return new Path(currentUri.getScheme(), currentUri.getAuthority(),
          defaultNewPath.toUri().getPath());
  }

}
