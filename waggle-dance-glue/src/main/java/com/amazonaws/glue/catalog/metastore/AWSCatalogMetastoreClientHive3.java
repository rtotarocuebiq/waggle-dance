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
package com.amazonaws.glue.catalog.metastore;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterCatalogRequest;
import org.apache.hadoop.hive.metastore.api.AlterISchemaRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CreateCatalogRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DropCatalogRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.GetRuntimeStatsRequest;
import org.apache.hadoop.hive.metastore.api.GetSerdeRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MapSchemaVersionToSerdeRequest;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsResult;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.SetSchemaVersionStateRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableStatsResult;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMAlterPoolRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterPoolResponse;
import org.apache.hadoop.hive.metastore.api.WMAlterResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMAlterTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateOrDropTriggerToPoolMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateOrDropTriggerToPoolMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateOrUpdateMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateOrUpdateMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMCreatePoolRequest;
import org.apache.hadoop.hive.metastore.api.WMCreatePoolResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMDropMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMDropMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMDropPoolRequest;
import org.apache.hadoop.hive.metastore.api.WMDropPoolResponse;
import org.apache.hadoop.hive.metastore.api.WMDropResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMDropResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMDropTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMDropTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetAllResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetAllResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetTriggersForResourePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetTriggersForResourePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.thrift.TException;

import com.facebook.fb303.fb_status;

public class AWSCatalogMetastoreClientHive3 extends AWSCatalogMetastoreClient
{

    public AWSCatalogMetastoreClientHive3(Builder builder)
            throws MetaException
    {
        super(builder);
    }

    public AWSCatalogMetastoreClientHive3(HiveConf conf, HiveMetaHookLoader hook)
            throws MetaException
    {
        super(conf, hook);
    }

    @Override
    public boolean isOpen()
    {
        return false;
    }

    @Override
    public void create_catalog(CreateCatalogRequest createCatalogRequest)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException
    {

    }

    @Override
    public void alter_catalog(AlterCatalogRequest alterCatalogRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {

    }

    @Override
    public GetCatalogResponse get_catalog(GetCatalogRequest getCatalogRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public GetCatalogsResponse get_catalogs()
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public void drop_catalog(DropCatalogRequest dropCatalogRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {

    }

    @Override
    public Type get_type(String s)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    public boolean create_type(Type type)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException
    {
        return false;
    }

    @Override
    public boolean drop_type(String s)
            throws MetaException, NoSuchObjectException, TException
    {
        return false;
    }

    @Override
    public Map<String, Type> get_type_all(String s)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public List<FieldSchema> get_fields_with_environment_context(String s, String s1, EnvironmentContext environmentContext)
            throws MetaException, UnknownTableException, UnknownDBException, TException
    {
        return null;
    }

    @Override
    public List<FieldSchema> get_schema_with_environment_context(String s, String s1, EnvironmentContext environmentContext)
            throws MetaException, UnknownTableException, UnknownDBException, TException
    {
        return null;
    }

    @Override
    public void create_table_with_environment_context(Table table, EnvironmentContext environmentContext)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException
    {

    }

    @Override
    public void add_unique_constraint(AddUniqueConstraintRequest addUniqueConstraintRequest)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    public void add_not_null_constraint(AddNotNullConstraintRequest addNotNullConstraintRequest)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    public void add_default_constraint(AddDefaultConstraintRequest addDefaultConstraintRequest)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    public void add_check_constraint(AddCheckConstraintRequest addCheckConstraintRequest)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    public void truncate_table(String s, String s1, List<String> list)
            throws MetaException, TException
    {

    }

    @Override
    public List<String> get_materialized_views_for_rewriting(String s)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public GetTableResult get_table_req(GetTableRequest getTableRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    public GetTablesResult get_table_objects_by_name_req(GetTablesRequest getTablesRequest)
            throws MetaException, InvalidOperationException, UnknownDBException, TException
    {
        return null;
    }

    @Override
    public Materialization get_materialization_invalidation_info(CreationMetadata creationMetadata, String s)
            throws MetaException, InvalidOperationException, UnknownDBException, TException
    {
        return null;
    }

    @Override
    public void update_creation_metadata(String s, String s1, String s2, CreationMetadata creationMetadata)
            throws MetaException, InvalidOperationException, UnknownDBException, TException
    {

    }

    @Override
    public Partition add_partition_with_environment_context(Partition partition, EnvironmentContext environmentContext)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException
    {
        return null;
    }

    @Override
    public Partition append_partition_with_environment_context(String s, String s1, List<String> list, EnvironmentContext environmentContext)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException
    {
        return null;
    }

    @Override
    public Partition append_partition_by_name_with_environment_context(String s, String s1, String s2, EnvironmentContext environmentContext)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException
    {
        return null;
    }

    @Override
    public PartitionValuesResponse get_partition_values(PartitionValuesRequest partitionValuesRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    public boolean partition_name_has_valid_characters(List<String> list, boolean b)
            throws MetaException, TException
    {
        return false;
    }

    @Override
    public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest uniqueConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest notNullConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest defaultConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest checkConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    public TableStatsResult get_table_statistics_req(TableStatsRequest tableStatsRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest partitionsStatsRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public boolean set_aggr_stats_for(SetPartitionsStatsRequest setPartitionsStatsRequest)
            throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException
    {
        return false;
    }

    @Override
    public boolean revoke_role(String s, String s1, PrincipalType principalType)
            throws MetaException, TException
    {
        return false;
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privilegeBag)
            throws MetaException, TException
    {
        return false;
    }

    @Override
    public GrantRevokePrivilegeResponse refresh_privileges(HiveObjectRef hiveObjectRef, String s, GrantRevokePrivilegeRequest grantRevokePrivilegeRequest)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public List<String> set_ugi(String s, List<String> list)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public void repl_tbl_writeid_state(ReplTblWriteIdStateRequest replTblWriteIdStateRequest)
            throws TException
    {

    }

    @Override
    public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest getValidWriteIdsRequest)
            throws NoSuchTxnException, MetaException, TException
    {
        return null;
    }

    @Override
    public AllocateTableWriteIdsResponse allocate_table_write_ids(AllocateTableWriteIdsRequest allocateTableWriteIdsRequest)
            throws NoSuchTxnException, TxnAbortedException, MetaException, TException
    {
        return null;
    }

    @Override
    public void compact(CompactionRequest compactionRequest)
            throws TException
    {

    }

    @Override
    public NotificationEventsCountResponse get_notification_events_count(NotificationEventsCountRequest notificationEventsCountRequest)
            throws TException
    {
        return null;
    }

    @Override
    public CmRecycleResponse cm_recycle(CmRecycleRequest cmRecycleRequest)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public String get_metastore_db_uuid()
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public WMCreateResourcePlanResponse create_resource_plan(WMCreateResourcePlanRequest wmCreateResourcePlanRequest)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMGetResourcePlanResponse get_resource_plan(WMGetResourcePlanRequest wmGetResourcePlanRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMGetActiveResourcePlanResponse get_active_resource_plan(WMGetActiveResourcePlanRequest wmGetActiveResourcePlanRequest)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public WMGetAllResourcePlanResponse get_all_resource_plans(WMGetAllResourcePlanRequest wmGetAllResourcePlanRequest)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public WMAlterResourcePlanResponse alter_resource_plan(WMAlterResourcePlanRequest wmAlterResourcePlanRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMValidateResourcePlanResponse validate_resource_plan(WMValidateResourcePlanRequest wmValidateResourcePlanRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest wmDropResourcePlanRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMCreateTriggerResponse create_wm_trigger(WMCreateTriggerRequest wmCreateTriggerRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMAlterTriggerResponse alter_wm_trigger(WMAlterTriggerRequest wmAlterTriggerRequest)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest wmDropTriggerRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(WMGetTriggersForResourePlanRequest wmGetTriggersForResourePlanRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest wmCreatePoolRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest wmAlterPoolRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest wmDropPoolRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(WMCreateOrUpdateMappingRequest wmCreateOrUpdateMappingRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest wmDropMappingRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(WMCreateOrDropTriggerToPoolMappingRequest wmCreateOrDropTriggerToPoolMappingRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public void create_ischema(ISchema iSchema)
            throws AlreadyExistsException, NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    public void alter_ischema(AlterISchemaRequest alterISchemaRequest)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    public ISchema get_ischema(ISchemaName iSchemaName)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public void drop_ischema(ISchemaName iSchemaName)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {

    }

    @Override
    public void add_schema_version(SchemaVersion schemaVersion)
            throws AlreadyExistsException, NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    public SchemaVersion get_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public SchemaVersion get_schema_latest_version(ISchemaName iSchemaName)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public List<SchemaVersion> get_schema_all_versions(ISchemaName iSchemaName)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public void drop_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    public FindSchemasByColsResp get_schemas_by_cols(FindSchemasByColsRqst findSchemasByColsRqst)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public void map_schema_version_to_serde(MapSchemaVersionToSerdeRequest mapSchemaVersionToSerdeRequest)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    public void set_schema_version_state(SetSchemaVersionStateRequest setSchemaVersionStateRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {

    }

    @Override
    public void add_serde(SerDeInfo serDeInfo)
            throws AlreadyExistsException, MetaException, TException
    {

    }

    @Override
    public SerDeInfo get_serde(GetSerdeRequest getSerdeRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    public LockResponse get_lock_materialization_rebuild(String s, String s1, long l)
            throws TException
    {
        return null;
    }

    @Override
    public boolean heartbeat_lock_materialization_rebuild(String s, String s1, long l)
            throws TException
    {
        return false;
    }

    @Override
    public void add_runtime_stats(RuntimeStat runtimeStat)
            throws MetaException, TException
    {

    }

    @Override
    public List<RuntimeStat> get_runtime_stats(GetRuntimeStatsRequest getRuntimeStatsRequest)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    public String getName()
            throws TException
    {
        return null;
    }

    @Override
    public String getVersion()
            throws TException
    {
        return null;
    }

    @Override
    public fb_status getStatus()
            throws TException
    {
        return null;
    }

    @Override
    public String getStatusDetails()
            throws TException
    {
        return null;
    }

    @Override
    public Map<String, Long> getCounters()
            throws TException
    {
        return null;
    }

    @Override
    public long getCounter(String s)
            throws TException
    {
        return 0;
    }

    @Override
    public void setOption(String s, String s1)
            throws TException
    {

    }

    @Override
    public String getOption(String s)
            throws TException
    {
        return null;
    }

    @Override
    public Map<String, String> getOptions()
            throws TException
    {
        return null;
    }

    @Override
    public String getCpuProfile(int i)
            throws TException
    {
        return null;
    }

    @Override
    public long aliveSince()
            throws TException
    {
        return 0;
    }

    @Override
    public void reinitialize()
            throws TException
    {

    }

    @Override
    public void shutdown()
            throws TException
    {

    }
}
