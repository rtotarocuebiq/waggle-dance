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
package com.hotels.bdp.waggledance.server;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterISchemaRequest;
import org.apache.hadoop.hive.metastore.api.CreateCatalogRequest;
import org.apache.hadoop.hive.metastore.api.DropCatalogRequest;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.GetRuntimeStatsRequest;
import org.apache.hadoop.hive.metastore.api.GetSerdeRequest;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MapSchemaVersionToSerdeRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetSchemaVersionStateRequest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.impl.NotifyingFederationService;

public class FederatedHMSHandlerHive3 extends FederatedHMSHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(FederatedHMSHandlerHive3.class);

    FederatedHMSHandlerHive3(MappingEventListener databaseMappingService, NotifyingFederationService notifyingFederationService)
    {
        super(databaseMappingService, notifyingFederationService);
    }

    @Override
    public void create_catalog(CreateCatalogRequest createCatalogRequest)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException
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
}