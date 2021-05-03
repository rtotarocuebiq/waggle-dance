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
package com.hotels.bdp.waggledance.client.wd.minihms;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.RemoteMetaStoreForTests;
import org.junit.rules.TemporaryFolder;

import com.hotels.bdp.waggledance.WaggleDanceRunner;
import com.hotels.bdp.waggledance.api.model.AccessControlType;

public abstract class AbstractWaggleDanceRemoteService
        extends RemoteMetaStoreForTests
{

    private WaggleDanceRunner runner;

    private int port;

    public AbstractWaggleDanceRemoteService(Configuration configuration)
    {
        super(configuration);
    }

    @Override
    public void start()
            throws Exception
    {
        super.start();
        String uri = getConfiguration().get(MetastoreConf.ConfVars.THRIFT_URIS.getVarname());
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        port = MetaStoreTestUtils.findFreePort();
        WaggleDanceRunner.Builder builder = WaggleDanceRunner.builder(temporaryFolder.newFolder("config"))
                .port(port)
                .primary("remote", uri, AccessControlType.READ_AND_WRITE_AND_CREATE, "*");

        extraBuilderConfiguration(builder);

        runner = builder.build();
        runWaggleDance(runner);
    }

    private String getWaggleDanceThriftUri()
    {
        return "thrift://localhost:" + port;
    }

    private void runWaggleDance(WaggleDanceRunner runner)
            throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                runner.run();
            }
            catch (RuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new RuntimeException("Error during execution", e);
            }
        });
        runner.waitForService();
    }

    protected abstract void extraBuilderConfiguration(WaggleDanceRunner.Builder builder);

    @Override
    public IMetaStoreClient getClient()
            throws MetaException
    {
        Configuration conf = getConfiguration();
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, getWaggleDanceThriftUri());
        conf.set(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname, "true");
        return new HiveMetaStoreClient(conf);
    }
}
