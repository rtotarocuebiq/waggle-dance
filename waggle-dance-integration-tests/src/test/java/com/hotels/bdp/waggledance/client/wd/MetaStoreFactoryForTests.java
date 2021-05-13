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
package com.hotels.bdp.waggledance.client.wd;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;

import com.hotels.bdp.waggledance.client.wd.minihms.PrimaryWaggleDanceService;
import com.hotels.bdp.waggledance.client.wd.minihms.PrimaryWaggleDanceServiceWithPrefix;

/**
 * Factory for creating specific
 * {@link AbstractMetaStoreService} implementations for
 * tests.
 */
public final class MetaStoreFactoryForTests {
  private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;

  private MetaStoreFactoryForTests() {}

  /**
   * We would like to run the tests with 2 MetaStore configurations
   * - Embedded - Where the MetaStore is running in the same thread, and does not use Thrift
   * - Remote - Where the MetaStore is started in a different thread, and uses Thrift for
   * communication
   *
   * Or if the test.hms.client.configs system property is set, it would return a single test
   * MetaStoreService which uses these configs. In this case the MetaStore should be created
   * manually or by an external application.
   * @return The list of the test MetaStoreService implementations usable by @Parameterized
   * .Parameters
   */
  public static List<Object[]> getMetaStores() throws Exception {
    List<Object[]> metaStores = new ArrayList<Object[]>();

    Configuration conf = MetastoreConf.newMetastoreConf();
    // set some values to use for getting conf. vars
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX, 2);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.LIMIT_PARTITION_REQUEST,
        DEFAULT_LIMIT_PARTITION_REQUEST);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    // Do this only on your own peril, and never in the production code
    conf.set("datanucleus.autoCreateTables", "false");

    // Create Remote MetaStore
    conf.set("javax.jdo.option.ConnectionURL",
            "jdbc:derby:memory:${test.tmp.dir}/junit_metastore_db3;create=true");
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TRY_DIRECT_SQL, true);
    AbstractMetaStoreService wd = new PrimaryWaggleDanceService(conf);
    metaStores.add(new Object[] {"WaggleDancePrimary", wd});


    // Create Remote MetaStore
//    conf.set("javax.jdo.option.ConnectionURL",
//            "jdbc:derby:memory:${test.tmp.dir}/junit_metastore_db4;create=true");
//    AbstractMetaStoreService wd_prefix = new PrimaryWaggleDanceServiceWithPrefix(conf,"test_");
//    metaStores.add(new Object[] {"WaggleDancePrimaryWithPrefix", wd_prefix});



    return metaStores;
  }
}