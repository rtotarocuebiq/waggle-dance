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

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.amazonaws.services.glue.AWSGlue;

public class AWSGlueMetastoreFactory {

    public AWSGlueMetastore newMetastore(HiveConf conf) throws MetaException {
        AWSGlue glueClient = new AWSGlueClientFactory(conf).newClient();
        AWSGlueMetastore defaultMetastore = new DefaultAWSGlueMetastore(conf, glueClient);
        if(isCacheEnabled(conf)) {
            return new AWSGlueMetastoreCacheDecorator(conf, defaultMetastore);
        }
        return defaultMetastore;
    }

    private boolean isCacheEnabled(HiveConf conf) {
        boolean databaseCacheEnabled = conf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        boolean tableCacheEnabled = conf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
        return (databaseCacheEnabled || tableCacheEnabled);
    }
}
