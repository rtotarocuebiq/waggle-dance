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

import org.apache.hadoop.conf.Configuration;

import com.hotels.bdp.waggledance.WaggleDanceRunner;
import com.hotels.bdp.waggledance.api.model.DatabaseResolution;

public class PrimaryWaggleDanceServiceWithPrefix extends AbstractWaggleDanceRemoteService
{
    private String prefix;

    public PrimaryWaggleDanceServiceWithPrefix(Configuration configuration,String prefix)
    {
        super(configuration);
        this.prefix = prefix;
    }

    @Override
    protected void extraBuilderConfiguration(WaggleDanceRunner.Builder builder)
    {
        if(prefix!=null){
            builder.databaseResolution(DatabaseResolution.PREFIXED).withPrimaryPrefix(prefix);
        }
    }
}