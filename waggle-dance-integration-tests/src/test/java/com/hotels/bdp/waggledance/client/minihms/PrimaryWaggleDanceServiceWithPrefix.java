package com.hotels.bdp.waggledance.client.minihms;

import com.hotels.bdp.waggledance.WaggleDanceRunner;
import com.hotels.bdp.waggledance.api.model.DatabaseResolution;
import org.apache.hadoop.conf.Configuration;

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
