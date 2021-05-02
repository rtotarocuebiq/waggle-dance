package com.hotels.bdp.waggledance.client.minihms;

import com.hotels.bdp.waggledance.WaggleDanceRunner;
import org.apache.hadoop.conf.Configuration;

public class PrimaryWaggleDanceService extends AbstractWaggleDanceRemoteService
{
    public PrimaryWaggleDanceService(Configuration configuration)
    {
        super(configuration);
    }

    @Override
    protected void extraBuilderConfiguration(WaggleDanceRunner.Builder builder)
    {

    }
}
