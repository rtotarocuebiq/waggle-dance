package com.hotels.bdp.waggledance.client.minihms;

import com.hotels.bdp.waggledance.WaggleDanceRunner;
import com.hotels.bdp.waggledance.api.model.AccessControlType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.RemoteMetaStoreForTests;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
