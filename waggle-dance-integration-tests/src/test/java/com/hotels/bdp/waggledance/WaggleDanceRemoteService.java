package com.hotels.bdp.waggledance;

import com.hotels.bdp.waggledance.api.model.AccessControlType;
import com.hotels.bdp.waggledance.server.MetaStoreProxyServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.minihms.RemoteMetaStoreForTests;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WaggleDanceRemoteService extends RemoteMetaStoreForTests
{

    private WaggleDanceRunner runner;

    public WaggleDanceRemoteService(Configuration configuration)
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
        runner = WaggleDanceRunner.builder(temporaryFolder.newFolder("config"))
                .primary("remote",uri, AccessControlType.READ_AND_WRITE_AND_CREATE,"*")
                .build();
        runWaggleDance(runner);

    }

    private String getWaggleDanceThriftUri() {
        return "thrift://localhost:" + MetaStoreProxyServer.DEFAULT_WAGGLEDANCE_PORT;
    }

    private void runWaggleDance(WaggleDanceRunner runner) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                runner.run();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Error during execution", e);
            }
        });
        runner.waitForService();
    }

    @Override
    public IMetaStoreClient getClient()
            throws MetaException
    {
        Configuration conf = getConfiguration();
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS,getWaggleDanceThriftUri());
//        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, getWaggleDanceThriftUri());
        conf.set(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname, "true");
        return new HiveMetaStoreClient(conf);
    }
}
