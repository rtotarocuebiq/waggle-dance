package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;

import java.util.concurrent.ConcurrentHashMap;

public class AWSGlueDataCatalogHiveClientFactory implements HiveMetastoreClientFactory
{

  private HiveConf conf;
  private HiveMetaHookLoader hookLoader;

  public AWSGlueDataCatalogHiveClientFactory(HiveConf conf, HiveMetaHookLoader hookLoader)
  {
    this.conf = conf;
    this.hookLoader = hookLoader;
  }

  @Override
  public IMetaStoreClient getHiveMetastoreClient()
          throws HiveAuthzPluginException
  {
    AWSCatalogMetastoreClient client = null;
    try {
      return  new AWSCatalogMetastoreClient(conf, hookLoader);
    }
    catch (MetaException e) {
      throw  new HiveAuthzPluginException(e);
    }
  }
}
