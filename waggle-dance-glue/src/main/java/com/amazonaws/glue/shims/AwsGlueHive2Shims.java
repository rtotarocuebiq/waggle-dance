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
package com.amazonaws.glue.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

final class AwsGlueHive2Shims
        implements AwsGlueHiveShims {

  private static final String HIVE_2_VERSION = "3.";

  static boolean supportsVersion(String version) {
    return version.startsWith(HIVE_2_VERSION);
  }

  @Override
  public ExprNodeGenericFuncDesc getDeserializeExpression(byte[] exprBytes) {
    return SerializationUtilities.deserializeExpressionFromKryo(exprBytes);
  }

  @Override
  public byte[] getSerializeExpression(ExprNodeGenericFuncDesc expr) {
    return SerializationUtilities.serializeExpressionToKryo(expr);
  }

  @Override
  public Path getDefaultTablePath(Database db, String tableName, Warehouse warehouse) throws MetaException {
    return warehouse.getDefaultTablePath(db, tableName);
  }

  @Override
  public boolean validateTableName(String name, Configuration conf) {
    return MetaStoreUtils.validateName(name, conf);
  }

  @Override
  public boolean requireCalStats(
      Configuration conf,
      Partition oldPart,
      Partition newPart,
      Table tbl,
      EnvironmentContext environmentContext) {
    return MetaStoreUtils.requireCalStats(oldPart, newPart, tbl, environmentContext);
  }

  @Override
  public boolean updateTableStatsFast(
      Database db,
      Table tbl,
      Warehouse wh,
      boolean madeDir,
      boolean forceRecompute,
      EnvironmentContext environmentContext
  ) throws MetaException {
    MetaStoreUtils.updateTableStatsSlow(db, tbl, wh, madeDir, forceRecompute, environmentContext);
    return true;
  }

}
