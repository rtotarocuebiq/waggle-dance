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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.hadoop.hive.conf.HiveConf;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;

public class SessionCredentialsProviderFactory implements AWSCredentialsProviderFactory {

  public final static String AWS_ACCESS_KEY_CONF_VAR = "hive.aws_session_access_id";
  public final static String AWS_SECRET_KEY_CONF_VAR = "hive.aws_session_secret_key";
  public final static String AWS_SESSION_TOKEN_CONF_VAR = "hive.aws_session_token";
  
  @Override
  public AWSCredentialsProvider buildAWSCredentialsProvider(HiveConf hiveConf) {

    checkArgument(hiveConf != null, "hiveConf cannot be null.");
    
    String accessKey = hiveConf.get(AWS_ACCESS_KEY_CONF_VAR);
    String secretKey = hiveConf.get(AWS_SECRET_KEY_CONF_VAR);
    String sessionToken = hiveConf.get(AWS_SESSION_TOKEN_CONF_VAR);
    
    checkArgument(accessKey != null, AWS_ACCESS_KEY_CONF_VAR + " must be set.");
    checkArgument(secretKey != null, AWS_SECRET_KEY_CONF_VAR + " must be set.");
    checkArgument(sessionToken != null, AWS_SESSION_TOKEN_CONF_VAR + " must be set.");
    
    AWSSessionCredentials credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
    
    return new StaticCredentialsProvider(credentials);
  }
}
