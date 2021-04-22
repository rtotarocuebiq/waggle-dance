/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.waggledance.mapping.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;

public class CatalogNameMapping extends MetaStoreMappingDecorator {

  private final static Logger log = LoggerFactory.getLogger(CatalogNameMapping.class);

  private final Map<String, String> inbound;
  private final Map<String, String> outbound;

  public CatalogNameMapping(MetaStoreMapping metaStoreMapping, BiMap<String, String> databaseNameMap) {
    super(metaStoreMapping);
    if (databaseNameMap != null && !databaseNameMap.isEmpty()) {
      inbound = new HashMap<>(databaseNameMap.inverse());
      outbound = new HashMap<>(databaseNameMap);
    } else {
      inbound = Collections.emptyMap();
      outbound = Collections.emptyMap();
    }
  }

  @Override
  public String transformOutboundCatalogName(String databaseName) {
    return transformOutboundCatalogNameMultiple(databaseName).get(0);
  }

  @Override
  public List<String> transformOutboundCatalogNameMultiple(String catalogName) {
    List<String> results = new ArrayList<>();
    results.addAll(super.transformOutboundCatalogNameMultiple(catalogName));
    if (outbound.containsKey(catalogName)) {
      String result = outbound.get(catalogName);
      List<String> catalogs = super.transformOutboundCatalogNameMultiple(result);
      log.debug("transformOutboundCatalogName '" + catalogName + "' to '" + catalogs + "'");
      results.addAll(catalogs);
    }
    return results;
  }

  @Override
  public String transformInboundCatalogName(String catalogName) {
    String newCatalogName = super.transformInboundCatalogName(catalogName);
    String result = inbound.getOrDefault(newCatalogName, newCatalogName);
    log.debug("transformInboundCatalogName '" + catalogName + "' to '" + result + "'");
    return result;
  }

}
