/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.waggledance.mapping.service.requests;

import java.util.List;
import java.util.function.BiFunction;

import org.apache.thrift.TException;

import com.hotels.bdp.waggledance.mapping.model.CatalogMapping;

public class GetAllDatabasesRequest implements RequestCallable<List<String>> {

  private final CatalogMapping mapping;
  private final BiFunction<List<String>, CatalogMapping, List<String>> filter;

  public GetAllDatabasesRequest(
      CatalogMapping mapping,
      BiFunction<List<String>, CatalogMapping, List<String>> filter) {
    this.mapping = mapping;
    this.filter = filter;
  }

  @Override
  public List<String> call() throws TException {
    List<String> databases = mapping.getClient().get_all_databases();
    return filter.apply(databases, mapping);
  }

  @Override
  public CatalogMapping getMapping() {
    return mapping;
  }
}
