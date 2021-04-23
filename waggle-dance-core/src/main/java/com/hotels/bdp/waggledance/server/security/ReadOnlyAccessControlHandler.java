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
package com.hotels.bdp.waggledance.server.security;

public class ReadOnlyAccessControlHandler implements AccessControlHandler {

  @Override
  public boolean hasWritePermission(String catalog, String databaseName) {
    return false;
  }

  @Override
  public boolean hasCreatePermission() {
    return false;
  }

  @Override
  public void databaseCreatedNotification(String catalog, String name) {
    // nothing to do
  }
}
