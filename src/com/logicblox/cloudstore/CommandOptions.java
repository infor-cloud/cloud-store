/*
  Copyright 2018, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.cloudstore;

/**
 * CommandOptions holds shared properties used by all options.  Builders
 * are used to create the CommandOptions objects that configure
 * cloud-store command execution.
 * 
 * @see CommandOptionsBuilder
 * @see OptionsBuilderFactory
 * @see CloudStoreClient#getOptionsBuilderFactory()
 */
public class CommandOptions
{
  private final CloudStoreClient _cloudStoreClient;

  CommandOptions(CloudStoreClient cloudStoreClient)
  {
    _cloudStoreClient = cloudStoreClient;
  }

  /**
   * Return the CloudStoreClient interface used to create these options.
   *
   * @return cloud store service interface
   */
  public CloudStoreClient getCloudStoreClient()
  {
    return _cloudStoreClient;
  }
}
