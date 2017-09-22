/*
  Copyright 2017, Infor Inc.

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


public class ExistsOptions extends CommandOptions
{
  private final String _bucket;
  private final String _objectKey;

  ExistsOptions(CloudStoreClient cloudStoreClient, String bucket, String objectKey)
  {
    super(cloudStoreClient);
    _bucket = bucket;
    _objectKey = objectKey;
  }

  public String getBucketName()
  {
    return _bucket;
  }

  public String getObjectKey()
  {
    return _objectKey;
  }
}
