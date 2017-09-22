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

public class ListOptionsBuilder extends CommandOptionsBuilder {
  private String bucket;
  private String objectKey;
  private boolean recursive;
  private boolean includeVersions;
  private boolean excludeDirs;

  ListOptionsBuilder(CloudStoreClient client) {
    _cloudStoreClient = client;
  }

  public ListOptionsBuilder setBucketName(String bucket) {
    this.bucket = bucket;
    return this;
  }
  
  public ListOptionsBuilder setObjectKey(String objectKey) {
    this.objectKey = objectKey;
    return this;
  }
  
  public ListOptionsBuilder setRecursive(boolean recursive) {
    this.recursive = recursive;
    return this;
  }
  
  public ListOptionsBuilder setIncludeVersions(boolean includeVersions) {
    this.includeVersions = includeVersions;
    return this;
  }
  
  public ListOptionsBuilder setExcludeDirs(boolean excludeDirs) {
    this.excludeDirs = excludeDirs;
    return this;
  }

  private void validateOptions()
  {
    if (_cloudStoreClient == null) {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if (bucket == null) {
      throw new UsageException("Bucket has to be set");
    }
  }

  @Override
  public ListOptions createOptions() {
    validateOptions();

    return new ListOptions(_cloudStoreClient, bucket, objectKey, recursive,
      includeVersions, excludeDirs);
  }
}
