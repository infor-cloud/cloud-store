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

import java.io.File;


/**
 * {@code UploadOptionsBuilder} is a builder for {@code UploadOptions} objects.
 * <p>
 * Setting fields {@code _file}, {@code _bucketName} and {@code _objectKey} is mandatory. All the others
 * are optional.
 */
public class UploadOptionsBuilder
  extends CommandOptionsBuilder
{
  private File _file;
  private String _bucketName;
  private String _objectKey;
  private long _chunkSize = -1;
  private String _encKey;
  private String _cannedAcl;
  private OverallProgressListenerFactory _overallProgressListenerFactory;
  private boolean _dryRun = false;
  private boolean _ignoreAbortInjection = false;

  UploadOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  public UploadOptionsBuilder setFile(File file)
  {
    _file = file;
    return this;
  }

  public UploadOptionsBuilder setBucketName(String bucket)
  {
    _bucketName = bucket;
    return this;
  }

  public UploadOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  public UploadOptionsBuilder setChunkSize(long chunkSize)
  {
    _chunkSize = chunkSize;
    return this;
  }

  public UploadOptionsBuilder setEncKey(String encKey)
  {
    _encKey = encKey;
    return this;
  }

  public UploadOptionsBuilder setCannedAcl(String acl)
  {
    _cannedAcl = acl;
    return this;
  }

  public UploadOptionsBuilder setOverallProgressListenerFactory(
    OverallProgressListenerFactory overallProgressListenerFactory)
  {
    _overallProgressListenerFactory = overallProgressListenerFactory;
    return this;
  }

  public UploadOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

  public UploadOptionsBuilder setIgnoreAbortInjection(boolean ignore)
  {
    _ignoreAbortInjection = ignore;
    return this;
  }

  private void validateOptions()
  {
    if(_cloudStoreClient == null)
    {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if(_file == null)
    {
      throw new UsageException("File has to be set");
    }
    else if(_bucketName == null)
    {
      throw new UsageException("Bucket has to be set");
    }
    else if(_objectKey == null)
    {
      throw new UsageException("Object key has to be set");
    }

    if(_cannedAcl != null)
    {
      if(!_cloudStoreClient.getAclHandler().isCannedAclValid(_cannedAcl))
      {
        throw new UsageException("Invalid canned ACL '" + _cannedAcl + "'");
      }
    }
    else
    {
      _cannedAcl = _cloudStoreClient.getAclHandler().getDefaultAcl();
    }
  }

  @Override
  public UploadOptions createOptions()
  {
    validateOptions();

    return new UploadOptions(_cloudStoreClient, _file, _bucketName, _objectKey, _chunkSize, _encKey,
      _cannedAcl, _dryRun, _ignoreAbortInjection, _overallProgressListenerFactory);
  }
}
