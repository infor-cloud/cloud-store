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
 * {@code UploadOptionsBuilder} is used to create and set properties for {@code UploadOptions} objects
 * that control the behavior of cloud-store upload commands.
 * <p>
 * Setting fields {@code file}, {@code bucket} and {@code objectKey} is mandatory. All the others
 * are optional.
 * <p>
 * @see UploadOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#upload()
 * @see CloudStoreClient#uploadDirectory()
 * @see OptionsBuilderFactory#newUploadOptionsBuilder()
 */
public class UploadOptionsBuilder
  extends CommandOptionsBuilder
{
  private File file;
  private String bucket;
  private String objectKey;
  private long chunkSize = -1;
  private String encKey;
  private String cannedAcl;
  private OverallProgressListenerFactory overallProgressListenerFactory;
  private boolean dryRun = false;
  private boolean ignoreAbortInjection = false;

  UploadOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  /**
   * Set the local file to be uploaded.
   */
  public UploadOptionsBuilder setFile(File file)
  {
    this.file = file;
    return this;
  }

  /**
   * Set the name of the bucket to receive the uploaded file.
   */
  public UploadOptionsBuilder setBucketName(String bucket)
  {
    this.bucket = bucket;
    return this;
  }

  /**
   * Set the key of the uploaded file.
   */
  public UploadOptionsBuilder setObjectKey(String objectKey)
  {
    this.objectKey = objectKey;
    return this;
  }

  /**
   * Set the chunk size used to control concurrent parallel file upload.
   */
  public UploadOptionsBuilder setChunkSize(long chunkSize)
  {
    this.chunkSize = chunkSize;
    return this;
  }

  /**
   * Set the name of the encryption key used to encrypt data in the file.
   * The public key for the named key pair must be in the local key directory.
   */
  public UploadOptionsBuilder setEncKey(String encKey)
  {
    this.encKey = encKey;
    return this;
  }

  /**
   * Set the name of access control list given to the uploaded file.  If not
   * specified, the default access control list for the service is used.
   */
  public UploadOptionsBuilder setCannedAcl(String acl)
  {
    this.cannedAcl = acl;
    return this;
  }

  /**
   * Set a progress listener used to track upload progress.
   */
  public UploadOptionsBuilder setOverallProgressListenerFactory(
    OverallProgressListenerFactory overallProgressListenerFactory)
  {
    this.overallProgressListenerFactory = overallProgressListenerFactory;
    return this;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   */
  public UploadOptionsBuilder setDryRun(boolean dryRun)
  {
    this.dryRun = dryRun;
    return this;
  }

  /**
   * Used by test framework to control abort injection behavior.
   */
  public UploadOptionsBuilder setIgnoreAbortInjection(boolean ignore)
  {
    this.ignoreAbortInjection = ignore;
    return this;
  }

  private void validateOptions()
  {
    if(_cloudStoreClient == null)
    {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if(file == null)
    {
      throw new UsageException("File has to be set");
    }
    else if(bucket == null)
    {
      throw new UsageException("Bucket has to be set");
    }
    else if(objectKey == null)
    {
      throw new UsageException("Object key has to be set");
    }

    if(cannedAcl != null)
    {
      if(!_cloudStoreClient.getAclHandler().isCannedAclValid(cannedAcl))
      {
        throw new UsageException("Invalid canned ACL '" + cannedAcl + "'");
      }
    }
    else
    {
      cannedAcl = _cloudStoreClient.getAclHandler().getDefaultAcl();
    }
  }

  /**
   * Validate that all required parameters are set and if so return a new {@link UploadOptions}
   * object.
   */
  @Override
  public UploadOptions createOptions()
  {
    validateOptions();

    return new UploadOptions(_cloudStoreClient, file, bucket, objectKey, chunkSize, encKey,
      cannedAcl, dryRun, ignoreAbortInjection, overallProgressListenerFactory);
  }
}
