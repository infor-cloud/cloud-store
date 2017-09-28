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
 * Setting fields {@code _file}, {@code _bucketName} and {@code _objectKey} is mandatory. All the others
 * are optional.
 * 
 * @see UploadOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#upload(UploadOptions)
 * @see CloudStoreClient#uploadDirectory(UploadOptions)
 * @see OptionsBuilderFactory#newUploadOptionsBuilder()
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

  /**
   * Set the local file to be uploaded.
   *
   * @param file local file to be uploaded
   * @return this builder
   */
  public UploadOptionsBuilder setFile(File file)
  {
    _file = file;
    return this;
  }

  /**
   * Set the name of the bucket to receive the uploaded file.
   *
   * @param bucket name of bucket to receive file
   * @return this builder
   */
  public UploadOptionsBuilder setBucketName(String bucket)
  {
    _bucketName = bucket;
    return this;
  }

  /**
   * Set the key of the uploaded file.
   *
   * @param objectKey key of file to create
   * @return this builder
   */
  public UploadOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  /**
   * Set the chunk size used to control concurrent parallel file upload.
   *
   * @param chunkSize size in bytes of chunks to use when uploading
   * @return this builder
   */
  public UploadOptionsBuilder setChunkSize(long chunkSize)
  {
    _chunkSize = chunkSize;
    return this;
  }

  /**
   * Set the name of the encryption key used to encrypt data in the file.
   * The public key for the named key pair must be in the local key directory.
   *
   * @param encKey name of key used to encrypt file
   * @return this builder
   */
  public UploadOptionsBuilder setEncKey(String encKey)
  {
    _encKey = encKey;
    return this;
  }

  /**
   * Set the name of access control list given to the uploaded file.  If not
   * specified, the default access control list for the service is used.
   *
   * @param acl name of access control list to apply to uploaded file
   * @return this builder
   */
  public UploadOptionsBuilder setCannedAcl(String acl)
  {
    _cannedAcl = acl;
    return this;
  }

  /**
   * Set a progress listener used to track upload progress.
   *
   * @param overallProgressListenerFactory factory used to create progress listeners
   * @return this builder
   */
  public UploadOptionsBuilder setOverallProgressListenerFactory(
    OverallProgressListenerFactory overallProgressListenerFactory)
  {
    _overallProgressListenerFactory = overallProgressListenerFactory;
    return this;
  }

  /**
   * If set to true, print operations that would be executed, but do not perform them.
   *
   * @param dryRun true if operations should be printed but not executed
   * @return this builder
   */
  public UploadOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

  /**
   * Used by test framework to control abort injection behavior.
   *
   * @param ignore true if abort injection checks should be skipped
   * @return this builder
   */
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

  /**
   * Validate that all required parameters are set and if so return a new {@link UploadOptions}
   * object.
   *
   * @return immutable options object with values from this builder
   */
  @Override
  public UploadOptions createOptions()
  {
    validateOptions();

    return new UploadOptions(_cloudStoreClient, _file, _bucketName, _objectKey, _chunkSize, _encKey,
      _cannedAcl, _dryRun, _ignoreAbortInjection, _overallProgressListenerFactory);
  }
}
