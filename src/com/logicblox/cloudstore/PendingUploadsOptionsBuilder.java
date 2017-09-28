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

import java.util.Date;

/**
 * {@code PendingUploadingOptionsBuilder} is used to create and set properties for {@code PendingUploadingOptions} 
 * objects used to control behavior of the cloud-store commands for managing pending uploads.
 * <p>
 * Setting {@code bucketName} and {@code objectKey} is mandatory.  Other properties are optional.
 * 
 * @see PendingUploadsOptions
 * @see CloudStoreClient#getOptionsBuilderFactory()
 * @see CloudStoreClient#listPendingUploads(PendingUploadsOptions)
 * @see CloudStoreClient#abortPendingUploads(PendingUploadsOptions)
 * @see OptionsBuilderFactory#newPendingUploadsOptionsBuilder()
 */
public class PendingUploadsOptionsBuilder
  extends CommandOptionsBuilder
{
  private String _bucket;
  private String _objectKey;
  private String _uploadId;
  private Date _date;

  PendingUploadsOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  /**
   * Set the name of the bucket in which to check for pending uploads.
   *
   * @param bucket name of bucket
   * @return this builder
   */
  public PendingUploadsOptionsBuilder setBucketName(String bucket)
  {
    _bucket = bucket;
    return this;
  }

  /**
   * Set the key of the file to check for pending uploads.
   *
   * @param objectKey file key
   * @return this builder
   */
  public PendingUploadsOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }

  /**
   * Set the upload ID for an upload to be aborted.
   *
   * @param uploadId indentifier of upload to be aborted
   * @return this builder
   */
  public PendingUploadsOptionsBuilder setUploadId(String uploadId)
  {
    _uploadId = uploadId;
    return this;
  }

  /**
   * Set the date of an upload to be aborted.
   *
   * @param date date of upload to be aborted
   * @return this builder
   */
  public PendingUploadsOptionsBuilder setDate(Date date)
  {
    _date = date;
    return this;
  }

  private void validateOptions()
  {
    if(_cloudStoreClient == null)
    {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if(_bucket == null)
    {
      throw new UsageException("Bucket has to be set");
    }
    else if(_objectKey == null)
    {
      throw new UsageException("Object key has to be set");
    }
  }

  /**
   * Validate that all required parameters are set and if so return a new {@link PendingUploadsOptions}
   * object.
   *
   * @return immutable options object with values from this builder
   */
  @Override
  public PendingUploadsOptions createOptions()
  {
    validateOptions();

    return new PendingUploadsOptions(_cloudStoreClient, _bucket, _objectKey, _uploadId, _date);
  }
}
