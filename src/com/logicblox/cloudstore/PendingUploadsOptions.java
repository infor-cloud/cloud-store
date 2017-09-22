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
import java.util.Optional;

/**
 * {@code PendingUploadsOptions} contains all the options needed by the various pending upload
 * methods.
 * <p>
 * Each pending upload method might use different options provided by this class. Which options are
 * used is documented in each method individually.
 * <p>
 * {@code PendingUploadsOptions} objects are meant to be built by {@code
 * PendingUploadsOptionsBuilder}. This class provides only public getter methods.
 */
public class PendingUploadsOptions extends CommandOptions
{
  private final String _bucket;
  private final String _objectKey;
  private final String _uploadId;
  private final Date _date;

  PendingUploadsOptions(CloudStoreClient cloudStoreClient,
                        String bucket,
                        String objectKey,
                        String uploadId,
                        Date date)
  {
    super(cloudStoreClient);
    _bucket = bucket;
    _objectKey = objectKey;
    _uploadId = uploadId;
    _date = date;
  }

  public String getBucketName()
  {
    return _bucket;
  }

  public String getObjectKey()
  {
    return _objectKey;
  }

  public Optional<String> getUploadId()
  {
    return Optional.ofNullable(_uploadId);
  }

  public Optional<Date> getDate()
  {
    return Optional.ofNullable(_date);
  }
}
