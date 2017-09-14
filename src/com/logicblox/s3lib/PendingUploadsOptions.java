package com.logicblox.s3lib;

import java.util.Date;
import java.util.Optional;

/**
 * {@code PendingUploadsOptions} contains all the options needed by the various
 * pending upload methods.
 * <p>
 * Each pending upload method might use different options provided by this
 * class. Which options are used is documented in each method individually.
 *
 * {@code PendingUploadsOptions} objects are meant to be built by {@code
 * PendingUploadsOptionsBuilder}. This class provides only public getter methods.
 */
public class PendingUploadsOptions
{
  private final CloudStoreClient _cloudStoreClient;
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
    _cloudStoreClient = cloudStoreClient;
    _bucket = bucket;
    _objectKey = objectKey;
    _uploadId = uploadId;
    _date = date;
  }

  public CloudStoreClient getCloudStoreClient()
  {
    return _cloudStoreClient;
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
