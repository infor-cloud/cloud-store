package com.logicblox.s3lib;

import java.util.Optional;

/**
 * {@code RenameOptions} contains all the details needed by the rename operation.
 * The specified {@code sourceObjectKey}, under {@code sourceBucketName} bucket, is renamed
 * to {@code destinationObjectKey}, under {@code destinationBucketName}.
 * <p>
 * If {@code cannedAcl} is specified then it's applied to the destination
 * object.
 * <p>
 * {@code RenameOptions} objects are meant to be built by {@code
 * RenameOptionsBuilder}. This class provides only public getter methods.
 */
public class RenameOptions
{
  private final CloudStoreClient _cloudStoreClient;
  private final String _sourceBucketName;
  private final String _sourceObjectKey;
  private final String _destinationBucketName;
  private final String _destinationObjectKey;
  private final boolean _recursive;
  private final boolean _dryRun;
  private String _cannedAcl;

  RenameOptions(
    CloudStoreClient cloudStoreClient, String sourceBucketName, String sourceObjectKey,
    String destinationBucket, String destinationObjectKey,
    String cannedAcl, boolean recursive,
    boolean dryRun)
  {
    _cloudStoreClient = cloudStoreClient;
    _sourceBucketName = sourceBucketName;
    _sourceObjectKey = sourceObjectKey;
    _destinationBucketName = destinationBucket;
    _destinationObjectKey = destinationObjectKey;
    _recursive = recursive;
    _cannedAcl = cannedAcl;
    _dryRun = dryRun;
  }

  public CloudStoreClient getCloudStoreClient()
  {
    return _cloudStoreClient;
  }

  public String getSourceBucketName()
  {
    return _sourceBucketName;
  }

  public String getSourceObjectKey()
  {
    return _sourceObjectKey;
  }

  public String getDestinationBucketName()
  {
    return _destinationBucketName;
  }

  public String getDestinationObjectKey()
  {
    return _destinationObjectKey;
  }

  public Optional<String> getCannedAcl()
  {
    return Optional.ofNullable(_cannedAcl);
  }

  public boolean isRecursive()
  {
    return _recursive;
  }

  public boolean isDryRun()
  {
    return _dryRun;
  }

}
