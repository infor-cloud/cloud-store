package com.logicblox.s3lib;

import com.google.common.base.Optional;

/**
 * {@code RenameOptions} contains all the details needed by the rename operation.
 * The specified {@code sourceKey}, under {@code sourceBucketName} bucket, is renamed
 * to {@code destinationKey}, under {@code destinationBucketName}.
 * <p>
 * If {@code cannedAcl} is specified then it's applied to the destination
 * object.
 * <p>
 * {@code RenameOptions} objects are meant to be built by {@code
 * RenameOptionsBuilder}. This class provides only public getter methods.
 */
public class RenameOptions extends CommandOptions
{
  private final String _sourceBucket;
  private final String _sourceKey;
  private final String _destinationBucket;
  private final String _destinationKey;
  private final boolean _recursive;
  private final boolean _dryRun;
  private Optional<String> _cannedAcl;

  RenameOptions(
    CloudStoreClient cloudStoreClient, String sourceBucket, String sourceKey,
    String destinationBucket, String destinationKey,
    Optional<String> cannedAcl, boolean recursive,
    boolean dryRun)
  {
    super(cloudStoreClient);
    _sourceBucket = sourceBucket;
    _sourceKey = sourceKey;
    _destinationBucket = destinationBucket;
    _destinationKey = destinationKey;
    _recursive = recursive;
    _cannedAcl = cannedAcl;
    _dryRun = dryRun;
  }

  public String getSourceBucket()
  {
    return _sourceBucket;
  }

  public String getSourceKey()
  {
    return _sourceKey;
  }

  public String getDestinationBucket()
  {
    return _destinationBucket;
  }

  public String getDestinationKey()
  {
    return _destinationKey;
  }

  public Optional<String> getCannedAcl()
  {
    if (!_cannedAcl.isPresent()) {
      if (getCloudStoreClient().getScheme().equals("s3")) {
        _cannedAcl = Optional.of("bucket-owner-full-control");
      }
      else if (getCloudStoreClient().getScheme().equals("gs")) {
        _cannedAcl = Optional.of("projectPrivate");
      }
    }
    return _cannedAcl;
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
