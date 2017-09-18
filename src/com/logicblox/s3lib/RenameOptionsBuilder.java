package com.logicblox.s3lib;

/**
 * {@code RenameOptionsBuilder} is a builder for {@code RenameOptions} objects.
 * <p>
 * Setting {@code sourceBucketName}, {@code sourceObjectKey}, {@code
 * destinationBucket} and {@code destinationObjectKey} is mandatory. All the
 * others are optional.
 */
public class RenameOptionsBuilder
{
  private CloudStoreClient _cloudStoreClient;
  private String _sourceBucketName;
  private String _sourceObjectKey;
  private String _destinationBucketName;
  private String _destinationObjectKey;
  private String _cannedAcl;
  private boolean _recursive = false;
  private boolean _dryRun = false;

  public RenameOptionsBuilder setCloudStoreClient(CloudStoreClient client) {
    _cloudStoreClient = client;
    return this;
  }

  public RenameOptionsBuilder setSourceBucketName(String sourceBucketName)
  {
    _sourceBucketName = sourceBucketName;
    return this;
  }

  public RenameOptionsBuilder setSourceObjectKey(String sourceObjectKey)
  {
    _sourceObjectKey = sourceObjectKey;
    return this;
  }

  public RenameOptionsBuilder setDestinationBucketName(String destinationBucket)
  {
    _destinationBucketName = destinationBucket;
    return this;
  }

  public RenameOptionsBuilder setDestinationObjectKey(String destinationObjectKey)
  {
    _destinationObjectKey = destinationObjectKey;
    return this;
  }

  public RenameOptionsBuilder setCannedACL(String cannedAcl)
  {
    _cannedAcl = cannedAcl;
    return this;
  }

  public RenameOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }

  public RenameOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }

  public RenameOptions createRenameOptions()
  {
    if (_cloudStoreClient == null) {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if (_sourceBucketName == null) {
      throw new UsageException("Source bucket name has to be set");
    }
    else if (_sourceObjectKey == null) {
      throw new UsageException("Source object key has to be set");
    }
    else if (_destinationBucketName == null) {
      throw new UsageException("Destination bucket name key has to be set");
    }
    else if (_destinationObjectKey == null) {
      throw new UsageException("Destination object key has to be set");
    }

    if (_cannedAcl != null) {
      if (!Utils.isValidCannedACLFor(
        _cloudStoreClient.getStorageService(), _cannedAcl)); {
        throw new UsageException("Invalid canned ACL '" + _cannedAcl + "'");
      }
    }

    return new RenameOptions(_cloudStoreClient, _sourceBucketName,
      _sourceObjectKey, _destinationBucketName, _destinationObjectKey,
      _cannedAcl, _recursive, _dryRun);
  }
}
