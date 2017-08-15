package com.logicblox.s3lib;

import com.google.common.base.Optional;

/**
 * {@code RenameOptionsBuilder} is a builder for {@code RenameOptions} objects.
 * <p>
 * Setting {@code sourceBucket}, {@code sourceKey}, {@code
 * destinationBucket} and {@code destinationKey} is mandatory. All the
 * others are optional.
 */
public class RenameOptionsBuilder
{
  private String _sourceBucket;
  private String _sourceKey;
  private String _destinationBucket;
  private String _destinationKey;
  private Optional<String> _cannedAcl = Optional.absent();
  private boolean _recursive = false;
  private boolean _dryRun = false;

  
  public RenameOptionsBuilder setSourceBucket(String sourceBucket)
  {
    _sourceBucket = sourceBucket;
    return this;
  }

  public RenameOptionsBuilder setSourceKey(String sourceKey)
  {
    _sourceKey = sourceKey;
    return this;
  }

  public RenameOptionsBuilder setDestinationBucket(String destinationBucket)
  {
    _destinationBucket = destinationBucket;
    return this;
  }

  public RenameOptionsBuilder setDestinationKey(String destinationKey)
  {
    _destinationKey = destinationKey;
    return this;
  }

  public RenameOptionsBuilder setCannedAcl(String cannedAcl)
  {
    _cannedAcl = Optional.fromNullable(cannedAcl);
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
    return new RenameOptions(
      _sourceBucket, _sourceKey, _destinationBucket, _destinationKey,
      _cannedAcl, _recursive, _dryRun);
  }
}
