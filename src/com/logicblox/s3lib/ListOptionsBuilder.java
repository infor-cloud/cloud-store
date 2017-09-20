package com.logicblox.s3lib;

public class ListOptionsBuilder extends CommandOptionsBuilder {
  private String bucket;
  private String objectKey;
  private boolean recursive;
  private boolean includeVersions;
  private boolean excludeDirs;

  ListOptionsBuilder(CloudStoreClient client) {
    _cloudStoreClient = client;
  }

  public ListOptionsBuilder setBucketName(String bucket) {
    this.bucket = bucket;
    return this;
  }
  
  public ListOptionsBuilder setObjectKey(String objectKey) {
    this.objectKey = objectKey;
    return this;
  }
  
  public ListOptionsBuilder setRecursive(boolean recursive) {
    this.recursive = recursive;
    return this;
  }
  
  public ListOptionsBuilder setIncludeVersions(boolean includeVersions) {
    this.includeVersions = includeVersions;
    return this;
  }
  
  public ListOptionsBuilder setExcludeDirs(boolean excludeDirs) {
    this.excludeDirs = excludeDirs;
    return this;
  }

  private void validateOptions()
  {
    if (_cloudStoreClient == null) {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if (bucket == null) {
      throw new UsageException("Bucket has to be set");
    }
  }

  @Override
  public ListOptions createOptions() {
    validateOptions();

    return new ListOptions(_cloudStoreClient, bucket, objectKey, recursive,
      includeVersions, excludeDirs);
  }
}
