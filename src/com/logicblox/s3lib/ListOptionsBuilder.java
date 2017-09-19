package com.logicblox.s3lib;

public class ListOptionsBuilder {
  private CloudStoreClient cloudStoreClient;
  private String bucket;
  private String objectKey;
  private boolean recursive;
  private boolean includeVersions;
  private boolean excludeDirs;

  public ListOptionsBuilder setCloudStoreClient(CloudStoreClient client) {
    this.cloudStoreClient = client;
    return this;
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
    if (cloudStoreClient == null) {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if (bucket == null) {
      throw new UsageException("Bucket has to be set");
    }
  }

  public ListOptions createListOptions() {
    validateOptions();

    return new ListOptions(cloudStoreClient, bucket, objectKey, recursive,
      includeVersions, excludeDirs);
  }
}
