package com.logicblox.s3lib;


import java.util.Optional;

public class ListOptions {
  private CloudStoreClient cloudStoreClient;
  private String bucket;
  private String objectKey;
  private boolean recursive;
  private boolean includeVersions;
  private boolean excludeDirs;
  
  ListOptions(CloudStoreClient cloudStoreClient,
              String bucket,
              String objectKey,
              boolean recursive,
              boolean includeVersions,
              boolean excludeDirs) {
    this.cloudStoreClient = cloudStoreClient;
    this.bucket = bucket;
    this.objectKey = objectKey;
    this.recursive = recursive;
    this.includeVersions = includeVersions;
    this.excludeDirs = excludeDirs;
  }

  public CloudStoreClient getCloudStoreClient() {
    return cloudStoreClient;
  }

  public String getBucketName() {
    return bucket;
  }
  
  public Optional<String> getObjectKey() {
    return Optional.ofNullable(objectKey);
  }
  
  public boolean isRecursive() {
    return recursive;
  }
  
  public boolean versionsIncluded() {
    return includeVersions;
  }
  
  public boolean dirsExcluded() {
    return excludeDirs;
  }
  
}
