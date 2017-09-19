package com.logicblox.s3lib;


public class ListOptions extends CommandOptions {
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
    super(cloudStoreClient);
    this.bucket = bucket;
    this.objectKey = objectKey;
    this.recursive = recursive;
    this.includeVersions = includeVersions;
    this.excludeDirs = excludeDirs;
  }
  
  public String getBucket() {
    return bucket;
  }
  
  public String getObjectKey() {
    return objectKey;
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
