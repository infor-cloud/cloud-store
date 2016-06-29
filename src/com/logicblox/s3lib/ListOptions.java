package com.logicblox.s3lib;

import java.io.File;

public class ListOptions {
  
  private String bucket;
  private String objectKey;
  private boolean recursive;
  private boolean includeVersions;
  private boolean excludeDirs;
  
  ListOptions(String bucket,
      String objectKey,
      boolean recursive,
      boolean includeVersions,
      boolean excludeDirs) {
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
  
  public boolean isIncludeVersions() {
    return includeVersions;
  }
  
  public boolean isExcludeDirs() {
    return excludeDirs;
  }
  
}
