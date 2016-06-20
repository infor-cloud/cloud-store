package com.logicblox.s3lib;

import java.io.File;

public class ListOptions {
  private String bucket;
  private String objectKey;
  private boolean recursive;
  private boolean ls_versions;
  private boolean exclude_dirs;

  ListOptions(String bucket,
      String objectKey,
      boolean recursive,
      boolean ls_versions,
      boolean exclude_dirs) {
this.bucket = bucket;
this.objectKey = objectKey;
this.recursive = recursive;
this.ls_versions = ls_versions;
this.exclude_dirs = exclude_dirs;
}

  
  public String getBucket() {
    return bucket;
  }

  
  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  
  public String getObjectKey() {
    return objectKey;
  }

  
  public void setObjectKey(String objectKey) {
    this.objectKey = objectKey;
  }

  
  public boolean isRecursive() {
    return recursive;
  }

  
  public void setRecursive(boolean recursive) {
    this.recursive = recursive;
  }

  
  public boolean isLs_versions() {
    return ls_versions;
  }

  
  public void setLs_versions(boolean ls_versions) {
    this.ls_versions = ls_versions;
  }

  
  public boolean isExclude_dirs() {
    return exclude_dirs;
  }

  
  public void setExclude_dirs(boolean exclude_dirs) {
    this.exclude_dirs = exclude_dirs;
  }
  
}
