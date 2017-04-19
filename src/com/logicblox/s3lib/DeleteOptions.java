package com.logicblox.s3lib;


public class DeleteOptions {
  
  private String _bucket;
  private String _objectKey;
  private boolean _recursive;
  private boolean _dryRun;
  private boolean _forceDelete;
  
  DeleteOptions(String bucket, String objectKey, boolean recursive, boolean dryRun, boolean forceDelete)
  {
    _bucket = bucket;
    _objectKey = objectKey;
    _recursive = recursive;
    _dryRun = dryRun;
    _forceDelete = forceDelete;
  }
  
  public String getBucket()
  {
    return _bucket;
  }
  
  public String getObjectKey()
  {
    return _objectKey;
  }
  
  public boolean isRecursive()
  {
    return _recursive;
  }
  
  public boolean isDryRun()
  {
    return _dryRun;
  }

  public boolean forceDelete()
  {
    return _forceDelete;
  }
}
