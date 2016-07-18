package com.logicblox.s3lib;

import java.io.File;

public class S3File
{
  private File _localFile;
  private String _eTag;
  private String _key;
  private String _bucket;
  private Long _size;

  public File getLocalFile()
  {
    return _localFile;
  }

  public void setLocalFile(File f)
  {
    _localFile = f;
  }

  public String getETag()
  {
    return _eTag;
  }

  public void setETag(String tag)
  {
    _eTag = tag;
  }

  public String getKey()
  {
    return _key;
  }

  public void setKey(String key)
  {
    _key = key;
  }

  public String getBucketName()
  {
    return _bucket;
  }

  public void setBucketName(String bucket)
  {
    _bucket = bucket;
  }
  
  public Long getSize() {
    return _size;
  }

  public void setSize(Long size) {
    _size = size;
  }
}