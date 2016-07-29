package com.logicblox.s3lib;

import java.io.File;
import java.util.Optional;
import java.util.Date;

public class S3File
{
  private File _localFile;
  private String _eTag;
  private String _key;
  private String _bucket;
  private Optional<String> _versionId;
  private long _size;
  private Optional<Date> _timestamp;

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

  public void setBucketName(String bucket) {
    _bucket = bucket;
  }

  public Optional<String> getVersionId() {
    return _versionId;
  }

  public void setVersionId(Optional<String> versionId) {
    _versionId = versionId;
  }

  public long getSize() {
    return _size;
  }

  public void setSize(long size) {
    _size = size;
  }
  
  public Optional<Date> getTimestamp() {
    return _timestamp;
  }

  public void setTimestamp(Optional<Date> timestamp) {
    _timestamp = timestamp;
  }

}