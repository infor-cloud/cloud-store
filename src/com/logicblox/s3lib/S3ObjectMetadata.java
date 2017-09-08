package com.logicblox.s3lib;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.HashMap;
import java.util.Map;

class S3ObjectMetadata
{
  private AmazonS3 client;
  private ListeningExecutorService executor;
  private ObjectMetadata meta;
  private String key;
  private String bucketName;
  private String version;

  public S3ObjectMetadata(
    AmazonS3 client,
    String key,
    String bucketName,
    String version,
    ObjectMetadata meta,
    ListeningExecutorService executor)
  {
    this.client = client;
    this.key = key;
    this.bucketName = bucketName;
    this.version = version;
    this.executor = executor;
    this.meta = meta;
  }

  public String getBucket()
  {
    return bucketName;
  }

  public String getKey()
  {
    return key;
  }

  public ObjectMetadata getAllMetadata()
  {
    return meta;
  }

  // make sure all metadata keys are lowercase.  some servers (minio) return
  // mixed case keys
  public Map<String,String> getUserMetadata()
  {
    Map<String,String> userData = meta.getUserMetadata();
    Map<String,String> fixed = new HashMap<String,String>();
    for(Map.Entry<String,String> e : userData.entrySet())
      fixed.put(e.getKey().toLowerCase(), e.getValue());
    return fixed;
  }

  public String getETag()
  {
    return meta.getETag();
  }

  public long getLength()
  {
    return meta.getContentLength();
  }

  public String getVersion()
  {
    return version;
  }
}
