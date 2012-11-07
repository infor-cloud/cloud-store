package com.logicblox.s3lib;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public class Utils
{
  public static String getDefaultKeyDirectory()
  {
    return System.getProperty("user.home") + File.separator + ".s3lib-keys";
  }

  public static long getDefaultChunkSize()
  {
    return 5 * 1024 * 1024;
  }

  public static URI getURI(String s) throws URISyntaxException
  {
    URI uri = new URI(s);
    
    if(!"s3".equals(uri.getScheme()))
      throw new UsageException("S3 object URL needs to have 's3' as scheme");
    
    return uri;
  }

  public static String getBucket(URI uri)
  {
    return uri.getAuthority();
  }

  public static String getObjectKey(URI uri)
  {
    String path = uri.getPath();

    if(path == null || path.length() == 0)
      throw new UsageException("S3 URLs have the format s3://bucket/key");
    
    if(path.charAt(0) != '/')
      throw new UsageException("S3 URLs have the format s3://bucket/key");
    
    return path.substring(1);
  }

  /**
   * TODO remove once Guava 14.0 is out.
   */ 
  public static <V> ListenableFuture<V> withFallback(
    ListenableFuture<V> future,
    FutureFallback<V> fallback)
  {
    return new FallbackFuture<V>(future, fallback, MoreExecutors.sameThreadExecutor());
  }
}