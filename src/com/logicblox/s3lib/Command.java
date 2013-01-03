package com.logicblox.s3lib;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.Key;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ListenableFuture;

public class Command
{
  protected boolean _stubborn = true;
  protected int _retryCount = 50;
  protected File file;
  protected long chunkSize;
  protected Key encKey;
  protected long fileLength;

  private AmazonS3Client _client = null;

  public void setChunkSize(long chunkSize)
  {
    this.chunkSize = chunkSize;
  }

  public void setRetryCount(int retryCount)
  {
    _retryCount = retryCount;
  }

  public void setRetryClientException(boolean retry)
  {
    _stubborn = retry;
  }
  
  public void setAmazonS3Client(AmazonS3Client client)
  {
    _client = client;
  }

  protected AmazonS3Client getAmazonS3Client()
  {
    return _client;
  }

  protected static Key readKeyFromFile(String encKeyName, File encKeyFile) throws IOException, ClassNotFoundException
  {
    FileInputStream fs = new FileInputStream(encKeyFile);
    ObjectInputStream in = new ObjectInputStream(fs);
    Map<String,Key> keys = (HashMap<String,Key>) in.readObject();
    in.close();
    if (keys.containsKey(encKeyName)) {
      return keys.get(encKeyName);
    }
    return null;
  }

  protected <V> ListenableFuture<V> withFallback(
    ListenableFuture<V> future,
    FutureFallback<V> fallback)
  {
    return Utils.withFallback(future, fallback);
  }

  /**
   * Utility to rethrow the given throwable as an exception if the
   * retryCount has exceeded the configured maximum.
   */
  protected void rethrowOnMaxRetry(Throwable thrown, int retryCount) throws Exception
  {
    if(!_stubborn && thrown instanceof AmazonServiceException)
    {
      AmazonServiceException exc = (AmazonServiceException) thrown;
      if(exc.getErrorType() == AmazonServiceException.ErrorType.Client)
        throw exc;
    }

    if(retryCount >= _retryCount)
    {
      if(thrown instanceof Exception)
        throw (Exception) thrown;
      if(thrown instanceof Error)
        throw (Error) thrown;
      else
        throw new RuntimeException(thrown);
    }
  }

  protected static void rethrow(Throwable thrown) throws Exception
  {
    if(thrown instanceof Exception)
      throw (Exception) thrown;
    if(thrown instanceof Error)
      throw (Error) thrown;
    else
      throw new RuntimeException(thrown);
  }
}
