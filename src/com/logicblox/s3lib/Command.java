package com.logicblox.s3lib;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.Key;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;

import com.google.api.services.storage.Storage;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

public class Command
{
  protected boolean _stubborn = true;
  protected int _retryCount = 15;
  protected File file;
  protected long chunkSize;
  protected Key encKey;
  protected long fileLength;
  protected String scheme;

  private Function<Integer, Integer> _retryDelayFunction = Utils.createExponentialDelayFunction(300, 20 * 1000);

  private AmazonS3Client _client = null;

  private Storage _gcs_client = null;

  public void setChunkSize(long chunkSize)
  {
    this.chunkSize = chunkSize;
  }

  public void setFileLength(long fileLength)
  {
    this.fileLength = fileLength;
  }

  public void setRetryCount(int retryCount)
  {
    _retryCount = retryCount;
  }

  public void setRetryClientException(boolean retry)
  {
    _stubborn = retry;
  }
  
  public String getScheme()
  {
    return scheme;
  }

  public String getUri(String bucket, String object)
  {
    return getScheme() + bucket + "/" + object;
  }

  public void setScheme(String scheme)
  {
    this.scheme = scheme;
  }

  public void setAmazonS3Client(AmazonS3Client client)
  {
    _client = client;
  }

  protected AmazonS3Client getAmazonS3Client()
  {
    return _client;
  }

  public void setGCSClient(Storage client)
  {
    _gcs_client = client;
  }

  protected Storage getGCSClient()
  {
    return _gcs_client;
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

  protected <V> ListenableFuture<V> executeWithRetry(ListeningScheduledExecutorService executor, Callable<ListenableFuture<V>> callable)
  {
    return Utils.executeWithRetry(executor, callable, _retryCondition, _retryDelayFunction, TimeUnit.MILLISECONDS, _retryCount);
  }

  protected <V> ListenableFuture<V> executeWithRetry(ListeningScheduledExecutorService executor, Callable<ListenableFuture<V>> callable, int retryCount)
  {
    return Utils.executeWithRetry(executor, callable, _retryCondition, _retryDelayFunction, TimeUnit.MILLISECONDS, retryCount);
  }

  private Predicate<Throwable> _retryCondition = new Predicate<Throwable>()
  {
    public boolean apply(Throwable thrown)
    {
      if(!_stubborn && thrown instanceof AmazonServiceException)
      {
        AmazonServiceException exc = (AmazonServiceException) thrown;
        if(exc.getErrorType() == AmazonServiceException.ErrorType.Client)
          return false;
      }

      return true;
    }
  };

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
