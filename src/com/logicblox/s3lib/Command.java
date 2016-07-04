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
import com.google.common.util.concurrent.Futures;
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

  public boolean getRetryClientException()
  {
    return _stubborn;
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
    int initialDelay = 300;
    int maxDelay = 20 * 1000;

    ThrowableRetryPolicy trp = new ExpBackoffRetryPolicy(
      initialDelay, maxDelay, _retryCount, TimeUnit.MILLISECONDS)
    {
      @Override
      public boolean retryOnThrowable(Throwable thrown)
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

    Callable<ListenableFuture<V>> rt = new ThrowableRetriableTask(callable, executor, trp);
    ListenableFuture<V> f;
    try
    {
      f = rt.call();
    }
    catch (Exception e)
    {
      f = Futures.immediateFailedFuture(e);
    }

    return f;
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
