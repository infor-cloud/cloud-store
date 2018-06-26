/*
  Copyright 2018, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.cloudstore;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.google.api.services.storage.Storage;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

class Command
{
  protected boolean _stubborn = true;
  protected int _retryCount = 15;
  protected File file;
  protected long chunkSize;
  protected Key encKey;
  protected long fileLength;
  protected String scheme;
  protected CloudStoreClient _client;

  private AmazonS3 _s3Client = null;

  private Storage _gcsClient = null;

  public Command(CommandOptions options)
  {
    _client = options.getCloudStoreClient();
  }

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

  // TODO: It's provided by the CloudStoreClient. Drop it.
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

  public void setS3Client(AmazonS3 client)
  {
    _s3Client = client;
  }

  protected AmazonS3 getS3Client()
  {
    return _s3Client;
  }

  public void setGCSClient(Storage client)
  {
    _gcsClient = client;
  }

  protected Storage getGCSClient()
  {
    return _gcsClient;
  }

  protected static Key readKeyFromFile(String encKeyName, File encKeyFile)
    throws IOException, ClassNotFoundException
  {
    FileInputStream fs = new FileInputStream(encKeyFile);
    ObjectInputStream in = new ObjectInputStream(fs);
    Map<String, Key> keys = (HashMap<String, Key>) in.readObject();
    in.close();
    if(keys.containsKey(encKeyName))
    {
      return keys.get(encKeyName);
    }
    return null;
  }

  protected <V> ListenableFuture<V> executeWithRetry(
    ListeningScheduledExecutorService executor, Callable<ListenableFuture<V>> callable)
  {
    int initialDelay = 300;
    int maxDelay = 20 * 1000;

    ThrowableRetryPolicy trp = new ExpBackoffRetryPolicy(initialDelay, maxDelay, _retryCount,
      TimeUnit.MILLISECONDS)
    {
      @Override
      public boolean retryOnThrowable(Throwable thrown)
      {
        if(!_stubborn && thrown instanceof AmazonServiceException)
        {
          AmazonServiceException exc = (AmazonServiceException) thrown;
          if(exc.getErrorType() == AmazonServiceException.ErrorType.Client)
          {
            return false;
          }
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
    catch(Exception e)
    {
      f = Futures.immediateFailedFuture(e);
    }

    return f;
  }

  protected static void rethrow(Throwable thrown)
    throws Exception
  {
    if(thrown instanceof Exception)
    {
      throw (Exception) thrown;
    }
    if(thrown instanceof Error)
    {
      throw (Error) thrown;
    }
    else
    {
      throw new RuntimeException(thrown);
    }
  }

  public static PublicKey getPublicKey(PrivateKey privateKey)
    throws NoSuchKeyException
  {
    try
    {
      RSAPrivateCrtKey privateCrtKey = (RSAPrivateCrtKey) privateKey;
      RSAPublicKeySpec publicKeySpec = new RSAPublicKeySpec(privateCrtKey.getModulus(),
        privateCrtKey.getPublicExponent());
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePublic(publicKeySpec);
    }
    catch(NoSuchAlgorithmException exc)
    {
      throw new RuntimeException(exc);
    }
    catch(InvalidKeySpecException exc)
    {
      throw new NoSuchKeyException(exc);
    }
  }
}
