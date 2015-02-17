package com.logicblox.s3lib;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.model.ObjectMetadata;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.*;

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

    if("https".equals(uri.getScheme()) && uri.getHost().endsWith("amazonaws.com"))
    {
      String path = uri.getPath();
      if(path == null || path.length() < 3 || path.charAt(0) != '/' || path.indexOf('/', 1) == -1)
        throw new UsageException("https S3 URLs have the format https://s3.amazonaws.com/bucket/key");

      String bucket = path.substring(1, path.indexOf('/', 1));
      String key = path.substring(path.indexOf('/', 1) + 1);
      uri = new URI("s3://" + bucket + "/" + key);
    }
    
    if(!"s3".equals(uri.getScheme()))
      throw new UsageException("S3 object URL needs to have 's3' as scheme");
    
    return uri;
  }

  public static String getBucket(URI uri)
  {
    if(!"s3".equals(uri.getScheme()))
      throw new IllegalArgumentException("S3 object URL needs to have 's3' as scheme");

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
  
  public static ListeningExecutorService getHttpExecutor(int nThreads)
  {
    return MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(nThreads));
  }

  public static ListeningScheduledExecutorService getInternalExecutor(int poolSize)
  {
    return MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(poolSize));
  }

  public static KeyProvider getKeyProvider(String encKeyDirectory)
  {
    File dir = new File(encKeyDirectory);
    if(!dir.exists() && !dir.mkdirs())
      throw new UsageException("specified key directory '" + encKeyDirectory + "' does not exist");

    if(!dir.isDirectory())
      throw new UsageException("specified key directory '" + encKeyDirectory + "' is not a directory");

    return new DirectoryKeyProvider(dir);
  }

  public static Function<Integer, Integer> createExponentialDelayFunction(final int initialDelay)
  {
    return new Function<Integer, Integer>()
    {
      public Integer apply(Integer retryCount)
      {
        if(retryCount > 0)
        {
          return initialDelay * (int) Math.pow(2, retryCount - 1);
        }
        else
        {
          return 0;
        }
      }
    };
  }

  public static Function<Integer, Integer> createLinearDelayFunction(final int increment)
  {
    return new Function<Integer, Integer>()
    {
      public Integer apply(Integer retryCount)
      {
        return increment * retryCount;
      }
    };
  }

  public static <V> ListenableFuture<V> executeWithRetry(
    ListeningScheduledExecutorService executor,
    Callable<ListenableFuture<V>> callable,
    Predicate<Throwable> retryCondition,
    Function<Integer, Integer> delayFun,
    TimeUnit timeUnit,
    int maxRetryCount)
  {
    return retry(executor, callable, retryCondition, delayFun, timeUnit, 0, maxRetryCount);
  }

  private static <V> ListenableFuture<V> retry(
    final ListeningScheduledExecutorService executor,
    final Callable<ListenableFuture<V>> callable,
    final Predicate<Throwable> retryCondition,
    final Function<Integer, Integer> delayFun,
    final TimeUnit timeUnit,
    final int retryCount,
    final int maxRetryCount)
  {
    int delay = delayFun.apply(retryCount);

    // TODO actually use the scheduled executor once Guava 15 is out
    // Futures.dereference(executor.schedule(callable, delay, timeUnit));
    if(delay > 0)
    {
      try
      {
        Thread.sleep(timeUnit.toMillis(delay));
      }
      catch(InterruptedException exc)
      {}
    }

    ListenableFuture<V> future;
    try
    {
      future = callable.call();
    }
    catch(Exception exc)
    {
      future = Futures.immediateFailedFuture(exc);
    }

    return Futures.withFallback(
      future,
      new FutureFallback<V>()
      {
        public ListenableFuture<V> create(Throwable t)
        {
          if(retryCondition.apply(t) && retryCount < maxRetryCount)
          {
            StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
            String tStr = sw.toString();

            String msg =
                "error in task: " + callable.toString() + ": " + t.getMessage() + '\n' +
                tStr +
                "retrying task: " + callable.toString() + '\n';

            System.err.println(msg);
            return retry(executor, callable, retryCondition, delayFun, timeUnit, retryCount + 1, maxRetryCount);
          }
          else
          {
            System.err.println("aborting (after " + retryCount + " retries): " + callable.toString());
            return Futures.immediateFailedFuture(t);
          }
        }
      });
  }

  protected static void print(ObjectMetadata m)
  {
    System.out.println("Cache-Control: " + m.getCacheControl());
    System.out.println("Content-Disposition: " + m.getContentDisposition());
    System.out.println("Content-Encoding: " + m.getContentEncoding());
    System.out.println("Content-Length: " + m.getContentLength());
    System.out.println("Content-MD5: " + m.getContentMD5());
    System.out.println("Content-Type: " + m.getContentType());
    System.out.println("ETag: " + m.getETag());
    System.out.println("Expiration-Time: " + m.getExpirationTime());
    System.out.println("Expiration-Time-Rule-Id: " + m.getExpirationTimeRuleId());
    System.out.println("Http-Expires: " + m.getHttpExpiresDate());
    System.out.println("Last-Modified: " + m.getLastModified());
    System.out.println("Server-Side-Encryption: " + m.getServerSideEncryption());
    System.out.println("Version-Id: " + m.getVersionId());
    System.out.println("");
    for(Map.Entry<String, String> entry : m.getUserMetadata().entrySet())
    {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
  }
}