package com.logicblox.s3lib;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
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
            System.err.println("retriable error: " + callable.toString() + ": " + t.getMessage());
            return retry(executor, callable, retryCondition, delayFun, timeUnit, retryCount + 1, maxRetryCount);
          }
          else
          {
            return Futures.immediateFailedFuture(t);
          }
        }
      });
  }
}