package com.logicblox.s3lib;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.s3.model.ObjectMetadata;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

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
      uri = getS3URI(s);
      return uri;
    }

    uri = new URI(s);
    if("https".equals(uri.getScheme()) && uri.getHost().endsWith("googleapis.com"))
    {
      uri = getGCSURI(s);
      return uri;
    }

    uri = new URI(s);
    if((!"s3".equals(uri.getScheme())) && (!"gs".equals(uri.getScheme())))
      throw new UsageException("Object URL needs to have either 's3' or 'gs' as scheme");
    
    return uri;
  }

  public static URI getS3URI(String s) throws URISyntaxException, UsageException {
    URI uri = new URI(s);

    String path = uri.getPath();
    if(path == null || path.length() < 3 || path.charAt(0) != '/' || path.indexOf('/', 1) == -1)
      throw new UsageException("HTTPS S3 URLs have the format https://s3.amazonaws.com/bucket/key");

    String bucket = path.substring(1, path.indexOf('/', 1));
    String key = path.substring(path.indexOf('/', 1) + 1);
    uri = new URI("s3://" + bucket + "/" + key);

    return uri;
  }

  public static URI getGCSURI(String s) throws URISyntaxException, UsageException {
    Matcher matcher;
    matcher = matchesGCSURI("https://storage.googleapis.com/(.+?)/(.+?)$", s);
    if (matcher.find())
      return new URI("gs://" + matcher.group(1) + "/" + (matcher.group(2)));

    matcher = matchesGCSURI("https://(.+?).storage.googleapis.com/(.+?)$", s);
    if (matcher.find())
      return new URI("gs://" + matcher.group(1) + "/" + (matcher.group(2)));

    matcher = matchesGCSURI("https://www.googleapis.com/upload/storage/v1/b/(.+?)/o/(.+?)$", s);
    if (matcher.find())
      return new URI("gs://" + matcher.group(1) + "/" + (matcher.group(2)));

    throw new UsageException("HTTPS GCS URLs have one of the following formats:\n" +
            "https://storage.googleapis.com/bucket/key (for non-upload operations)\n" +
            "https://bucket.storage.googleapis.com/key (for non-upload operations)\n" +
            "https://www.googleapis.com/upload/storage/v1/b/bucket/o/key\n");
  }

  private static Matcher matchesGCSURI(String pattern, String uri)
  {
    return Pattern.compile(pattern).matcher(uri);
  }

  public static boolean backendIsGCS(String endpoint, URI uri)
  {
    // We consider endpoint (if exists) stronger evidence than URI
    if (endpoint != null)
    {
      URI endpointuri;
      try
      {
        if (!endpoint.startsWith("https://"))
          endpointuri = new URI("https://" + endpoint);
        else
          endpointuri = new URI(endpoint);
      }
      catch (URISyntaxException e)
      {
        return false;
      }

      return endpointuri.getHost().endsWith("googleapis.com");
    }

    if (uri != null)
      return "gs".equals(uri.getScheme());

    return false;
  }

  public static String getDefaultACL(boolean gcsMode)
  {
    if (gcsMode)
      return "projectPrivate";
    else
      return "bucket-owner-full-control";
  }

  public static String getGCSEndpoint(String command) throws URISyntaxException
  {
    if (command == "upload")
    {
      // We use GCS-native JSON API for uploads
      // We use HTTPS since we authenticate with OAuth
      return "https://www.googleapis.com";
    }
    else
    {
      // Currently, we use S3-compatible XML API for non-upload operations
      return "https://storage.googleapis.com";
    }
  }

  public static String getBucket(URI uri)
  {
    if((!"s3".equals(uri.getScheme())) && (!"gs".equals(uri.getScheme())))
      throw new IllegalArgumentException("Object URL needs to have either 's3' or 'gs' as scheme");

    return uri.getAuthority();
  }

  public static String getObjectKey(URI uri)
  {
    String path = uri.getPath();

    if(path == null || path.length() == 0)
      throw new UsageException("URLs have the format scheme://bucket/key, where scheme is either 's3' or 'gs'");

    if(path.charAt(0) != '/')
      throw new UsageException("URLs have the format scheme://bucket/key, where scheme is either 's3' or 'gs'");
    
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
