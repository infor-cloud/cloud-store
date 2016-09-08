package com.logicblox.s3lib;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class Utils
{
  public static DateFormat getDefaultDateFormat()
  {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSSSSS");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));

    return df;
  }

  public static String getDefaultKeyDirectory()
  {
    String home = System.getenv("HOME");
    if (home == null)
      home = System.getProperty("user.home");
    return home + File.separator + ".s3lib-keys";
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

  public static boolean isStorageServiceURL(String url)
  {
    try
    {
      return getURI(url) != null;
    }
    catch (URISyntaxException e)
    {
      return false;
    }
    catch (UsageException e)
    {
      return false;
    }
  }

  /**
   * Enum values for all supported storage services.
   * <p>
   * {@code UNKNOWN} value represents storage services that run on an unknown
   * endpoint but provide an API compatible with one of the supported services
   * (e.g. S3 or GCS-compatible API). It's mostly useful for testing purposes.
   */
  public enum StorageService
  {
    S3, GCS
  }

  public static StorageService detectStorageService(String endpoint, URI uri)
      throws URISyntaxException
  {
    // We consider endpoint (if exists) stronger evidence than URI
    if (endpoint != null)
    {
      URI endpointuri;
      if (!endpoint.startsWith("https://") && !endpoint.startsWith("http://"))
        endpointuri = new URI("https://" + endpoint);
      else
        endpointuri = new URI(endpoint);

      if (endpointuri.getHost().endsWith("amazonaws.com"))
        return StorageService.S3;
      else if (endpointuri.getHost().endsWith("googleapis.com"))
        return StorageService.GCS;
    }

    if (uri != null)
    {
      switch (uri.getScheme())
      {
        case "s3":
          return StorageService.S3;
        case "gs":
          return StorageService.GCS;
      }
    }

    throw new UsageException("Cannot detect storage service: endpoint " +
        endpoint +  ", URI " + uri);
  }

  public static String getDefaultCannedACLFor(StorageService service)
  {
    switch (service)
    {
      case S3:
        return S3Client.defaultCannedACL;
      case GCS:
        return GCSClient.defaultCannedACL;
      default:
        throw new UsageException("Unknown storage service " + service);
    }
  }

  public static boolean isValidCannedACLFor(StorageService service, String
      cannedACL)
  {
    switch (service)
    {
      case S3:
        return S3Client.isValidCannedACL(cannedACL);
      case GCS:
        return GCSClient.isValidCannedACL(cannedACL);
      default:
        throw new UsageException("Unknown storage service " + service);
    }
  }

  public static final List<String> defaultCredentialProvidersS3 = Arrays.asList(
      "env-vars", "system-properties", "credentials-profile", "ec2-metadata-service");

  public static AWSCredentialsProvider getCredentialsProviderS3(List<String> credentialsProvidersS3)
  {
    if (credentialsProvidersS3 == null || credentialsProvidersS3.size() == 0)
      return new DefaultAWSCredentialsProviderChain();

    List<AWSCredentialsProvider> choices = new ArrayList<>();
    for (String cp : credentialsProvidersS3)
    {
      switch (cp)
      {
        case "env-vars":
          choices.add(new EnvironmentVariableCredentialsProvider());
          break;
        case "system-properties":
          choices.add(new SystemPropertiesCredentialsProvider());
          break;
        case "credentials-profile":
          choices.add(new ProfileCredentialsProvider());
          break;
        case "ec2-metadata-service":
          choices.add(new InstanceProfileCredentialsProvider());
          break;
        default:
          break;
      }
    }

    if (choices.size() == 0)
      return new DefaultAWSCredentialsProviderChain();

    AWSCredentialsProvider[] choicesArr = new AWSCredentialsProvider[choices.size()];
    choicesArr = choices.toArray(choicesArr);

    return new AWSCredentialsProviderChain(choicesArr);
  }

  public static final String GCS_XML_ACCESS_KEY_ENV_VAR = "GCS_XML_ACCESS_KEY";

  public static final String GCS_XML_SECRET_KEY_ENV_VAR = "GCS_XML_SECRET_KEY";

  static class GCSXMLEnvironmentVariableCredentialsProvider implements AWSCredentialsProvider {

    public AWSCredentials getCredentials() {
      String accessKey = System.getenv(GCS_XML_ACCESS_KEY_ENV_VAR);
      String secretKey = System.getenv(GCS_XML_SECRET_KEY_ENV_VAR);

      if (accessKey == null || secretKey == null) {
        throw new UsageException(
            "Unable to load GCS credentials from environment variables " +
                GCS_XML_ACCESS_KEY_ENV_VAR + " and " + GCS_XML_SECRET_KEY_ENV_VAR);
      }

      return new BasicAWSCredentials(accessKey, secretKey);
    }

    public void refresh() {}

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  public static GCSXMLEnvironmentVariableCredentialsProvider getGCSXMLEnvironmentVariableCredentialsProvider() {
    return new GCSXMLEnvironmentVariableCredentialsProvider();
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

  public static boolean viaProxy()
  {
    if (System.getenv("HTTP_PROXY") != null || System.getenv("HTTPS_PROXY") != null)
      return true;
    return false;
  }

  public static ClientConfiguration setProxy(ClientConfiguration clientCfg)
  {
    if (!Utils.viaProxy())
      return clientCfg;

    String proxy = System.getenv("HTTPS_PROXY");
    if (proxy == null)
    {
      proxy = System.getenv("HTTP_PROXY");
    }

    URL url;
    try
    {
      url = new URL(proxy);
    } catch (MalformedURLException e)
    {
      System.err.println("Malformed proxy url: " + proxy);
      e.printStackTrace();
      return clientCfg;
    }

    if (System.getenv("HTTPS_PROXY") != null)
    {
      clientCfg.setProtocol(Protocol.HTTPS);
    }
    else
    {
      clientCfg.setProtocol(Protocol.HTTP);
    }

    clientCfg.setProxyHost(url.getHost());
    clientCfg.setProxyPort(url.getPort());
    if (url.getUserInfo() != null)
    {
      String[] userInfo = url.getUserInfo().split(":");
      clientCfg.setProxyUsername(userInfo[0]);
      clientCfg.setProxyPassword(userInfo[1]);
    }

    return clientCfg;
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
