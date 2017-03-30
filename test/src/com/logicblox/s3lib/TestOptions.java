package com.logicblox.s3lib;

import java.io.IOException;
import java.lang.InterruptedException;
import java.net.URI;
import java.net.URL;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.ExecutionException;


public class TestOptions
{
  private static String _service = "s3";
  private static String _endpoint = null;
  private static URI _destUri = null;
  static boolean SKIP_CLEANUP = false;


  // handle all input parameters
  public static void parseArgs(String[] args)
  {
    String destPrefix = null;
    for(int i = 0; i < args.length; ++i)
    {
      if(args[i].equals("--help") || args[i].equals("-h"))
      {
        usage();
        System.exit(0);
      }
      else if(args[i].equals("--service"))
      {
        ++i;
        _service = args[i];
      }
      else if(args[i].equals("--endpoint"))
      {
        ++i;
        _endpoint = args[i];
      }
      else if(args[i].equals("--dest-prefix"))
      {
        ++i;
        destPrefix = args[i];
      }
      else if(args[i].equals("--keydir"))
      {
        ++i;
        Utils.setDefaultKeyDir(args[i]);
      }
      else
      {
        System.out.println("Error:  '" + args[i] + "' unexpected");
	usage();
        System.exit(1);
      }
    }

    if(null != destPrefix)
    {
      try
      {
        _destUri = Utils.getURI(destPrefix);
        _service = _destUri.getScheme();
      }
      catch(Throwable t)
      {
        System.out.println("Error: could not parse --dest-prefix URL ["
	   + t.getMessage() + "]");
	System.exit(1);
      }
    }

    if(!_service.equals("s3") && !_service.equals("gs"))
    {
      System.out.println("Error:  --service must be s3 or gs");
      System.exit(1);
    }
  }


  // return URL representing the endpoint used to connect to storage 
  // service, or null if not passed as a command line parameter
  public static URL getEndpoint()
    throws MalformedURLException
  {
    if(null == _endpoint)
      return null;
    else
      return new URL(_endpoint);
  }


  // return String representing the endpoint used to connect to storage 
  // service, or null if not passed as a command line parameter
  public static String getEndpointString()
    throws MalformedURLException
  {
    if(null == _endpoint)
      return null;
    else
      return getEndpoint().toString();
  }


  // return the name of the storage service (currently either s3 or gs)
  public static String getService()
  {
    return _service;
  }


  public static URI getDestUri()
  {
    return _destUri;
  }


  // return a string that describes the storage service used for the tests
  public static String getServiceDescription()
  {
    String ep = _endpoint;
    if((null == ep) && _service.equals("s3"))
      ep = "AWS";
    else if((null == ep) && _service.equals("gs"))
      ep = "GCS";
    String svc =_service + " (" + ep + ")";
    if(null != _destUri)
       svc = svc + ", in " + _destUri;
    return svc;
  }


  // create a new CloudStoreClient from service name and endpoint parameters.
  // use default retry count
  public static CloudStoreClient createClient()
    throws MalformedURLException, URISyntaxException, GeneralSecurityException,
           IOException
  {
    return Utils.createCloudStoreClient(getService(), getEndpointString());
  }


  // create a new CloudStoreClient from service name and endpoint parameters.
  // use specified retry count
  public static CloudStoreClient createClient(int retryCount)
    throws MalformedURLException, URISyntaxException, GeneralSecurityException,
           IOException
  {
    CloudStoreClient client =
      Utils.createCloudStoreClient(getService(), getEndpointString());
    client.setRetryCount(retryCount);
    return client;
  }


  // shutdown the CloudStoreClient
  public static void destroyClient(CloudStoreClient client)
  {
    if(null != client)
      client.shutdown();
  }


  // create a new unique bucket to be used for test cases.  this will fail
  // if it cannot create a new bucket in 1000 tries.  returns the name of
  // the bucket.
  public static String createBucket(CloudStoreClient client)
  {
    // give up if we can't find a unique bucket name in 1000 tries....
    Throwable lastException = null;
    for(int b = 0; b < 1000; ++b)
    {
      String bucket = "cloud-store-ut-bucket-" + System.currentTimeMillis() + "-" + b;
      try
      {
        client.createBucket(bucket);
        return bucket;
      }
      catch(Throwable t)
      {
        // ignore
        lastException = t;
      }
    }
    throw new RuntimeException(
      "failed to create a test bucket: " + lastException.getMessage(), 
      lastException);
  }


  // destroy the bucket created by testing.  first have to delete any
  // objects that still exist in the bucket
  public static void destroyBucket(CloudStoreClient client, String bucket)
    throws InterruptedException, ExecutionException
  {
    if(SKIP_CLEANUP || (null == bucket) || bucket.isEmpty())
      return;

    clearBucket(client, bucket, null);
    client.destroyBucket(bucket);
  }


  public static void clearBucket(CloudStoreClient client, String bucket, String prefix)
    throws InterruptedException, ExecutionException
  {
    if(SKIP_CLEANUP)
      return;
      
    ListOptionsBuilder builder = new ListOptionsBuilder()
      .setBucket(bucket)
      .setRecursive(true)
      .setIncludeVersions(false)
      .setExcludeDirs(false);
    if((null != prefix) && !prefix.isEmpty())
      builder.setObjectKey(prefix);
    ListOptions lsOpts = builder.createListOptions();
    List<S3File> objs = client.listObjects(lsOpts).get();
    for(S3File f : objs)
      client.delete(bucket, f.getKey()).get();
  }


  private static void usage()
  {
    System.out.println("usage:  TestRunner {--help || -h} {--service s3|gs} {--endpoint url} {--dest-prefix url} {--keydir dir}");
  }

}
