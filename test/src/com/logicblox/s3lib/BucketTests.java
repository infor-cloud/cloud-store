package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.Bucket;
import java.lang.InterruptedException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class BucketTests
{
  private static CloudStoreClient _client = null;


  @BeforeClass
  public static void setUp()
    throws URISyntaxException, InterruptedException, MalformedURLException,
           GeneralSecurityException, IOException
  {
    _client = Utils.createCloudStoreClient(
      TestOptions.getService(), TestOptions.getEndpointString());
  }


  @AfterClass
  public static void tearDown()
  {
    if(null != _client)
    {
      _client.shutdown();
      _client = null;
    }
  }


  @Test
  public void testBasics()
    throws InterruptedException, ExecutionException
  {
    List<Bucket> buckets = _client.listBuckets().get();
    int originalCount = buckets.size();

    String testBucket = "cloud-store-ut-bucket-" + System.currentTimeMillis();
    Assert.assertFalse(_client.hasBucket(testBucket));

    _client.createBucket(testBucket);
    Assert.assertTrue(_client.hasBucket(testBucket));

    buckets = _client.listBuckets().get();
    Assert.assertEquals(originalCount + 1, buckets.size());
    boolean found = false;
    for(Bucket b : buckets)
      if(b.getName().equals(testBucket))
        found = true;
    Assert.assertTrue(found);

    _client.destroyBucket(testBucket);
    Assert.assertFalse(_client.hasBucket(testBucket));
  }


  @Test
  public void testCreateFailure()
  {
    String testBucket = "cloud-store-ut-bucket-" + System.currentTimeMillis();
    Assert.assertFalse(_client.hasBucket(testBucket));
    _client.createBucket(testBucket);
    Assert.assertTrue(_client.hasBucket(testBucket));

    try
    {
      _client.createBucket(testBucket);
      Assert.fail("Expected exception to be thrown");
    }
    catch(Throwable t)
    {
      // exception expected

      // clean up
      _client.destroyBucket(testBucket);
    }
  }


  @Test
  public void testDestroyFailure()
  {
    String testBucket = "cloud-store-ut-bucket-" + System.currentTimeMillis();
    Assert.assertFalse(_client.hasBucket(testBucket));
    try
    {
      _client.destroyBucket(testBucket);
      Assert.fail("Expected exception to be thrown");
    }
    catch(Throwable t)
    {
      // expected
    }
  }

}

