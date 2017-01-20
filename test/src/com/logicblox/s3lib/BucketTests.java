package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.Bucket;
//import java.lang.InterruptedException;
//import java.io.IOException;
//import java.net.URISyntaxException;
import java.net.URL;
//import java.security.GeneralSecurityException;
import java.util.List;
//import java.util.concurrent.ExecutionException;
import junit.framework.Assert;
import org.junit.Test;


public class BucketTests
{

  @Test
  public void testBasics()
    throws Throwable
//    throws URISyntaxException, InterruptedException, GeneralSecurityException,
//           ExecutionException, IOException
  {
    URL endpoint = TestOptions.getEndpoint();
    CloudStoreClient client = null;
    try
    {
      client = Utils.createCloudStoreClient("s3", endpoint.toString());
      List<Bucket> buckets = client.listBuckets().get();
      int originalCount = buckets.size();

      String testBucket = "test-bucket-" + System.currentTimeMillis();
      Assert.assertFalse(client.hasBucket(testBucket));

      client.createBucket(testBucket);
      Assert.assertTrue(client.hasBucket(testBucket));

      buckets = client.listBuckets().get();
      Assert.assertEquals(originalCount + 1, buckets.size());
      boolean found = false;
      for(Bucket b : buckets)
        if(b.getName().equals(testBucket))
          found = true;
      Assert.assertTrue(found);

      client.destroyBucket(testBucket);
      Assert.assertFalse(client.hasBucket(testBucket));

    }
    finally
    {
      if(null != client)
        client.shutdown();
    }
  }

}

