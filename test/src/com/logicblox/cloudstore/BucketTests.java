/*
  Copyright 2017, Infor Inc.

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

import java.net.URL;
import java.util.List;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class BucketTests
{
  private static CloudStoreClient _client = null;


  @BeforeClass
  public static void setUp()
    throws Throwable
  {
    _client = TestUtils.createClient(0);
  }


  @AfterClass
  public static void tearDown()
  {
    TestUtils.destroyClient(_client);
  }


  @Test
  public void testBasics()
    throws Throwable
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
    {
      if(b.getName().equals(testBucket))
        found = true;
    }
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

