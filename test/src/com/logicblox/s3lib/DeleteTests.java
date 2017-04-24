package com.logicblox.s3lib;

import java.io.File;
import java.net.URI;
import java.util.List;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class DeleteTests
{
  private static CloudStoreClient _client = null;
  private static String _testBucket = null;
  

  @BeforeClass
  public static void setUp()
    throws Throwable
  {
    TestUtils.setUp();
    _testBucket = TestUtils.getTestBucket();
    _client = TestUtils.getClient();
  }


  @AfterClass
  public static void tearDown()
    throws Throwable
  {
    TestUtils.tearDown();
    _testBucket = null;
    _client = null;
  }


  // FIXME - break this into multiple functions for test isolation
  @Test
  public void testBasicOperations()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = TestUtils.RETRY_COUNT;
int count = 0;
while(count < retryCount)
{
try
{
    // create simple directory structure with a few files
    File top = TestUtils.createTmpDir(true);
    File a = TestUtils.createTextFile(top, 100);
    File b = TestUtils.createTextFile(top, 100);
    File sub = TestUtils.createTmpDir(top);
    File c = TestUtils.createTextFile(sub, 100);
    File d = TestUtils.createTextFile(sub, 100);
    File sub2 = TestUtils.createTmpDir(sub);
    File e = TestUtils.createTextFile(sub2, 100);

    String rootPrefix = TestUtils.addPrefix("delete-basics-" + count + "/");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // upload the directory
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(5, uploaded.size());
    int uploadCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    Assert.assertEquals(uploaded.size(), uploadCount);

    // try to delete a non-existent key, should fail without force
    URI uri = TestUtils.getUri(
      _testBucket, top.getName() + "/delete-basics-bad-" + System.currentTimeMillis(),
      rootPrefix);
    DeleteOptions opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(uri))
        .setObjectKey(Utils.getObjectKey(uri))
        .createDeleteOptions();
    String msg = null;
    try
    {
      _client.delete(opts).get();
      msg = "expected exception (object not found)";
    }
    catch(UsageException ex)
    {
      // expected
      Assert.assertTrue(ex.getMessage().contains("Object not found"));
    }
    Assert.assertNull(msg);
    Assert.assertEquals(uploadCount, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // non-existent key with force should be ok
    opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(uri))
        .setObjectKey(Utils.getObjectKey(uri))
	.setForceDelete(true)
        .createDeleteOptions();
    S3File f = _client.delete(opts).get();
    Assert.assertNotNull(f);
    Assert.assertEquals(uploadCount, TestUtils.listObjects(_testBucket, rootPrefix).size());


    // non-existent directory without force should fail
    uri = TestUtils.getUri(
     _testBucket, top.getName() + "-delete-basics-bad-dir-" + System.currentTimeMillis(),
     rootPrefix);
    opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(uri))
        .setObjectKey(Utils.getObjectKey(uri))
	.setRecursive(true)
        .createDeleteOptions();
    try
    {
      _client.deleteDir(opts).get();
      msg = "expected exception (object not found)";
    }
    catch(UsageException ex)
    {
      // expected
      Assert.assertTrue(ex.getMessage().contains("No objects found that match"));
    }
    Assert.assertNull(msg);
    Assert.assertEquals(uploadCount, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // non-existent directory with force should be OK
    opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(uri))
        .setObjectKey(Utils.getObjectKey(uri))
	.setRecursive(true)
	.setForceDelete(true)
        .createDeleteOptions();
    List<S3File> files = _client.deleteDir(opts).get();
    Assert.assertTrue(files.isEmpty());
    Assert.assertEquals(uploadCount, TestUtils.listObjects(_testBucket, rootPrefix).size());
    
    // delete folder that isn't empty without recursion should only delete top level files
    uri = TestUtils.getUri(_testBucket, top, rootPrefix);
    opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(uri))
        .setObjectKey(Utils.getObjectKey(uri))
	.setRecursive(false)
        .createDeleteOptions();
    files = _client.deleteDir(opts).get();
    Assert.assertEquals(2, files.size());
    Assert.assertEquals(uploadCount - 2, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // delete single exiting file
    uri = TestUtils.getUri(
      _testBucket, top.getName() + "/" + sub.getName() + "/" + c.getName(), rootPrefix);
    opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(uri))
        .setObjectKey(Utils.getObjectKey(uri))
        .createDeleteOptions();
    f = _client.delete(opts).get();
    Assert.assertNotNull(f);
    Assert.assertEquals(uploadCount - 3, TestUtils.listObjects(_testBucket, rootPrefix).size());
    Assert.assertNull(_client.exists(uri).get());

    // recursively delete the rest of the files (should just be two left by now)
    uri = TestUtils.getUri(_testBucket, top, rootPrefix);
    opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(uri))
        .setObjectKey(Utils.getObjectKey(uri))
	.setRecursive(true)
        .createDeleteOptions();
    files = _client.deleteDir(opts).get();
    Assert.assertEquals(2, files.size());
    Assert.assertEquals(0, TestUtils.listObjects(_testBucket, rootPrefix).size());

    return;
}
catch(Throwable t)
{
  ++count;
  if(count >= retryCount)
    throw t;
}
}
  }

}
