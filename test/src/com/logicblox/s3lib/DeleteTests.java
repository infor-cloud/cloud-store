package com.logicblox.s3lib;

import java.io.File;
import java.net.URI;
import java.util.List;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class DeleteTests
  implements RetryListener
{
  private static CloudStoreClient _client = null;
  private static String _testBucket = null;
  private int _retryCount = 0;
  

  // RetryListener
  @Override
  public synchronized void retryTriggered(RetryEvent e)
  {
    ++_retryCount;
  }


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


  @Test
  public void testRetry()
    throws Throwable
  {
    // create test file and upload it
    String rootPrefix = TestUtils.addPrefix("delete-retry");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);
    Assert.assertEquals(
      originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // delete the file
    DeleteOptions opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(dest))
        .setObjectKey(Utils.getObjectKey(dest))
        .createDeleteOptions();
    try
    {
      // set retry and abort options
      ThrowableRetriableTask.addRetryListener(this);
      clearRetryCount();
      int retryCount = 10;
      int abortCount = 3;
      _client.setRetryCount(retryCount);
      DeleteOptions.setAbortInjectionCounter(abortCount);

      f = _client.delete(opts).get();
      Assert.assertNotNull(f);
      Assert.assertEquals(abortCount, getRetryCount());
    }
    finally
    {
      // reset retry and abort injection state so we don't affect other tests
      TestUtils.resetRetryCount();
      DeleteOptions.setAbortInjectionCounter(0);
      DeleteOptions.clearAbortInjectionCounters();
    }

    // verify the deletion
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount, objs.size());
    Assert.assertFalse(TestUtils.findObject(objs, Utils.getObjectKey(dest)));
  }
  

  @Test
  public void testRetryDir()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int testLoops = TestUtils.RETRY_COUNT;
int count = 0;
while(count < testLoops)
{
try
{
    // create simple directory structure with a few files and upload
    File top = TestUtils.createTmpDir(true);
    File a = TestUtils.createTextFile(top, 100);
    File b = TestUtils.createTextFile(top, 100);
    File sub = TestUtils.createTmpDir(top);
    File c = TestUtils.createTextFile(sub, 100);
    File d = TestUtils.createTextFile(sub, 100);
    File sub2 = TestUtils.createTmpDir(sub);
    File e = TestUtils.createTextFile(sub2, 100);

    String rootPrefix = TestUtils.addPrefix("delete-retry-dir/");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(5, uploaded.size());
    Assert.assertEquals(
      originalCount + uploaded.size(), TestUtils.listObjects(_testBucket, rootPrefix).size());

    // dryrun the delete and make sure the files still exist
    DeleteOptions opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(dest))
        .setObjectKey(Utils.getObjectKey(dest))
        .setRecursive(true)
        .createDeleteOptions();
    boolean oldGlobalFlag = false;
    try
    {
      // set retry and abort options
      oldGlobalFlag = DeleteOptions.useGlobalAbortCounter(true);
      ThrowableRetriableTask.addRetryListener(this);
      clearRetryCount();
      int retryCount = 10;
      int abortCount = 3;
      _client.setRetryCount(retryCount);
      DeleteOptions.setAbortInjectionCounter(abortCount);

      List<S3File> files = _client.deleteDir(opts).get();
      Assert.assertEquals(5, files.size());
      Assert.assertEquals(abortCount, getRetryCount());
    }
    finally
    {
      // reset retry and abort injection state so we don't affect other tests
      TestUtils.resetRetryCount();
      DeleteOptions.setAbortInjectionCounter(0);
      DeleteOptions.useGlobalAbortCounter(oldGlobalFlag);
      DeleteOptions.clearAbortInjectionCounters();
    }

    // verify the deletions
    Assert.assertEquals(0, TestUtils.listObjects(_testBucket, rootPrefix).size());
    return;
}
catch(Throwable t)
{
  ++count;
  if(count >= testLoops)
    throw t;
}
}
  }


  @Test
  public void testDryRunFile()
    throws Throwable
  {
    // create test file and upload it
    String rootPrefix = TestUtils.addPrefix("delete-dryrun");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);
    Assert.assertEquals(
      originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // dryrun the delete and make sure file still exists
    DeleteOptions opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(dest))
        .setObjectKey(Utils.getObjectKey(dest))
        .setDryRun(true)
        .createDeleteOptions();
    f = _client.delete(opts).get();
    Assert.assertNull(f);
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));
  }


  @Test
  public void testDryRunDir()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = TestUtils.RETRY_COUNT;
int count = 0;
while(count < retryCount)
{
try
{
    // create simple directory structure with a few files and upload
    File top = TestUtils.createTmpDir(true);
    File a = TestUtils.createTextFile(top, 100);
    File b = TestUtils.createTextFile(top, 100);
    String rootPrefix = TestUtils.addPrefix("delete-dryrun-dir/");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(2, uploaded.size());
    Assert.assertEquals(
      originalCount + 2, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // dryrun the delete and make sure the files still exist
    DeleteOptions opts = new DeleteOptionsBuilder()
        .setBucket(Utils.getBucket(dest))
        .setObjectKey(Utils.getObjectKey(dest))
        .setRecursive(true)
        .setDryRun(true)
        .createDeleteOptions();
    List<S3File> files = _client.deleteDir(opts).get();
    Assert.assertNull(files);
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 2, objs.size());

    String topN = rootPrefix + top.getName() + "/";
    Assert.assertTrue(TestUtils.findObject(objs, topN + a.getName()));
    Assert.assertTrue(TestUtils.findObject(objs, topN + b.getName()));
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
    catch(Exception ex)
    {
      // expected
      checkUsageException(ex, "Object not found");
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
    catch(Exception ex)
    {
      // expected
      checkUsageException(ex, "No objects found that match");
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


  private synchronized void clearRetryCount()
  {
    _retryCount = 0;
  }


  private synchronized int getRetryCount()
  {
    return _retryCount;
  }


  private void checkUsageException(Exception ex, String expectedMsg)
    throws Exception
  {
    UsageException uex = null;
    if(ex instanceof UsageException)
    {
      uex = (UsageException) ex;
    }
    else
    {
      if((null != ex.getCause()) && (ex.getCause() instanceof UsageException))
        uex = (UsageException) ex.getCause();
    }

    if(null == uex)
      throw ex;
    else
      Assert.assertTrue(uex.getMessage().contains(expectedMsg));
  }
}
