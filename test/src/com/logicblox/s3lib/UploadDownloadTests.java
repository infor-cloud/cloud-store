package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class UploadDownloadTests
  implements OverallProgressListenerFactory, OverallProgressListener,
             RetryListener
{
  private static CloudStoreClient _client = null;
  private static String _testBucket = null;
  
  private Set<String> _partSet = new HashSet<String>();
  private int _retryCount = 0;


  // RetryListener
  @Override
  public synchronized void retryTriggered(RetryEvent e)
  {
    ++_retryCount;
  }

  // OverallProgressListenerFactory
  @Override
  public OverallProgressListener create(ProgressOptions progressOptions)
  {
    return this;
  }


  // OverallProgressListener
  @Override
  public synchronized void progress(PartProgressEvent ev)
  {
    _partSet.add(ev.getPartId());
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
  public void testExists()
    throws Throwable
  {
    // NOTE - Not testing object keys that look like folders (i.e.
    // s3://my-bucket/a/b/).  minio reports a bad request with these
    // keys.  Plus whether a folder "exists" or not in AWS or GCS depends
    // on whether it was created from the console or implicitly through
    // some upload.
    
    // upload a file and verify it exists
    String rootPrefix = TestUtils.addPrefix("a/b/");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);
    Assert.assertNotNull(_client.exists(dest).get());

    // check for missing file key
    URI src = TestUtils.getUri(
      _testBucket, toUpload, rootPrefix + "-missing-" + System.currentTimeMillis());
    Assert.assertNull(_client.exists(src).get());
    
    // bad bucket should fail
    src = TestUtils.getUri(
    _testBucket + "-missing-" + System.currentTimeMillis(), toUpload, rootPrefix);
    Assert.assertNull(_client.exists(src).get());
  }


  @Test
  public void testSimpleUploadDownload()
    throws Throwable
  {
    String rootPrefix = TestUtils.addPrefix("a/b/");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // create a small file and upload it
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = rootPrefix + toUpload.getName();
    Assert.assertTrue(TestUtils.findObject(objs, key));

    // download the file and compare it with the original
    File dlTemp = TestUtils.createTmpFile();
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));

    // test a few other misc cloud-store functions
    Assert.assertNotNull(_client.exists(dest).get());
    Assert.assertNotNull(_client.exists(_testBucket, key).get());
    Assert.assertEquals(
      dest.toString(), _client.getUri(_testBucket, key).toString());

    Assert.assertNotNull(TestUtils.deleteObject(dest));
    Assert.assertNull(_client.exists(dest).get());
  }


  @Test
  public void testFailedUploadRetry()
    throws Throwable
  {
    // NOTE:  This test will log an exception stack trace that can be ignored
    try
    {
      ThrowableRetriableTask.addRetryListener(this);
      clearRetryCount();
      int retryCount = 3;
      _client.setRetryCount(retryCount);
      UploadOptions.setAbortInjectionCounter(10);
      List<S3File> objs = TestUtils.listTestBucketObjects();
      int originalCount = objs.size();

      // create test file - AWS requires a min 5M chunk size...
      int chunkSize = 5 * 1024 * 1024;
      int fileSize = chunkSize + 1000000;
      File toUpload = TestUtils.createTextFile(fileSize);
      String rootPrefix = TestUtils.addPrefix("");
      URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

      // upload file in multiple concurrent chunks
      clearParts();
      UploadOptions upOpts = new UploadOptionsBuilder()
        .setFile(toUpload)
        .setBucket(Utils.getBucket(dest))
        .setObjectKey(Utils.getObjectKey(dest))
        .setChunkSize(chunkSize)
        .createUploadOptions();
      try
      {
        _client.upload(upOpts).get();
        Assert.fail("expected exception");
      }
      catch(Throwable t)
      {
        // expected
      }

      // we'll get a set of retries for each part
      int partCount = TestUtils.getExpectedPartCount(fileSize, chunkSize);
      int expectedRetryCount = partCount * (retryCount - 1);
      Assert.assertEquals(expectedRetryCount, getRetryCount());

      // make sure the file isn't on the server
      objs = TestUtils.listTestBucketObjects();
      Assert.assertEquals(originalCount, objs.size());
      Assert.assertFalse(TestUtils.findObject(objs, TestUtils.addPrefix(toUpload.getName())));
    }
    finally
    {
      // reset retry and abort injection state so we don't affect other tests
      TestUtils.resetRetryCount();
      UploadOptions.setAbortInjectionCounter(0);
    }
  }

    
  @Test
  public void testSuccessfulUploadRetry()
    throws Throwable
  {
    try
    {
      ThrowableRetriableTask.addRetryListener(this);
      clearRetryCount();
      int retryCount = 10;
      int abortCount = 3;
      _client.setRetryCount(retryCount);
      UploadOptions.setAbortInjectionCounter(abortCount);
      List<S3File> objs = TestUtils.listTestBucketObjects();
      int originalCount = objs.size();

      // create test file - AWS requires a min 5M chunk size...
      int chunkSize = 5 * 1024 * 1024;
      int fileSize = chunkSize + 1000000;
      File toUpload = TestUtils.createTextFile(fileSize);
      String rootPrefix = TestUtils.addPrefix("");
      URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

      // upload file in multiple concurrent chunks
      UploadOptions upOpts = new UploadOptionsBuilder()
        .setFile(toUpload)
        .setBucket(Utils.getBucket(dest))
        .setObjectKey(Utils.getObjectKey(dest))
        .setChunkSize(chunkSize)
        .createUploadOptions();
      S3File f = _client.upload(upOpts).get();
      Assert.assertNotNull(f);
      Assert.assertEquals(abortCount, getRetryCount());

      // validate the upload
      objs = TestUtils.listTestBucketObjects();
      Assert.assertEquals(originalCount + 1, objs.size());
      Assert.assertTrue(TestUtils.findObject(objs, TestUtils.addPrefix(toUpload.getName())));

      // download the file and compare it with the original
      File dlTemp = TestUtils.createTmpFile();
      DownloadOptions dlOpts = new DownloadOptionsBuilder()
        .setFile(dlTemp)
        .setUri(dest)
        .setRecursive(false)
        .setOverwrite(true)
        .createDownloadOptions();
      f = _client.download(dlOpts).get();
      Assert.assertNotNull(f.getLocalFile());
      Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
    }
    finally
    {
      // reset retry and abort injection state so we don't affect other tests
      TestUtils.resetRetryCount();
      UploadOptions.setAbortInjectionCounter(0);
    }
  }


  @Test
  public void testUploadAttributes()
    throws Throwable
  {
    // create a small file and upload it
    long fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);

    Assert.assertEquals(fileSize, toUpload.length());
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);
    Assert.assertEquals(
      toUpload.getAbsolutePath(), f.getLocalFile().getAbsolutePath());
    Assert.assertNotNull(f.getETag());
    Assert.assertEquals(TestUtils.addPrefix(toUpload.getName()), f.getKey());
    Assert.assertEquals(_testBucket, f.getBucketName());
//    Assert.assertTrue(f.getVersionId().isPresent());
      // FIXME - this info is not being populated right now
//    Assert.assertTrue(f.getSize().isPresent());
      // FIXME - this info is not being populated right now
//    Assert.assertTrue(f.getTimestamp().isPresent());
      // FIXME - this info is not being populated right now

    List<S3File> objs = TestUtils.listTestBucketObjects();
    for(S3File o : objs)
    {
      if(o.getKey() == TestUtils.addPrefix(toUpload.getName()))
      {
        Assert.assertNull(o.getLocalFile());
//        Assert.asserTrue(o.getETag() != "");
          // FIXME - this info is not being populated right now
        Assert.assertEquals(_testBucket, o.getBucketName());
//        Assert.assertTrue(o.getVersionId().isPresent());
          // FIXME - this info is not being populated right now
        Assert.assertEquals(fileSize, o.getSize().get().longValue());
//        Assert.assertTrue(o.getTimestamp().isPresent());
          // FIXME - this info is not being populated right now
      }
    }

    ObjectMetadata meta = _client.exists(dest).get();
    Assert.assertNotNull(meta.getLastModified());
    Assert.assertEquals(fileSize, meta.getContentLength());
    Assert.assertEquals(fileSize, meta.getInstanceLength());
    Assert.assertNotNull(meta.getETag());
  }


  @Test
  public void testDownloadAttributes()
    throws Throwable
  {
    // create a small file and upload it
    long fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    TestUtils.uploadFile(toUpload, dest);

    // now download it
    File dlTemp = TestUtils.createTmpFile();
    S3File f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f);
    Assert.assertEquals(
      dlTemp.getAbsolutePath(), f.getLocalFile().getAbsolutePath());
    Assert.assertNotNull(f.getETag());
    Assert.assertEquals(TestUtils.addPrefix(toUpload.getName()), f.getKey());
    Assert.assertEquals(_testBucket, f.getBucketName());
//    Assert.assertTrue(f.getVersionId().isPresent());
      // FIXME - this info is not being populated right now
//    Assert.assertTrue(f.getSize().isPresent());
      // FIXME - this info is not being populated right now
//    Assert.assertTrue(f.getTimestamp().isPresent());
      // FIXME - this info is not being populated right now
  }


  @Test
  public void testEmptyFile()
    throws Throwable
  {
    List<S3File> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // upload a file
    File toUpload = TestUtils.createTextFile(0);
    Assert.assertEquals(0, toUpload.length());
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = TestUtils.addPrefix(toUpload.getName());
    Assert.assertTrue(TestUtils.findObject(objs, key));

    // download, overwriting a larger file
    File dlTemp = TestUtils.createTextFile(100);
    Assert.assertEquals(100, dlTemp.length());
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertEquals(0, dlTemp.length());
  }

// PUT THIS BACK ONCE WE GET GEORGE'S FIX MERGED IN
//  @Test
  public void testEmptyEncryptedFile()
    throws Throwable
  {
    // generate a new public/private key pair
    String keyName = "cloud-store-ut";
    File keydir = TestUtils.createTmpDir(true);
    TestUtils.setKeyProvider(keydir);
    String[] keys = TestUtils.createEncryptionKey(keydir, keyName);
    String privateKey = keys[0];
    String publicKey = keys[1];

    List<S3File> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // upload a file
    File toUpload = TestUtils.createTextFile(0);
    Assert.assertEquals(0, toUpload.length());
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = TestUtils.addPrefix(toUpload.getName());
    Assert.assertTrue(TestUtils.findObject(objs, key));

    // download, overwriting a larger file
    File dlTemp = TestUtils.createTextFile(100);
    Assert.assertEquals(100, dlTemp.length());
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertEquals(0, dlTemp.length());
  }


  @Test
  public void testDownloadMissingFile()
    throws Throwable
  {
    // upload a file
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // test missing file
    File dlTemp = TestUtils.createTmpFile();
    URI src = TestUtils.getUri(_testBucket, toUpload, rootPrefix + "missing.txt");
    String msg = null;
    try
    {
      TestUtils.downloadFile(src, dlTemp);
      msg = "expected exception";
    }
    catch(Throwable t)
    {
      Assert.assertTrue(t.getMessage().contains("Object not found"));
    }
    Assert.assertNull(msg);

    // test missing bucket
    src = TestUtils.getUri(_testBucket + "-missing", toUpload, rootPrefix);
    msg = null;
    try
    {
      TestUtils.downloadFile(src, dlTemp);
      msg = "expected exception";
    }
    catch(Throwable t)
    {
      Assert.assertTrue(t.getMessage().contains("Object not found"));
    }
    Assert.assertNull(msg);
  }
  

  @Test
  public void testDownloadNoOverwriteFile()
    throws Throwable
  {
    List<S3File> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // upload a file
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = TestUtils.addPrefix(toUpload.getName());
    Assert.assertTrue(TestUtils.findObject(objs, key));

    // download without overwrite to make sure it fails
    File dlTemp = TestUtils.createTextFile(10);
    Assert.assertFalse(TestUtils.compareFiles(toUpload, dlTemp));
    try
    {
      f = TestUtils.downloadFile(dest, dlTemp, false);
      Assert.fail("Expected download exception");
    }
    catch(Throwable t)
    {
      // expected
    }

    // now download with overwite to make sure it replaces the file
    f = TestUtils.downloadFile(dest, dlTemp, true);
    Assert.assertTrue(TestUtils.compareFiles(toUpload, dlTemp));
  }

  
  @Test
  public void testDirectoryOverwrite()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = 5;
int count = 0;
while(count < retryCount)
{
try
{
    // create simple directory structure and upload
    File top = TestUtils.createTmpDir(true);
    File a = TestUtils.createTextFile(top, 100);
    File b = TestUtils.createTextFile(top, 100);
    String rootPrefix = TestUtils.addPrefix("dir-overwrite/");
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(2, uploaded.size());

    // should fail if source doesn't exist
    File dlDir = TestUtils.createTmpDir(true);
    String badPrefix = TestUtils.addPrefix("dir-overwrite-bad"
      + System.currentTimeMillis() + "/");
    URI src = TestUtils.getUri(_testBucket, top, badPrefix);
    String msg = null;
    try
    {
      TestUtils.downloadDir(src, dlDir, false);
      msg = "expected exception (no object found)";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("No objects found"));
    }
    Assert.assertNull(msg);

    // should fail if dest is existing file with no overwrite
    File existingFile = TestUtils.createTextFile(top, 1);
    src = dest;
    try
    {
      TestUtils.downloadDir(src, existingFile, true, false);
      msg = "expected exception (can't overwrite file)";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("must be a directory"));
    }
    Assert.assertNull(msg);
    
    // should succeed if dest is existing file with overwrite
    List<S3File> downloaded = TestUtils.downloadDir(src, existingFile, true, true);
    Assert.assertEquals(2, downloaded.size());
    Assert.assertTrue(existingFile.isDirectory());
    
    // should fail if dest is existing directory and one file exists with no overwrite
    File destDir = TestUtils.createTmpDir(true);
    existingFile = new File(destDir, a.getName());
    Assert.assertTrue(existingFile.createNewFile());
    Assert.assertTrue(existingFile.exists());
    try
    {
      TestUtils.downloadDir(src, destDir, true, false);
      msg = "expected exception (can't overwrite file)";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("already exists"));
    }
    Assert.assertNull(msg);
    Assert.assertEquals(1, destDir.list().length);
         // should just have the original file.  everything else should be cleaned up
    
    // should succeed if dest is existing directory and one file exists with overwrite
    downloaded = TestUtils.downloadDir(src, destDir, true, true);
    Assert.assertEquals(2, downloaded.size());
    
    // should succeed if dest doesn't exist
    destDir = new File(destDir.getAbsolutePath() + "-missing-" + System.currentTimeMillis());
    Assert.assertFalse(destDir.exists());
    downloaded = TestUtils.downloadDir(src, destDir, true, true);
    Assert.assertEquals(2, downloaded.size());
    
    return;
}
catch(Throwable t)
{
  ++count;
  if(count >= retryCount)
    throw t;
//  System.out.println(" ++++++++++++++++ RETRYING: " + t.getMessage());
}
}
  }


  @Test
  public void testDirectoryUploadDownload()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = 5;
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

    String rootPrefix = TestUtils.addPrefix("dir-ul-dl/");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // upload the directory
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(5, uploaded.size());

    // verify that the structure was replicated
    objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 5, objs.size());
    String topN = rootPrefix + top.getName() + "/";
    String subN = topN + sub.getName() + "/";
    String sub2N = subN + sub2.getName() + "/";
    Assert.assertTrue(TestUtils.findObject(objs, topN + a.getName()));
    Assert.assertTrue(TestUtils.findObject(objs, topN + b.getName()));
    Assert.assertTrue(TestUtils.findObject(objs, subN + c.getName()));
    Assert.assertTrue(TestUtils.findObject(objs, subN + d.getName()));
    Assert.assertTrue(TestUtils.findObject(objs, sub2N + e.getName()));

    // non-recursive directory download
    File dlDir = TestUtils.createTmpDir(true);
    List<S3File> downloaded = TestUtils.downloadDir(dest, dlDir, false);
    Assert.assertEquals(2, downloaded.size());
    Assert.assertEquals(2, dlDir.list().length);
    Assert.assertTrue(TestUtils.compareFiles(a, new File(dlDir, a.getName())));
    Assert.assertTrue(TestUtils.compareFiles(b, new File(dlDir, b.getName())));

    // recursive directory download
    File dlDir2 = TestUtils.createTmpDir(true);
    downloaded = TestUtils.downloadDir(dest, dlDir2, true);
    Assert.assertEquals(5, downloaded.size());
    Assert.assertEquals(3, dlDir2.list().length);
    Assert.assertTrue(TestUtils.compareFiles(a, new File(dlDir2, a.getName())));
    Assert.assertTrue(TestUtils.compareFiles(b, new File(dlDir2, b.getName())));

    File dlsub = new File(dlDir2, sub.getName());
    Assert.assertTrue(dlsub.exists() && dlsub.isDirectory());
    Assert.assertEquals(3, dlsub.list().length);
    Assert.assertTrue(TestUtils.compareFiles(c, new File(dlsub, c.getName())));
    Assert.assertTrue(TestUtils.compareFiles(d, new File(dlsub, d.getName())));

    File dlsub2 = new File(dlsub, sub2.getName());
    Assert.assertTrue(dlsub2.exists() && dlsub2.isDirectory());
    Assert.assertEquals(1, dlsub2.list().length);
    Assert.assertTrue(TestUtils.compareFiles(e, new File(dlsub2, e.getName())));
    return;
}
catch(Throwable t)
{
  ++count;
  if(count >= retryCount)
    throw t;
//  System.out.println(" ++++++++++++++++ RETRYING: " + t.getMessage());
}
}
  }


  @Test
  public void testMultipartUploadDownload()
    throws Throwable
  {
    List<S3File> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // create test file - AWS requires a min 5M chunk size...
    int chunkSize = 5 * 1024 * 1024;
    int fileSize = chunkSize + 1000000;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

    // upload file in multiple concurrent chunks
    clearParts();
    UploadOptions upOpts = new UploadOptionsBuilder()
      .setFile(toUpload)
      .setBucket(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setOverallProgressListenerFactory(this)
      .setChunkSize(chunkSize)
      .createUploadOptions();
    S3File f = _client.upload(upOpts).get();
    Assert.assertNotNull(f);

    // validate the upload
    int partCount = getPartCount();
    int expectedCount = TestUtils.getExpectedPartCount(fileSize, chunkSize);
    Assert.assertEquals(expectedCount, partCount);
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, TestUtils.addPrefix(toUpload.getName())));

    // download the file and compare it with the original
    File dlTemp = TestUtils.createTmpFile();
    DownloadOptions dlOpts = new DownloadOptionsBuilder()
      .setFile(dlTemp)
      .setUri(dest)
      .setRecursive(false)
      .setOverwrite(true)
      .setOverallProgressListenerFactory(this)
      .createDownloadOptions();
    clearParts();
    f = _client.download(dlOpts).get();
    partCount = getPartCount();
    Assert.assertEquals(expectedCount, partCount);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
  }


  @Test
  public void testSmallMultipartUploadDownload()
    throws Throwable
  {
    // files smaller than the minimum chunk size can still be uploaded using
    // multi-part protocol.  should just use a single part.
    
    List<S3File> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // create test file - AWS requires a min 5M chunk size...
    int chunkSize = 5 * 1024 * 1024;
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

    // upload file in multiple concurrent chunks
    clearParts();
    UploadOptions upOpts = new UploadOptionsBuilder()
      .setFile(toUpload)
      .setBucket(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setOverallProgressListenerFactory(this)
      .setChunkSize(chunkSize)
      .createUploadOptions();
    S3File f = _client.upload(upOpts).get();
    Assert.assertNotNull(f);

    // validate the upload
    int partCount = getPartCount();
    int expectedCount = TestUtils.getExpectedPartCount(fileSize, chunkSize);
    Assert.assertEquals(expectedCount, partCount);
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, TestUtils.addPrefix(toUpload.getName())));

    // download the file and compare it with the original
    File dlTemp = TestUtils.createTmpFile();
    DownloadOptions dlOpts = new DownloadOptionsBuilder()
      .setFile(dlTemp)
      .setUri(dest)
      .setRecursive(false)
      .setOverwrite(true)
      .setOverallProgressListenerFactory(this)
      .createDownloadOptions();
    clearParts();
    f = _client.download(dlOpts).get();
    partCount = getPartCount();
    Assert.assertEquals(expectedCount, partCount);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
  }
  

  @Test
  public void testSimpleCopy()
    throws Throwable
  {
    List<S3File> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // create test file and upload it
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, TestUtils.addPrefix(toUpload.getName())));

    // copy file
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f.getKey())
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(f.getKey() + "-COPY")
       .createCopyOptions();
    S3File copy = _client.copy(copyOpts).get();
    String expectedKey = TestUtils.addPrefix(toUpload.getName() + "-COPY");
    Assert.assertEquals(expectedKey, copy.getKey());

    // check for the copy
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 2, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, TestUtils.addPrefix(toUpload.getName())));
    Assert.assertTrue(TestUtils.findObject(objs, expectedKey));

    // download and compare copy
    File dlTemp = TestUtils.createTmpFile();
    dest = new URI(dest.toString() + "-COPY");
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
  }


  @Test
  public void testCrossBucketCopy()
    throws Throwable
  {
    // skip this if we're using a pre-exising test bucket, assuming we're
    // running against a server that we don't want to (or can't) create
    // buckets in...
    if(null != TestUtils.getPrefix())
       return;
    
    List<S3File> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // create test file and upload it
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, TestUtils.addPrefix(toUpload.getName())));

    // copy file
    String bucket2 = TestUtils.createTestBucket();
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f.getKey())
       .setDestinationBucketName(bucket2)
       .setDestinationKey(f.getKey())
       .createCopyOptions();
    S3File copy = _client.copy(copyOpts).get();

    // check for the copy in 1st bucket, should be the same
    List<S3File> copyObjs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(objs.size(), copyObjs.size());
    for(S3File sf : copyObjs)
      Assert.assertTrue(TestUtils.findObject(objs, sf.getKey()));

    // check for the copy in 2nd bucket
    copyObjs = TestUtils.listObjects(bucket2, rootPrefix);
    Assert.assertEquals(1, copyObjs.size());
    Assert.assertTrue(TestUtils.findObject(copyObjs, toUpload.getName()));

    // download and compare copy
    File dlTemp = TestUtils.createTmpFile();
    URI src = new URI(TestUtils.getService() + "://" + bucket2 + "/"
                      + toUpload.getName());
    DownloadOptions dlOpts = new DownloadOptionsBuilder()
      .setFile(dlTemp)
      .setUri(src)
      .setRecursive(false)
      .setOverwrite(true)
      .createDownloadOptions();
    f = _client.download(dlOpts).get();

    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
  }


  @Test
  public void testCopyDir()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = 5;
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

    String rootPrefix = TestUtils.addPrefix("copy-dir/");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // upload the directory
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(5, uploaded.size());

    // non-recursive copy
    String topN = rootPrefix + top.getName() + "/";
    String copyTopN = rootPrefix + top.getName() + "-COPY/";
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(topN)
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(copyTopN)
       .setRecursive(false)
       .createCopyOptions();
    List<S3File> copy = _client.copyToDir(copyOpts).get();
    Assert.assertEquals(2, copy.size());

    // verify the non-recursive copy
    List<S3File> copyObjs = TestUtils.listObjects(_testBucket, rootPrefix);
    int currentSize = copyObjs.size();
    Assert.assertEquals(originalCount + 5 + 2, currentSize);
        // original files plus 5 uploaded plus 2 copied
    Assert.assertTrue(TestUtils.findObject(copyObjs, copyTopN + a.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, copyTopN + b.getName()));

    // recursive copy
    String copyTopN2 = topN + "COPY2/";
    copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(topN)
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(copyTopN2)
       .setRecursive(true)
       .createCopyOptions();
    copy = _client.copyToDir(copyOpts).get();
    Assert.assertEquals(5, copy.size());

    // verify the recursive copy
    String subN = copyTopN2 + sub.getName() + "/";
    String sub2N = subN + sub2.getName() + "/";
    copyObjs = TestUtils.listObjects(_testBucket, rootPrefix);
    int lastSize = copyObjs.size();
    Assert.assertEquals(currentSize + 5, lastSize);
       // previous size plus 5 copies
    Assert.assertTrue(TestUtils.findObject(copyObjs, copyTopN2 + a.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, copyTopN2 + b.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, subN + c.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, subN + d.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, sub2N + e.getName()));
    return;
}
catch(Throwable t)
{
  ++count;
  if(count >= retryCount)
    throw t;
//  System.out.println(" ++++++++++++++++ RETRYING: " + t.getMessage());
}
}
  }


  @Test
  public void testCopyMissingDestBucket()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = 5;
int count = 0;
while(count < retryCount)
{
try
{
    // skip this if we're using a pre-exising test bucket, assuming we're
    // running against a server that we don't want to (or can't) create
    // buckets in...
    if(null != TestUtils.getPrefix())
       return;
    
    // create simple directory structure with a few files
    File top = TestUtils.createTmpDir(true);
    File a = TestUtils.createTextFile(top, 100);

    String rootPrefix = TestUtils.addPrefix("copy-missing-dest-bucket/");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // upload the directory
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(1, uploaded.size());

    String missingBucketName = "MISSING-cloud-store-ut-bucket-" + System.currentTimeMillis();
    String topN = rootPrefix + top.getName() + "/";
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(topN)
       .setDestinationBucketName(missingBucketName)
       .setDestinationKey(topN)
       .setRecursive(true)
       .createCopyOptions();
    try
    {
      _client.copyToDir(copyOpts).get();
      Assert.fail("Exception expected");
    }
    catch(Exception ex)
    {
      if(-1 == ex.getMessage().indexOf("specified bucket is not valid"))
         Assert.fail("Unexpected exception: " + ex.getMessage());
      else
         return;
    }
}
catch(Throwable t)
{
  ++count;
  if(count >= retryCount)
    throw t;
//  System.out.println(" ++++++++++++++++ RETRYING: " + t.getMessage());
}
}
  }


  @Test
  public void testCrossBucketCopyDir()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = 5;
int count = 0;
while(count < retryCount)
{
try
{
    // skip this if we're using a pre-exising test bucket, assuming we're
    // running against a server that we don't want to (or can't) create
    // buckets in...
    if(null != TestUtils.getPrefix())
       return;
    
    // create simple directory structure with a few files
    File top = TestUtils.createTmpDir(true);
    File a = TestUtils.createTextFile(top, 100);
    File b = TestUtils.createTextFile(top, 100);
    File sub = TestUtils.createTmpDir(top);
    File c = TestUtils.createTextFile(sub, 100);
    File d = TestUtils.createTextFile(sub, 100);
    File sub2 = TestUtils.createTmpDir(sub);
    File e = TestUtils.createTextFile(sub2, 100);

    String rootPrefix = TestUtils.addPrefix("copy-dir-bucket/");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // upload the directory
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(5, uploaded.size());

    // non-recursive copy
    String bucket2 = TestUtils.createTestBucket();
    List<S3File> objs2 = TestUtils.listObjects(bucket2, rootPrefix);
    int originalCount2 = objs2.size();
    String topN = rootPrefix + top.getName() + "/";
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(topN)
       .setDestinationBucketName(bucket2)
       .setDestinationKey(topN)
       .setRecursive(false)
       .createCopyOptions();
    List<S3File> copy = _client.copyToDir(copyOpts).get();
    Assert.assertEquals(2, copy.size());

    // verify the non-recursive copy
    objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 5, objs.size());
       // original files plus 5 uploaded

    List<S3File> copyObjs = TestUtils.listObjects(bucket2, rootPrefix);
    Assert.assertEquals(originalCount2 + 2, copyObjs.size());
       // original files plus 2 copied
    Assert.assertTrue(TestUtils.findObject(copyObjs, topN + a.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, topN + b.getName()));

    // recursive copy
    String copyTopN = rootPrefix + top.getName() + "-COPY/";
    copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(topN)
       .setDestinationBucketName(bucket2)
       .setDestinationKey(copyTopN)
       .setRecursive(true)
       .createCopyOptions();
    copy = _client.copyToDir(copyOpts).get();
    Assert.assertEquals(5, copy.size());

    // verify the recursive copy
    String subN = copyTopN + sub.getName() + "/";
    String sub2N = subN + sub2.getName() + "/";
    objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 5, objs.size());
       // same original plus 5 uploaded
    int lastSize = copyObjs.size();
    copyObjs = TestUtils.listObjects(bucket2, rootPrefix);
    Assert.assertEquals(lastSize + 5, copyObjs.size());
       // previous size plus 5 copies
    Assert.assertTrue(TestUtils.findObject(copyObjs, copyTopN + a.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, copyTopN + b.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, subN + c.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, subN + d.getName()));
    Assert.assertTrue(TestUtils.findObject(copyObjs, sub2N + e.getName()));
    return;
}
catch(Throwable t)
{
  ++count;
  if(count >= retryCount)
    throw t;
//  System.out.println(" ++++++++++++++++ RETRYING: " + t.getMessage());
}
}
  }


  @Test
  public void testEncryptedUploadDownload()
    throws Throwable
  {
    // generate a new public/private key pair
    String keyName = "cloud-store-ut";
    File keydir = TestUtils.createTmpDir(true);
    File keydir2 = TestUtils.createTmpDir(true);
    String[] keys = TestUtils.createEncryptionKey(keydir, keyName);
    String privateKey = keys[0];
    String publicKey = keys[1];

    // capture files currently in test bucket
    String rootPrefix = TestUtils.addPrefix("");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // create a small file
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

    // upload should fail if key can't be found
    TestUtils.setKeyProvider(keydir2);
    try
    {
      TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
      Assert.fail("Expected upload error (key not found)");
    }
    catch(Throwable t)
    {
      // expected
    }

    // upload should succeed now
    TestUtils.setKeyProvider(keydir);
    S3File f = TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = rootPrefix + toUpload.getName();
    Assert.assertTrue(TestUtils.findObject(objs, key));

    // download the file and compare it with the original
    File dlTemp = TestUtils.createTmpFile();
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(dlTemp.exists());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
    dlTemp.delete();

    // should fail to download if we can't find the key
    TestUtils.setKeyProvider(keydir2);
    try
    {
      f = TestUtils.downloadFile(dest, dlTemp);
      Assert.fail("Expected download error (key not found)");
    }
    catch(Throwable t)
    {
      // expected
    }
    Assert.assertFalse(dlTemp.exists());

    // try download with only private key in keydir.  should succeed
    String keyFileName = keyName + ".pem";
    TestUtils.writeToFile(privateKey, new File(keydir2, keyFileName));
    dlTemp = TestUtils.createTmpFile();
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertTrue(dlTemp.exists());
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));

    // upload should fail if only have private key
    try
    {
      TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
      Assert.fail("Expected upload error (key not found)");
    }
    catch(Throwable t)
    {
      // expected
    }

    // upload with only public key in key dir.  should succeed
    TestUtils.writeToFile(publicKey, new File(keydir2, keyFileName));
    f = TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
    Assert.assertNotNull(f);
  }

  
  private synchronized int getPartCount()
  {
    return _partSet.size();
  }


  private synchronized void clearParts()
  {
    _partSet.clear();
  }


  private synchronized void clearRetryCount()
  {
    _retryCount = 0;
  }

  private synchronized int getRetryCount()
  {
    return _retryCount;
  }

}
