package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class CopyTests
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
  public void testRetryDir()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int testLoop = TestUtils.RETRY_COUNT;
int count = 0;
while(count < testLoop)
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

    String rootPrefix = TestUtils.addPrefix("copy-dir-retry/");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // upload the directory
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    int uploadedSize = uploaded.size();
    Assert.assertEquals(5, uploadedSize);
    int currentSize = TestUtils.listObjects(_testBucket, rootPrefix).size();
    Assert.assertEquals(originalCount + uploadedSize, currentSize);

    // non-recursive copy
    String topN = rootPrefix + top.getName() + "/";
    String copyTopN = rootPrefix + top.getName() + "-COPY/";
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(topN)
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(copyTopN)
       .setRecursive(true)
       .createCopyOptions();
    boolean oldGlobalFlag = false;
    try
    {
      // set retry and abort options
      oldGlobalFlag = CopyOptions.useGlobalAbortCounter(true);
      ThrowableRetriableTask.addRetryListener(this);
      clearRetryCount();
      int retryCount = 10;
      int abortCount = 3;
      _client.setRetryCount(retryCount);
      CopyOptions.setAbortInjectionCounter(abortCount);

      List<S3File> copy = _client.copyToDir(copyOpts).get();
      Assert.assertEquals(5, copy.size());
      Assert.assertEquals(abortCount, getRetryCount());
    }
    finally
    {
      // reset retry and abort injection state so we don't affect other tests
      TestUtils.resetRetryCount();
      CopyOptions.setAbortInjectionCounter(0);
      CopyOptions.useGlobalAbortCounter(oldGlobalFlag);
    }

    // verify the recursive copy
    String subN = copyTopN + sub.getName() + "/";
    String sub2N = subN + sub2.getName() + "/";
    List<S3File> copyObjs = TestUtils.listObjects(_testBucket, rootPrefix);
    int lastSize = copyObjs.size();
    Assert.assertEquals(currentSize + 5, lastSize);
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
  if(count >= testLoop)
    throw t;
}
}
  }


  @Test
  public void testRetry()
    throws Throwable
  {
    try
    {
      // create test file and upload it
      String rootPrefix = TestUtils.addPrefix("copy-retry");
      int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
      File toUpload = TestUtils.createTextFile(100);
      URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
      S3File f = TestUtils.uploadFile(toUpload, dest);
      Assert.assertNotNull(f);
      List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
      Assert.assertEquals(originalCount + 1, objs.size());
      Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));

      // set retry and abort options
      ThrowableRetriableTask.addRetryListener(this);
      clearRetryCount();
      int retryCount = 10;
      int abortCount = 3;
      _client.setRetryCount(retryCount);
      CopyOptions.setAbortInjectionCounter(abortCount);
      
      // copy file
      CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f.getKey())
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(f.getKey() + "-COPY")
       .createCopyOptions();
      S3File copy = _client.copy(copyOpts).get();
      Assert.assertEquals(abortCount, getRetryCount());

      // check for the copy
      String expectedKey = f.getKey() + "-COPY";
      Assert.assertEquals(expectedKey, copy.getKey());
      objs = TestUtils.listObjects(_testBucket, rootPrefix);
      Assert.assertEquals(originalCount + 2, objs.size());
      Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));
      Assert.assertTrue(TestUtils.findObject(objs, expectedKey));
    }
    finally
    {
      // reset retry and abort injection state so we don't affect other tests
      TestUtils.resetRetryCount();
      CopyOptions.setAbortInjectionCounter(0);
    }

  }


  
  @Test
  public void testDryRunFile()
    throws Throwable
  {
    // create test file and upload it
    String rootPrefix = TestUtils.addPrefix("copy-dryrun");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);
    Assert.assertEquals(
      originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // dryrun the copy and make sure dest stays the same
    CopyOptions opts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f.getKey())
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(f.getKey() + "-COPY")
       .setDryRun(true)
       .createCopyOptions();
    S3File copy = _client.copy(opts).get();
    Assert.assertNull(copy);
    Assert.assertEquals(
      originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());
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
    String rootPrefix = TestUtils.addPrefix("copy-dryrun-dir/");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(2, uploaded.size());
    Assert.assertEquals(
      originalCount + 2, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // dryrun the copy and make sure the dest doesn't change
    String topN = rootPrefix + top.getName() + "/";
    String copyTopN = rootPrefix + top.getName() + "-COPY/";
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(topN)
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(copyTopN)
       .setRecursive(false)
       .setDryRun(true)
       .createCopyOptions();
    List<S3File> copy = _client.copyToDir(copyOpts).get();
    Assert.assertNull(copy);
    Assert.assertEquals(
      originalCount + 2, TestUtils.listObjects(_testBucket, rootPrefix).size());
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

  
  @Test
  public void testSimpleCopy()
    throws Throwable
  {
    List<S3File> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // create test file and upload it
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("simple-copy");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));

    // copy file
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f.getKey())
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(f.getKey() + "-COPY")
       .createCopyOptions();
    S3File copy = _client.copy(copyOpts).get();
    String expectedKey = TestUtils.addPrefix("simple-copy/" + toUpload.getName() + "-COPY");
    Assert.assertEquals(expectedKey, copy.getKey());

    // check for the copy
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 2, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));
    Assert.assertTrue(TestUtils.findObject(objs, expectedKey));

    // download and compare copy
    File dlTemp = TestUtils.createTmpFile();
    dest = new URI(dest.toString() + "-COPY");
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
  }


  @Test
  public void testOverwriteExistingFile()
    throws Throwable
  {
    // test that copy overwrites without error
    
    // create a pair of files and upload
    int fileSize = 100;
    File file1 = TestUtils.createTextFile(fileSize);
    File file2 = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("copy-overwrite-file");
    URI dest1 = TestUtils.getUri(_testBucket, file1, rootPrefix);
    S3File f1 = TestUtils.uploadFile(file1, dest1);
    Assert.assertNotNull(f1);
    URI dest2 = TestUtils.getUri(_testBucket, file2, rootPrefix);
    S3File f2 = TestUtils.uploadFile(file2, dest2);
    Assert.assertNotNull(f2);

    // make sure files were uploaded
    List<S3File> objs = TestUtils.listTestBucketObjects();
    Assert.assertTrue(
      TestUtils.findObject(objs, Utils.getObjectKey(dest1)));
    Assert.assertTrue(
      TestUtils.findObject(objs, Utils.getObjectKey(dest2)));

    // copy file1 over file2
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f1.getKey())
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(f2.getKey())
       .createCopyOptions();
    S3File copy = _client.copy(copyOpts).get();
    String expectedKey = TestUtils.addPrefix("copy-overwrite-file/" + file2.getName());
    Assert.assertEquals(expectedKey, copy.getKey());

    // same files should still exist
    objs = TestUtils.listTestBucketObjects();
    Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest1)));
    Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest2)));

    // download and compare copy - should match file1
    File dlTemp = TestUtils.createTmpFile();
    S3File f = TestUtils.downloadFile(dest2, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(TestUtils.compareFiles(file1, f.getLocalFile()));
  }


  @Test
  public void testOverwriteExistingDir()
    throws Throwable
  {
    // copying a file to same name as existing dir should result in both
    // file and directory with same name

    // this test can't work with minio on a local file system -- can't have a file and
    // directory with same name.  assuming if --dest-prefix passed to test runner, we're not
    // using minio and can run the test.
    if(null == TestUtils.getDestUri())
      return;

// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = TestUtils.RETRY_COUNT;
int count = 0;
while(count < retryCount)
{
try
{
    int originalCount = TestUtils.listTestBucketObjects().size();
    
    // create simple directory structure with a few files
    File top = TestUtils.createTmpDir(true);
    File a = TestUtils.createTextFile(top, 100);
    File b = TestUtils.createTextFile(top, 100);
    File sub = TestUtils.createTmpDir(top);
    File c = TestUtils.createTextFile(sub, 100);
    File d = TestUtils.createTextFile(sub, 100);
    File sub2 = TestUtils.createTmpDir(sub);
    File e = TestUtils.createTextFile(sub2, 100);

    // upload the directory
    String rootPrefix = TestUtils.addPrefix("copy-overwrite-dir-" + count + "/");
    URI topUri = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, topUri);
    Assert.assertEquals(5, uploaded.size());

    // create test file and upload it
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    URI fileUri = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, fileUri);
    Assert.assertNotNull(f);

    // validate files were uploaded
    Assert.assertEquals(6 + originalCount, TestUtils.listTestBucketObjects().size());

    // copy file
    URI dest = TestUtils.getUri(_testBucket, top.getName(), rootPrefix);
    String expectedKey = Utils.getObjectKey(dest);
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f.getKey())
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(expectedKey)
       .createCopyOptions();
    S3File copy = _client.copy(copyOpts).get();
    Assert.assertEquals(expectedKey, copy.getKey());

    // validate results, dest file should exist and original dir should be the same
    Assert.assertEquals(7 + originalCount, TestUtils.listTestBucketObjects().size());
    URI file = TestUtils.getUri(_testBucket, top.getName(), rootPrefix);
    Assert.assertNotNull(_client.exists(file).get());
    List<S3File> objs = TestUtils.listObjects(_testBucket, Utils.getObjectKey(topUri));
    Assert.assertEquals(5, objs.size());
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


  @Test
  public void testCopyIntoExistingDir()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = TestUtils.RETRY_COUNT;
int count = 0;
while(count < retryCount)
{
try
{
    int originalCount = TestUtils.listTestBucketObjects().size();
    
    // create simple directory structure with a few files
    File top = TestUtils.createTmpDir(true);
    File a = TestUtils.createTextFile(top, 100);
    File b = TestUtils.createTextFile(top, 100);
    File sub = TestUtils.createTmpDir(top);
    File c = TestUtils.createTextFile(sub, 100);
    File d = TestUtils.createTextFile(sub, 100);
    File sub2 = TestUtils.createTmpDir(sub);
    File e = TestUtils.createTextFile(sub2, 100);

    // upload the directory
    String rootPrefix = TestUtils.addPrefix("copy-overwrite-dir-" + count + "/");
    URI topUri = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<S3File> uploaded = TestUtils.uploadDir(top, topUri);
    Assert.assertEquals(5, uploaded.size());

    // create test file and upload it
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    URI fileUri = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, fileUri);
    Assert.assertNotNull(f);

    // validate files were uploaded
    Assert.assertEquals(6 + originalCount, TestUtils.listTestBucketObjects().size());

    // copy file
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    String expectedKey = Utils.getObjectKey(dest);
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f.getKey())
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(expectedKey)
       .createCopyOptions();
    List<S3File> copy = _client.copyToDir(copyOpts).get();
    Assert.assertEquals(1, copy.size());

    // validate results, original dir have one more file in it
    Assert.assertEquals(7 + originalCount, TestUtils.listTestBucketObjects().size());
    List<S3File> objs = TestUtils.listObjects(_testBucket, Utils.getObjectKey(topUri));
    Assert.assertEquals(6, objs.size());
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
    String rootPrefix = TestUtils.addPrefix("cross-bucket-copy");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));

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
    Assert.assertTrue(TestUtils.findObject(copyObjs, f.getKey()));

    // download and compare copy
    File dlTemp = TestUtils.createTmpFile();
    URI src = new URI(TestUtils.getService() + "://" + bucket2 + "/" + f.getKey());
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
}
}
  }


  @Test
  public void testCopyMissingDestBucket()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = TestUtils.RETRY_COUNT;
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
    String msg = null;
    try
    {
      _client.copyToDir(copyOpts).get();
      msg = "Exception expected";
    }
    catch(ExecutionException ex)
    {
      Assert.assertTrue(ex.getMessage().contains("specified bucket is not valid"));
    }
    Assert.assertNull(msg);
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


  @Test
  public void testCrossBucketCopyDir()
    throws Throwable
  {
// directory copy/upload tests intermittently fail when using minio.  trying to minimize false failure reports by repeating and only failing the test if it consistently reports an error.
int retryCount = TestUtils.RETRY_COUNT;
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
}

