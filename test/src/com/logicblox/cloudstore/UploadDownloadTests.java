/*
  Copyright 2018, Infor Inc.

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

import junit.framework.Assert;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.crypto.Cipher;
import java.io.File;
import java.net.URI;
import java.security.Key;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;


public class UploadDownloadTests
  implements OverallProgressListenerFactory, OverallProgressListener, RetryListener
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
  public void testUploadDryRun()
    throws Throwable
  {
    // create a file
    String rootPrefix = TestUtils.addPrefix("upload-dryrun");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

    // dryrun the upload and make sure the dest doesn't change
    UploadOptions opts = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(toUpload)
      .setBucketName(Utils.getBucketName(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setDryRun(true)
      .createOptions();
    StoreFile f = _client.upload(opts).get();
    Assert.assertNull(f);
    Assert.assertEquals(originalCount, TestUtils.listObjects(_testBucket, rootPrefix).size());
  }


  @Test
  public void testUploadDirectoryDryRun()
    throws Throwable
  {
    // directory copy/upload tests intermittently fail when using minio.  trying to minimize
    // false failure reports by repeating and only failing the test if it consistently reports an
    // error.
    int retryCount = TestUtils.RETRY_COUNT;
    int count = 0;
    while(count < retryCount)
    {
      try
      {
        // create simple directory structure
        File top = TestUtils.createTmpDir(true);
        File a = TestUtils.createTextFile(top, 100);
        File b = TestUtils.createTextFile(top, 100);
        String rootPrefix = TestUtils.addPrefix("upload-dir-dryrun");
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);

        // dryrun the upload and verify the dest didn't change
        UploadOptions opts = _client.getOptionsBuilderFactory()
          .newUploadOptionsBuilder()
          .setFile(top)
          .setBucketName(Utils.getBucketName(dest))
          .setObjectKey(Utils.getObjectKey(dest))
          .setDryRun(true)
          .createOptions();
        List<StoreFile> files = _client.uploadRecursively(opts).get();
        Assert.assertNull(files);
        Assert.assertEquals(originalCount, TestUtils.listObjects(_testBucket, rootPrefix).size());
        return;
      }
      catch(Throwable t)
      {
        ++count;
        if(count >= retryCount)
        {
          throw t;
        }
      }
    }
  }


  @Test
  public void testDownloadDryRun()
    throws Throwable
  {
    // create a small file and upload it
    String rootPrefix = TestUtils.addPrefix("download-dryrun");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);
    Assert.assertEquals(originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // dryrun the download and make sure the file isn't created locally
    URI src = dest;
    File dlTemp = TestUtils.createTmpFile();
    dlTemp.delete();
    DownloadOptions opts = _client.getOptionsBuilderFactory()
      .newDownloadOptionsBuilder()
      .setFile(dlTemp)
      .setBucketName(Utils.getBucketName(src))
      .setObjectKey(Utils.getObjectKey(src))
      .setOverwrite(true)
      .setDryRun(true)
      .createOptions();
    f = _client.download(opts).get();
    Assert.assertNull(f);
    Assert.assertFalse(dlTemp.exists());
  }


  @Test
  public void testDownloadDirectoryDryRun()
    throws Throwable
  {
    // create simple directory structure and upload
    String rootPrefix = TestUtils.addPrefix("download-dir-dryrun");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    File top = TestUtils.createTmpDir(true);
    File a = TestUtils.createTextFile(top, 100);
    File b = TestUtils.createTextFile(top, 100);
    URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
    List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
    Assert.assertEquals(2, uploaded.size());
    Assert.assertEquals(originalCount + uploaded.size(),
      TestUtils.listObjects(_testBucket, rootPrefix).size());

    // dryrun the download and make sure nothing changes locally
    File dlDir = TestUtils.createTmpDir(true);
    URI src = dest;
    DownloadOptions opts = _client.getOptionsBuilderFactory()
      .newDownloadOptionsBuilder()
      .setFile(dlDir)
      .setBucketName(Utils.getBucketName(src))
      .setObjectKey(Utils.getObjectKey(src))
      .setOverwrite(true)
      .setDryRun(true)
      .createOptions();
    List<StoreFile> files = _client.downloadRecursively(opts).get();
    Assert.assertNull(files);
    Assert.assertEquals(0, dlDir.list().length);
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
    String rootPrefix = TestUtils.addPrefix("exists/a/b/");
    List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);
    Assert.assertNotNull(TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest)));

    // check for missing file key
    URI src = TestUtils.getUri(_testBucket, toUpload,
      rootPrefix + "-missing-" + System.currentTimeMillis());
    Assert.assertNull(TestUtils.objectExists(Utils.getBucketName(src), Utils.getObjectKey(src)));

    // bad bucket should fail
    src = TestUtils.getUri(_testBucket + "-missing-" + System.currentTimeMillis(), toUpload,
      rootPrefix);
    Assert.assertNull(TestUtils.objectExists(Utils.getBucketName(src), Utils.getObjectKey(src)));
  }


  @Test
  public void testSimpleUploadDownload()
    throws Throwable
  {
    String rootPrefix = TestUtils.addPrefix("simple-upload/a/b/");
    List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // create a small file and upload it
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
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
    Assert.assertNotNull(TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest)));
    Assert.assertNotNull(TestUtils.objectExists(_testBucket, key));
    Assert.assertEquals(dest.toString(),
      Utils.getURI(_client.getScheme(), _testBucket, key).toString());

    Assert.assertNotNull(TestUtils.deleteObject(dest));
    Assert.assertNull(TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest)));
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
      UploadOptions.getAbortCounters().setInjectionCounter(10);
      List<StoreFile> objs = TestUtils.listTestBucketObjects();
      int originalCount = objs.size();

      // create test file - AWS requires a min 5M chunk size...
      int chunkSize = 5 * 1024 * 1024;
      int fileSize = chunkSize + 1000000;
      File toUpload = TestUtils.createTextFile(fileSize);
      String rootPrefix = TestUtils.addPrefix("failed-upload-retry");
      URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

      // upload file in multiple concurrent chunks
      clearParts();
      UploadOptions upOpts = _client.getOptionsBuilderFactory()
        .newUploadOptionsBuilder()
        .setFile(toUpload)
        .setBucketName(Utils.getBucketName(dest))
        .setObjectKey(Utils.getObjectKey(dest))
        .setChunkSize(chunkSize)
        .createOptions();
      try
      {
        _client.upload(upOpts).get();
        Assert.fail("expected exception");
      }
      catch(ExecutionException ex)
      {
        Assert.assertTrue(TestUtils.findCause(ex, AbortInjection.class));
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
      UploadOptions.getAbortCounters().setInjectionCounter(0);
      UploadOptions.getAbortCounters().clearInjectionCounters();
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
      UploadOptions.getAbortCounters().setInjectionCounter(abortCount);
      List<StoreFile> objs = TestUtils.listTestBucketObjects();
      int originalCount = objs.size();

      // create test file - AWS requires a min 5M chunk size...
      int chunkSize = 5 * 1024 * 1024;
      int fileSize = chunkSize + 1000000;
      File toUpload = TestUtils.createTextFile(fileSize);
      String rootPrefix = TestUtils.addPrefix("ok-upload-retry");
      URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

      // upload file in multiple concurrent chunks
      UploadOptions upOpts = _client.getOptionsBuilderFactory()
        .newUploadOptionsBuilder()
        .setFile(toUpload)
        .setBucketName(Utils.getBucketName(dest))
        .setObjectKey(Utils.getObjectKey(dest))
        .setChunkSize(chunkSize)
        .createOptions();
      StoreFile f = _client.upload(upOpts).get();
      Assert.assertNotNull(f);
      Assert.assertEquals(abortCount, getRetryCount());

      // validate the upload
      objs = TestUtils.listTestBucketObjects();
      Assert.assertEquals(originalCount + 1, objs.size());
      Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));

      // download the file and compare it with the original
      File dlTemp = TestUtils.createTmpFile();
      DownloadOptions dlOpts = _client.getOptionsBuilderFactory()
        .newDownloadOptionsBuilder()
        .setFile(dlTemp)
        .setBucketName(Utils.getBucketName(dest))
        .setObjectKey(Utils.getObjectKey(dest))
        .setOverwrite(true)
        .createOptions();
      f = _client.download(dlOpts).get();
      Assert.assertNotNull(f.getLocalFile());
      Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
    }
    finally
    {
      // reset retry and abort injection state so we don't affect other tests
      TestUtils.resetRetryCount();
      UploadOptions.getAbortCounters().setInjectionCounter(0);
      UploadOptions.getAbortCounters().clearInjectionCounters();
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
    String rootPrefix = TestUtils.addPrefix("upload-attrs");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);
    Assert.assertEquals(toUpload.getAbsolutePath(), f.getLocalFile().getAbsolutePath());
    Assert.assertNotNull(f.getETag());
    Assert.assertEquals(Utils.getObjectKey(dest), f.getObjectKey());
    Assert.assertEquals(_testBucket, f.getBucketName());
    //    Assert.assertTrue(f.getVersionId().isPresent());
    // FIXME - this info is not being populated right now
    //    Assert.assertTrue(f.getSize().isPresent());
    // FIXME - this info is not being populated right now
    //    Assert.assertTrue(f.getTimestamp().isPresent());
    // FIXME - this info is not being populated right now

    List<StoreFile> objs = TestUtils.listTestBucketObjects();
    for(StoreFile o : objs)
    {
      if(o.getObjectKey().equals(TestUtils.addPrefix(toUpload.getName())))
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

    Metadata meta = TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest));
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
    String rootPrefix = TestUtils.addPrefix("download-attrs");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    TestUtils.uploadFile(toUpload, dest);

    // now download it
    File dlTemp = TestUtils.createTmpFile();
    StoreFile f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f);
    Assert.assertEquals(dlTemp.getAbsolutePath(), f.getLocalFile().getAbsolutePath());
    Assert.assertNotNull(f.getETag());
    Assert.assertEquals(Utils.getObjectKey(dest), f.getObjectKey());
    Assert.assertEquals(_testBucket, f.getBucketName());
    //    Assert.assertTrue(f.getVersionId().isPresent());
    // FIXME - this info is not being populated right now
    //    Assert.assertTrue(f.getSize().isPresent());
    // FIXME - this info is not being populated right now
    //    Assert.assertTrue(f.getTimestamp().isPresent());
    // FIXME - this info is not being populated right now
  }


  @Test
  public void testDefaultS3Acl()
    throws Throwable
  {
    // this test makes sense only for AWS S3. Minio doesn't support ACLs and current ACL
    // support on GCS is limited
    Assume.assumeTrue(TestUtils.getService().equalsIgnoreCase("s3") &&
      TestUtils.supportsAcl());

    // Create a small file and upload it
    long fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("test-download-default-acl");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    TestUtils.uploadFile(toUpload, dest);

    AclHandler aclHandler = _client.getAclHandler();
    Acl objectAcl = aclHandler.getObjectAcl(Utils.getBucketName(dest), Utils.getObjectKey(dest))
      .get();
    Owner bucketOwner = _client.getBucket(Utils.getBucketName((dest))).get().getOwner();

    // Test default ACL - bucket-owner-full-control
    Assert.assertEquals(objectAcl.getGrants().size(), 1);
    AclPermission perm = objectAcl.getGrants().get(0).getPermission();
    Assert.assertEquals(perm.toString(), "FULL_CONTROL");

    AclGrantee grantee = objectAcl.getGrants().get(0).getGrantee();
    Assert.assertEquals(grantee.getId(), bucketOwner.getId());
  }

  @Test
  public void testNonDefaultS3Acl()
    throws Throwable
  {
    // this test makes sense only for AWS S3. Minio doesn't support ACLs and current ACL
    // support on GCS is limited
    Assume.assumeTrue(TestUtils.getService().equalsIgnoreCase("s3") &&
      TestUtils.supportsAcl());

    // Create a small file and upload it
    long fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("test-download-non-default-acl");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

    UploadOptions upOpts = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(toUpload)
      .setBucketName(Utils.getBucketName(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setCannedAcl("authenticated-read")
      .createOptions();
    StoreFile f = _client.upload(upOpts).get();

    AclHandler aclHandler = _client.getAclHandler();
    Acl objectAcl = aclHandler.getObjectAcl(Utils.getBucketName(dest), Utils.getObjectKey(dest))
      .get();
    Owner bucketOwner = _client.getBucket(Utils.getBucketName((dest))).get().getOwner();

    // authenticated-read ACL: Owner gets FULL_CONTROL. The AuthenticatedUsers group gets READ
    // access.
    Assert.assertEquals(objectAcl.getGrants().size(), 2);

    AclGrant fcAclGrant;
    AclGrant readAclGrant;
    if(objectAcl.getGrants().get(0).getPermission().toString().equals("FULL_CONTROL"))
    {
      fcAclGrant = objectAcl.getGrants().get(0);
      readAclGrant = objectAcl.getGrants().get(1);
    }
    else
    {
      fcAclGrant = objectAcl.getGrants().get(1);
      readAclGrant = objectAcl.getGrants().get(0);
    }

    AclPermission perm = fcAclGrant.getPermission();
    Assert.assertEquals(perm.toString(), "FULL_CONTROL");
    AclGrantee grantee = fcAclGrant.getGrantee();
    Assert.assertEquals(grantee.getId(), bucketOwner.getId());

    perm = readAclGrant.getPermission();
    Assert.assertEquals(perm.toString(), "READ");
    grantee = readAclGrant.getGrantee();
    Assert.assertEquals(grantee.getId(),
      "http://acs.amazonaws.com/groups/global/AuthenticatedUsers");
  }

  @Test
  public void testEmptyFile()
    throws Throwable
  {
    List<StoreFile> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // upload a file
    File toUpload = TestUtils.createTextFile(0);
    Assert.assertEquals(0, toUpload.length());
    String rootPrefix = TestUtils.addPrefix("upload-empty");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = Utils.getObjectKey(dest);
    Assert.assertTrue(TestUtils.findObject(objs, key));

    // download, overwriting a larger file
    File dlTemp = TestUtils.createTextFile(100);
    Assert.assertEquals(100, dlTemp.length());
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertEquals(0, dlTemp.length());
  }


  @Test
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

    List<StoreFile> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // upload a file
    File toUpload = TestUtils.createTextFile(0);
    Assert.assertEquals(0, toUpload.length());
    String rootPrefix = TestUtils.addPrefix("upload-empty-encrypted");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = Utils.getObjectKey(dest);
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
    String rootPrefix = TestUtils.addPrefix("download-missing");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
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
    catch(Exception ex)
    {
      // expected
      checkUsageException(ex, "Object not found");
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
    catch(Exception ex)
    {
      // expected
      checkUsageException(ex, "Object not found");
    }
    Assert.assertNull(msg);
  }


  @Test
  public void testDownloadNoOverwriteFile()
    throws Throwable
  {
    List<StoreFile> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // upload a file
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("download-no-overwrite");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = Utils.getObjectKey(dest);
    Assert.assertTrue(TestUtils.findObject(objs, key));

    // download without overwrite to make sure it fails
    File dlTemp = TestUtils.createTextFile(10);
    Assert.assertFalse(TestUtils.compareFiles(toUpload, dlTemp));
    try
    {
      f = TestUtils.downloadFile(dest, dlTemp, false);
      Assert.fail("Expected download exception");
    }
    catch(Exception ex)
    {
      // expected
      checkUsageException(ex, null);
    }

    // now download with overwite to make sure it replaces the file
    f = TestUtils.downloadFile(dest, dlTemp, true);
    Assert.assertTrue(TestUtils.compareFiles(toUpload, dlTemp));
  }


  @Test
  public void testDirectoryOverwrite()
    throws Throwable
  {
    // directory copy/upload tests intermittently fail when using minio.  trying to minimize
    // false failure reports by repeating and only failing the test if it consistently reports an
    // error.
    int retryCount = TestUtils.RETRY_COUNT;
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
        List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
        Assert.assertEquals(2, uploaded.size());

        // it's ok if source doesn't exist
        File dlDir = TestUtils.createTmpDir(true);
        String badPrefix = TestUtils.addPrefix(
          "dir-overwrite-bad" + System.currentTimeMillis() + "/");
        URI src = TestUtils.getUri(_testBucket, top, badPrefix);
        List<StoreFile> storeFiles = TestUtils.downloadRecursively(src, dlDir);
        Assert.assertTrue(storeFiles.isEmpty());

        // should fail if dest is existing file with no overwrite
        String msg = null;
        File existingFile = TestUtils.createTextFile(top, 1);
        src = dest;
        try
        {
          TestUtils.downloadRecursively(src, existingFile, false);
          msg = "expected exception (can't overwrite file)";
        }
        catch(Exception ex)
        {
          // expected
          checkUsageException(ex, "must be a directory");
        }
        Assert.assertNull(msg);

        // should succeed if dest is existing file with overwrite
        List<StoreFile> downloaded = TestUtils.downloadRecursively(src, existingFile, true);
        Assert.assertEquals(2, downloaded.size());
        Assert.assertTrue(existingFile.isDirectory());

        // should fail if dest is existing directory and one file exists with no overwrite
        File destDir = TestUtils.createTmpDir(true);
        existingFile = new File(destDir, a.getName());
        Assert.assertTrue(existingFile.createNewFile());
        Assert.assertTrue(existingFile.exists());
        try
        {
          TestUtils.downloadRecursively(src, destDir, false);
          msg = "expected exception (can't overwrite file)";
        }
        catch(Exception ex)
        {
          // expected
          checkUsageException(ex, "already exists");
        }
        Assert.assertNull(msg);
        Assert.assertEquals(1, destDir.list().length);
        // should just have the original file.  everything else should be cleaned up

        // should succeed if dest is existing directory and one file exists with overwrite
        downloaded = TestUtils.downloadRecursively(src, destDir, true);
        Assert.assertEquals(2, downloaded.size());

        // should succeed if dest doesn't exist
        destDir = new File(destDir.getAbsolutePath() + "-missing-" + System.currentTimeMillis());
        Assert.assertFalse(destDir.exists());
        downloaded = TestUtils.downloadRecursively(src, destDir, true);
        Assert.assertEquals(2, downloaded.size());

        return;
      }
      catch(Throwable t)
      {
        ++count;
        if(count >= retryCount)
        {
          throw t;
        }
      }
    }
  }


  @Test
  public void testDirectoryUploadDownload()
    throws Throwable
  {
    // directory copy/upload tests intermittently fail when using minio.  trying to minimize
    // false failure reports by repeating and only failing the test if it consistently reports an
    // error.
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

        String rootPrefix = TestUtils.addPrefix("dir-ul-dl/");
        List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
        int originalCount = objs.size();

        // upload the directory
        URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
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

        // recursive prefix download for topN w/o trailing '/'
        File dlDir = TestUtils.createTmpDir(true);
        List<StoreFile> downloaded = TestUtils.downloadRecursively(dest, dlDir);
        Assert.assertEquals(5, downloaded.size());
        Assert.assertEquals(3, dlDir.list().length);
        Assert.assertTrue(TestUtils.compareFiles(a, new File(dlDir, a.getName())));
        Assert.assertTrue(TestUtils.compareFiles(b, new File(dlDir, b.getName())));

        File dlsub = new File(dlDir, sub.getName());
        Assert.assertTrue(dlsub.exists() && dlsub.isDirectory());
        Assert.assertEquals(3, dlsub.list().length);
        Assert.assertTrue(TestUtils.compareFiles(c, new File(dlsub, c.getName())));
        Assert.assertTrue(TestUtils.compareFiles(d, new File(dlsub, d.getName())));

        File dlsub2 = new File(dlsub, sub2.getName());
        Assert.assertTrue(dlsub2.exists() && dlsub2.isDirectory());
        Assert.assertEquals(1, dlsub2.list().length);
        Assert.assertTrue(TestUtils.compareFiles(e, new File(dlsub2, e.getName())));

        // recursive prefix download for topN w/o trailing '/'
        dest = Utils.getURI(TestUtils.getService(), _testBucket,
          topN.substring(0, topN.length() - 1));
        File dlDirPrefix = TestUtils.createTmpDir(true);
        downloaded = TestUtils.downloadRecursively(dest, dlDirPrefix);
        Assert.assertEquals(5, downloaded.size());
        Assert.assertEquals(1, dlDirPrefix.list().length);

        dlDir = new File(dlDirPrefix, top.getName());
        Assert.assertTrue(dlDir.exists() && dlDir.isDirectory());
        Assert.assertEquals(3, dlDir.list().length);
        Assert.assertTrue(TestUtils.compareFiles(a, new File(dlDir, a.getName())));
        Assert.assertTrue(TestUtils.compareFiles(b, new File(dlDir, b.getName())));

        dlsub = new File(dlDir, sub.getName());
        Assert.assertTrue(dlsub.exists() && dlsub.isDirectory());
        Assert.assertEquals(3, dlsub.list().length);
        Assert.assertTrue(TestUtils.compareFiles(c, new File(dlsub, c.getName())));
        Assert.assertTrue(TestUtils.compareFiles(d, new File(dlsub, d.getName())));

        dlsub2 = new File(dlsub, sub2.getName());
        Assert.assertTrue(dlsub2.exists() && dlsub2.isDirectory());
        Assert.assertEquals(1, dlsub2.list().length);
        Assert.assertTrue(TestUtils.compareFiles(e, new File(dlsub2, e.getName())));
        return;
      }
      catch(Throwable t)
      {
        ++count;
        if(count >= retryCount)
        {
          throw t;
        }
      }
    }
  }


  @Test
  public void testMultipartUploadDownload()
    throws Throwable
  {
    List<StoreFile> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // create test file - AWS requires a min 5M chunk size...
    int chunkSize = 5 * 1024 * 1024;
    int fileSize = chunkSize + 1000000;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("multipart-upload");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

    // upload file in multiple concurrent chunks
    clearParts();
    UploadOptions upOpts = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(toUpload)
      .setBucketName(Utils.getBucketName(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setOverallProgressListenerFactory(this)
      .setChunkSize(chunkSize)
      .createOptions();
    StoreFile f = _client.upload(upOpts).get();
    Assert.assertNotNull(f);

    // validate the upload
    int partCount = getPartCount();
    int expectedCount = TestUtils.getExpectedPartCount(fileSize, chunkSize);
    Assert.assertEquals(expectedCount, partCount);
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));

    // download the file and compare it with the original
    File dlTemp = TestUtils.createTmpFile();
    DownloadOptions dlOpts = _client.getOptionsBuilderFactory()
      .newDownloadOptionsBuilder()
      .setFile(dlTemp)
      .setBucketName(Utils.getBucketName(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setOverwrite(true)
      .setOverallProgressListenerFactory(this)
      .createOptions();
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

    List<StoreFile> objs = TestUtils.listTestBucketObjects();
    int originalCount = objs.size();

    // create test file - AWS requires a min 5M chunk size...
    int chunkSize = 5 * 1024 * 1024;
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("small-multipart-upload");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

    // upload file in multiple concurrent chunks
    clearParts();
    UploadOptions upOpts = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(toUpload)
      .setBucketName(Utils.getBucketName(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setOverallProgressListenerFactory(this)
      .setChunkSize(chunkSize)
      .createOptions();
    StoreFile f = _client.upload(upOpts).get();
    Assert.assertNotNull(f);

    // validate the upload
    int partCount = getPartCount();
    int expectedCount = TestUtils.getExpectedPartCount(fileSize, chunkSize);
    Assert.assertEquals(expectedCount, partCount);
    objs = TestUtils.listTestBucketObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));

    // download the file and compare it with the original
    File dlTemp = TestUtils.createTmpFile();
    DownloadOptions dlOpts = _client.getOptionsBuilderFactory()
      .newDownloadOptionsBuilder()
      .setFile(dlTemp)
      .setBucketName(Utils.getBucketName(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setOverwrite(true)
      .setOverallProgressListenerFactory(this)
      .createOptions();
    clearParts();
    f = _client.download(dlOpts).get();
    partCount = getPartCount();
    Assert.assertEquals(expectedCount, partCount);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
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
    String rootPrefix = TestUtils.addPrefix("encrypted-upload");
    List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
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
    catch(Exception ex)
    {
      // expected
      checkUsageException(ex, null);
    }

    // upload should succeed now
    TestUtils.setKeyProvider(keydir);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = Utils.getObjectKey(dest);
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
    catch(ExecutionException ex)
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
    catch(Exception ex)
    {
      // expected
      checkUsageException(ex, null);
    }

    // upload with only public key in key dir.  should succeed
    TestUtils.writeToFile(publicKey, new File(keydir2, keyFileName));
    f = TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
    Assert.assertNotNull(f);
  }

  @Test
  public void testMissingPubkeyHash()
    throws Throwable
  {
    // generate new public/private key pair
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    TestUtils.createEncryptionKey(keydir, key1);
    TestUtils.setKeyProvider(keydir);
    String rootPrefix = TestUtils.addPrefix("test-missing-pubkey-hash");

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // remove "s3tool-pubkey-hash" from metadata to simulate files
    // uploaded by older cloud-store versions
    String objKey = rootPrefix + '/' + toUpload.getName();
    Metadata destMeta = TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest));
    Assert.assertNotNull(destMeta);
    Map<String, String> destUserMeta = destMeta.getUserMetadata();
    Assert.assertNotNull(destUserMeta);
    Assert.assertTrue(destUserMeta.containsKey("s3tool-pubkey-hash"));
    destUserMeta.remove("s3tool-pubkey-hash");
    TestUtils.updateObjectUserMetadata(_testBucket, objKey, destUserMeta);

    // make sure "s3tool-pubkey-hash" has been removed
    destMeta = TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest));
    Assert.assertNotNull(destMeta);
    destUserMeta = destMeta.getUserMetadata();
    Assert.assertNotNull(destUserMeta);
    Assert.assertFalse(destUserMeta.containsKey("s3tool-pubkey-hash"));

    // download the file and compare it with the original
    File dlTemp = TestUtils.createTmpFile();
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(dlTemp.exists());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
    dlTemp.delete();
  }

  @Test
  public void testUserMetadata()
    throws Throwable
  {
    // create a small file and upload it
    int fileSize = 100;
    String rootPrefix = TestUtils.addPrefix("test-user-metadata");
    File toUpload = TestUtils.createTextFile(fileSize);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // verify metadata
    Metadata destMeta = TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest));
    Assert.assertNotNull(destMeta);
    Map<String, String> destUserMeta = destMeta.getUserMetadata();
    Assert.assertNotNull(destUserMeta);
    Assert.assertEquals(destUserMeta.size(), 3);
    Assert.assertEquals(Long.parseLong(destUserMeta.get("s3tool-chunk-size")),
      Utils.getDefaultChunkSize(fileSize));
    Assert.assertEquals(Integer.parseInt(destUserMeta.get("s3tool-file-length")), fileSize);
    Assert.assertEquals(Integer.parseInt(destUserMeta.get("s3tool-version")), Version.CURRENT);
  }

  @Test
  public void testCustomChunkSize()
    throws Throwable
  {
    // Create a small file and upload it w/ default ACL
    long fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("test-custom-chunk-size");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);

    long chunkSize = 1024 * 1024;
    UploadOptions upOpts = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(toUpload)
      .setBucketName(Utils.getBucketName(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setChunkSize(chunkSize)
      .createOptions();
    StoreFile f = _client.upload(upOpts).get();
    Assert.assertNotNull(f);

    // verify metadata
    Metadata destMeta = TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest));
    Assert.assertNotNull(destMeta);
    Map<String, String> destUserMeta = destMeta.getUserMetadata();
    Assert.assertNotNull(destUserMeta);
    Assert.assertEquals(destUserMeta.size(), 3);
    Assert.assertEquals(Long.parseLong(destUserMeta.get("s3tool-chunk-size")), chunkSize);
    Assert.assertEquals(Integer.parseInt(destUserMeta.get("s3tool-file-length")), fileSize);
    Assert.assertEquals(Integer.parseInt(destUserMeta.get("s3tool-version")), Version.CURRENT);
  }

  @Test
  public void testUserMetadataEncrypted()
    throws Throwable
  {
    // generate new public/private key pair
    File keyDir = TestUtils.createTmpDir(true);
    TestUtils.setKeyProvider(keyDir);
    String keyName = "cloud-store-ut-1";
    TestUtils.createEncryptionKey(keyDir, keyName);
    String rootPrefix = TestUtils.addPrefix("test-user-metadata-encrypted");

    // create a small file and upload
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
    Assert.assertNotNull(f);
    long chunkSize = Utils.getDefaultChunkSize(fileSize);

    // verify metadata
    Metadata destMeta = TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest));
    Assert.assertNotNull(destMeta);
    Cipher cipherAES = Cipher.getInstance("AES/CBC/PKCS5Padding");
    long blockSize = cipherAES.getBlockSize();
    long partSize = blockSize * (fileSize / blockSize + 2);
    Assert.assertEquals(destMeta.getContentLength(), partSize);

    // verify user metadata
    Map<String, String> destUserMeta = destMeta.getUserMetadata();
    Assert.assertNotNull(destUserMeta);
    Assert.assertEquals(destUserMeta.size(), 6);
    Assert.assertEquals(Long.parseLong(destUserMeta.get("s3tool-chunk-size")), chunkSize);
    Assert.assertEquals(Integer.parseInt(destUserMeta.get("s3tool-file-length")), fileSize);
    Assert.assertEquals(Integer.parseInt(destUserMeta.get("s3tool-version")), Version.CURRENT);
    Assert.assertEquals(destUserMeta.get("s3tool-key-name"), keyName);

    final Base64.Decoder base64Decoder = Base64.getMimeDecoder();
    final Base64.Encoder base64Encoder = Base64.getEncoder();

    PrivateKey privKey = _client.getKeyProvider().getPrivateKey(keyName);
    Cipher cipherRSA = Cipher.getInstance("RSA");
    cipherRSA.init(Cipher.DECRYPT_MODE, privKey);
    String symKeyStr = destUserMeta.get("s3tool-symmetric-key");
    // Make sure we can decrypt symmetric key
    cipherRSA.doFinal(base64Decoder.decode(symKeyStr));

    Key pubKey = _client.getKeyProvider().getPublicKey(keyName);
    String pubKeyHash = base64Encoder.encodeToString(DigestUtils.sha256(pubKey.getEncoded()));
    Assert.assertEquals(destUserMeta.get("s3tool-pubkey-hash"), pubKeyHash.substring(0, 8));
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
      {
        uex = (UsageException) ex.getCause();
      }
    }

    if(null == uex)
    {
      throw ex;
    }

    if(null != expectedMsg)
    {
      Assert.assertTrue(uex.getMessage().contains(expectedMsg));
    }
  }
}
