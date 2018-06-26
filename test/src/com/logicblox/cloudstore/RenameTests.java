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
import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.net.URI;
import java.security.Key;
import java.security.PrivateKey;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RenameTests
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
  public void testDryRunFile()
    throws Throwable
  {
    // create test file and upload it
    String rootPrefix = TestUtils.addPrefix("rename-dryrun");
    int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);
    Assert.assertEquals(originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());

    // dryrun the rename and make sure dest stays the same
    URI src = dest;
    dest = TestUtils.getUri(_testBucket, toUpload.getName() + "-RENAME", rootPrefix);
    RenameOptions opts = _client.getOptionsBuilderFactory()
      .newRenameOptionsBuilder()
      .setSourceBucketName(Utils.getBucketName(src))
      .setSourceObjectKey(Utils.getObjectKey(src))
      .setDestinationBucketName(Utils.getBucketName(dest))
      .setDestinationObjectKey(Utils.getObjectKey(dest))
      .setDryRun(true)
      .createOptions();
    f = _client.rename(opts).get();
    Assert.assertNull(f);
    List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(src)));
    Assert.assertFalse(TestUtils.findObject(objs, Utils.getObjectKey(dest)));
  }


  @Test
  public void testDryRunDir()
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
        // create simple directory structure with a few files and upload
        File top = TestUtils.createTmpDir(true);
        File a = TestUtils.createTextFile(top, 100);
        File b = TestUtils.createTextFile(top, 100);
        String rootPrefix = TestUtils.addPrefix("rename-dryrun-dir/");
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
        Assert.assertEquals(2, uploaded.size());
        Assert.assertEquals(originalCount + 2,
          TestUtils.listObjects(_testBucket, rootPrefix).size());

        // dryrun the rename and make sure the dest doesn't change
        URI src = dest;
        dest = TestUtils.getUri(_testBucket, top.getName() + "-RENAME", rootPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest) + "/")
          .setDryRun(true)
          .createOptions();
        List<StoreFile> files = _client.renameRecursively(opts).get();
        Assert.assertNull(files);
        List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
        Assert.assertEquals(originalCount + 2, objs.size());

        String topN = rootPrefix + top.getName() + "/";
        Assert.assertTrue(TestUtils.findObject(objs, topN + a.getName()));
        Assert.assertTrue(TestUtils.findObject(objs, topN + b.getName()));

        String renameN = rootPrefix + top.getName() + "-RENAME" + "/";
        Assert.assertFalse(TestUtils.findObject(objs, renameN + a.getName()));
        Assert.assertFalse(TestUtils.findObject(objs, renameN + b.getName()));

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
  public void testRenameDirAbortOneDuringCopy()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.
    // trying to minimize false failure reports by repeating and only failing
    // the test if it consistently reports an error.
    int retryCount = TestUtils.RETRY_COUNT;
    int count = 0;
    while(count < retryCount)
    {
      boolean oldGlobalFlag = false;
      try
      {
        // create simple directory structure and upload
        File top = TestUtils.createTmpDir(true);
        File a = TestUtils.createTextFile(top, 100);
        File b = TestUtils.createTextFile(top, 100);
        File c = TestUtils.createTextFile(top, 100);

        String rootPrefix = TestUtils.addPrefix("rename-dir-abort-one-on-copy-" + count + "/");
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
        Assert.assertEquals(3, uploaded.size());
        int uploadCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        Assert.assertEquals(uploaded.size(), uploadCount);

        // rename the directory
        URI src = dest;
        String destPrefix = TestUtils.addPrefix(
          "rename-dir-abort-one-on-copy-dest-" + count + "/subdir/");
        int newCount = TestUtils.listObjects(_testBucket, destPrefix).size();
        dest = TestUtils.getUri(_testBucket, "subdir2", destPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest) + "/")
          .createOptions();
        oldGlobalFlag = CopyOptions.getAbortCounters().useGlobalCounter(true);
        CopyOptions.getAbortCounters().setInjectionCounter(1);
        // abort first rename during copy phase
        try
        {
          _client.renameRecursively(opts).get();
        }
        catch(ExecutionException ex)
        {
          // expected for one of the rename jobs
          Assert.assertTrue(ex.getMessage().contains("forcing copy abort"));
        }

        // verify that nothing moved
        List<StoreFile> destObjs = TestUtils.listObjects(_testBucket, destPrefix);
        String topDestN = destPrefix + "subdir2/";
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + a.getName()));
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + b.getName()));
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + c.getName()));

        List<StoreFile> srcObjs = TestUtils.listObjects(_testBucket, rootPrefix);
        String topN = rootPrefix + top.getName() + "/";
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + a.getName()));
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + b.getName()));
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + c.getName()));

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
      finally
      {
        // reset abort injection so other tests aren't affected
        CopyOptions.getAbortCounters().useGlobalCounter(oldGlobalFlag);
        CopyOptions.getAbortCounters().setInjectionCounter(0);
        CopyOptions.getAbortCounters().clearInjectionCounters();
      }
    }
  }


  // FIXME - need to find a way to abort a random operation.  always aborting the
  //         first which can hide any cleanup problems when trying to undo a
  //         partial rename
  // FIXME - disabling this test for now until we can find a robust way to correctly
  //         recover from these failures
  //  @Test
  public void testRenameDirAbortOneDuringDelete()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.
    // trying to minimize false failure reports by repeating and only failing
    // the test if it consistently reports an error.
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
        File c = TestUtils.createTextFile(top, 100);

        String rootPrefix = TestUtils.addPrefix("rename-dir-abort-one-on-delete-" + count + "/");
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
        Assert.assertEquals(3, uploaded.size());
        int uploadCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        Assert.assertEquals(uploaded.size(), uploadCount);

        // rename the directory
        URI src = dest;
        String destPrefix = TestUtils.addPrefix(
          "rename-dir-abort-one-on-delete-dest-" + count + "/subdir/");
        int newCount = TestUtils.listObjects(_testBucket, destPrefix).size();
        dest = TestUtils.getUri(_testBucket, "subdir2", destPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest) + "/")
          .createOptions();
        boolean oldGlobalFlag = false;
        try
        {
          oldGlobalFlag = DeleteOptions.getAbortCounters().useGlobalCounter(true);
          DeleteOptions.getAbortCounters().setInjectionCounter(1);
          // abort first rename during delete phase
          _client.renameRecursively(opts).get();
        }
        catch(ExecutionException ex)
        {
          // expected for one of the rename jobs
          Assert.assertTrue(ex.getMessage().contains("forcing delete abort"));
        }
        finally
        {
          // reset abort injection so other tests aren't affected
          DeleteOptions.getAbortCounters().useGlobalCounter(oldGlobalFlag);
          DeleteOptions.getAbortCounters().setInjectionCounter(0);
          DeleteOptions.getAbortCounters().clearInjectionCounters();
        }

        // verify that nothing moved
        List<StoreFile> destObjs = TestUtils.listObjects(_testBucket, destPrefix);
        List<StoreFile> srcObjs = TestUtils.listObjects(_testBucket, rootPrefix);

        String topDestN = destPrefix + "subdir2/";
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + a.getName()));
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + b.getName()));
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + c.getName()));

        String topN = rootPrefix + top.getName() + "/";
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + a.getName()));
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + b.getName()));
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + c.getName()));

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


  // FIXME - disabling this test for now until we can find a robust way to correctly
  //         recover from these failures
  //  @Test
  public void testRenameDirAllAbortDuringDelete()
    throws Throwable
  {
    // NOTE: this test dumps a stack trace that can be ignored

    // directory copy/upload/rename tests intermittently fail when using minio.
    // trying to minimize false failure reports by repeating and only failing
    // the test if it consistently reports an error.
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

        String rootPrefix = TestUtils.addPrefix("rename-dir-all-abort-on-delete-" + count + "/");
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
        Assert.assertEquals(2, uploaded.size());
        int uploadCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        Assert.assertEquals(uploaded.size(), uploadCount);

        // rename the directory
        URI src = dest;
        String destPrefix = TestUtils.addPrefix(
          "rename-dir-all-abort-on-delete-dest-" + count + "/subdir/");
        int newCount = TestUtils.listObjects(_testBucket, destPrefix).size();
        dest = TestUtils.getUri(_testBucket, "subdir2", destPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest) + "/")
          .createOptions();
        DeleteOptions.getAbortCounters().setInjectionCounter(1);
        // should be one more than retry count.  retries disabled by default
        String msg = null;
        try
        {
          _client.renameRecursively(opts).get();
          msg = "expected exception (forcing abort on delete)";
        }
        catch(ExecutionException ex)
        {
          // expected
          Assert.assertTrue(TestUtils.findCause(ex, AbortInjection.class));
          Assert.assertTrue(ex.getMessage().contains("forcing delete abort"));
        }
        Assert.assertNull(msg);

        // verify that nothing moved
        List<StoreFile> destObjs = TestUtils.listObjects(_testBucket, destPrefix);
        String topDestN = destPrefix + "subdir2/";
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + a.getName()));
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + b.getName()));

        List<StoreFile> srcObjs = TestUtils.listObjects(_testBucket, rootPrefix);
        String topN = rootPrefix + top.getName();
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + "/" + a.getName()));
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + "/" + b.getName()));

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
      finally
      {
        // reset abort injection so other tests aren't affected
        DeleteOptions.getAbortCounters().setInjectionCounter(0);
        DeleteOptions.getAbortCounters().clearInjectionCounters();
      }
    }
  }


  @Test
  public void testRenameDirAllAbortDuringCopy()
    throws Throwable
  {
    // NOTE: this test dumps a stack trace that can be ignored

    // directory copy/upload/rename tests intermittently fail when using minio.
    // trying to minimize false failure reports by repeating and only failing
    // the test if it consistently reports an error.
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

        String rootPrefix = TestUtils.addPrefix("rename-dir-all-abort-on-copy-" + count + "/");
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
        Assert.assertEquals(2, uploaded.size());
        int uploadCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        Assert.assertEquals(uploaded.size(), uploadCount);

        // rename the directory
        URI src = dest;
        String destPrefix = TestUtils.addPrefix(
          "rename-dir-all-abort-on-copy-dest-" + count + "/subdir/");
        int newCount = TestUtils.listObjects(_testBucket, destPrefix).size();
        dest = TestUtils.getUri(_testBucket, "subdir2", destPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest) + "/")
          .createOptions();
        CopyOptions.getAbortCounters().setInjectionCounter(1);
        // should be one more than retry count.  retries disabled by default
        String msg = null;
        try
        {
          _client.renameRecursively(opts).get();
          msg = "expected exception (forcing abort on copy)";
        }
        catch(ExecutionException ex)
        {
          // expected
          Assert.assertTrue(TestUtils.findCause(ex, AbortInjection.class));
          Assert.assertTrue(ex.getMessage().contains("forcing copy abort"));
        }
        Assert.assertNull(msg);

        // verify that nothing moved
        List<StoreFile> destObjs = TestUtils.listObjects(_testBucket, destPrefix);
        String topDestN = destPrefix + "subdir2/";
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + a.getName()));
        Assert.assertFalse(TestUtils.findObject(destObjs, topDestN + b.getName()));

        List<StoreFile> srcObjs = TestUtils.listObjects(_testBucket, rootPrefix);
        String topN = rootPrefix + top.getName();
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + "/" + a.getName()));
        Assert.assertTrue(TestUtils.findObject(srcObjs, topN + "/" + b.getName()));

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
      finally
      {
        // reset abort injection so other tests aren't affected
        CopyOptions.getAbortCounters().setInjectionCounter(0);
        CopyOptions.getAbortCounters().clearInjectionCounters();
      }
    }
  }


  @Test
  public void testRetryOnCopy()
    throws Throwable
  {
    try
    {
      // create a small file and upload it
      String rootPrefix = TestUtils.addPrefix("rename-retry-during-copy-");
      int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
      File toUpload = TestUtils.createTextFile(100);
      URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
      StoreFile f = TestUtils.uploadFile(toUpload, dest);
      Assert.assertNotNull(f);
      Assert.assertEquals(originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());

      // set retry options and aborts during copy phase of rename
      ThrowableRetriableTask.addRetryListener(this);
      clearRetryCount();
      int retryCount = 10;
      int abortCount = 3;
      _client.setRetryCount(retryCount);
      CopyOptions.getAbortCounters().setInjectionCounter(abortCount);

      // rename the file
      URI src = dest;
      dest = TestUtils.getUri(_testBucket, toUpload.getName() + "-RENAMED", rootPrefix);
      RenameOptions opts = _client.getOptionsBuilderFactory()
        .newRenameOptionsBuilder()
        .setSourceBucketName(Utils.getBucketName(src))
        .setSourceObjectKey(Utils.getObjectKey(src))
        .setDestinationBucketName(Utils.getBucketName(dest))
        .setDestinationObjectKey(Utils.getObjectKey(dest))
        .createOptions();
      f = _client.rename(opts).get();

      // verify that the rename succeeded and we triggered the right number of retries
      Assert.assertEquals(abortCount, getRetryCount());
      Assert.assertNotNull(f);
      Assert.assertEquals(Utils.getObjectKey(dest), f.getObjectKey());
      List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
      Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));
      Assert.assertFalse(TestUtils.findObject(objs, Utils.getObjectKey(src)));
    }
    finally
    {
      // reset retry and abort injection state so we don't affect other tests
      TestUtils.resetRetryCount();
      CopyOptions.getAbortCounters().setInjectionCounter(0);
      CopyOptions.getAbortCounters().clearInjectionCounters();
    }
  }


  @Test
  public void testRetryOnDelete()
    throws Throwable
  {
    try
    {
      // create a small file and upload it
      String rootPrefix = TestUtils.addPrefix("rename-retry-during-delete-");
      int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
      File toUpload = TestUtils.createTextFile(100);
      URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
      StoreFile f = TestUtils.uploadFile(toUpload, dest);
      Assert.assertNotNull(f);
      Assert.assertEquals(originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());

      // set retry options and aborts during copy phase of rename
      ThrowableRetriableTask.addRetryListener(this);
      clearRetryCount();
      int retryCount = 10;
      int abortCount = 3;
      _client.setRetryCount(retryCount);
      DeleteOptions.getAbortCounters().setInjectionCounter(abortCount);

      // rename the file
      URI src = dest;
      dest = TestUtils.getUri(_testBucket, toUpload.getName() + "-RENAMED", rootPrefix);
      RenameOptions opts = _client.getOptionsBuilderFactory()
        .newRenameOptionsBuilder()
        .setSourceBucketName(Utils.getBucketName(src))
        .setSourceObjectKey(Utils.getObjectKey(src))
        .setDestinationBucketName(Utils.getBucketName(dest))
        .setDestinationObjectKey(Utils.getObjectKey(dest))
        .createOptions();
      f = _client.rename(opts).get();

      // verify that the rename succeeded and we triggered the right number of retries
      Assert.assertEquals(abortCount, getRetryCount());
      Assert.assertNotNull(f);
      Assert.assertEquals(Utils.getObjectKey(dest), f.getObjectKey());
      List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
      Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(dest)));
      Assert.assertFalse(TestUtils.findObject(objs, Utils.getObjectKey(src)));
    }
    finally
    {
      // reset retry and abort injection state so we don't affect other tests
      TestUtils.resetRetryCount();
      DeleteOptions.getAbortCounters().setInjectionCounter(0);
      DeleteOptions.getAbortCounters().clearInjectionCounters();
    }
  }


  @Test
  public void testAbortDuringCopy()
    throws Throwable
  {
    try
    {
      // create a small file and upload it
      String rootPrefix = TestUtils.addPrefix("rename-abort-during-copy-");
      int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
      File toUpload = TestUtils.createTextFile(100);
      URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
      StoreFile f = TestUtils.uploadFile(toUpload, dest);
      Assert.assertNotNull(f);
      Assert.assertEquals(originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());

      // rename the file
      URI src = dest;
      dest = TestUtils.getUri(_testBucket, toUpload.getName() + "-RENAMED", rootPrefix);
      RenameOptions opts = _client.getOptionsBuilderFactory()
        .newRenameOptionsBuilder()
        .setSourceBucketName(Utils.getBucketName(src))
        .setSourceObjectKey(Utils.getObjectKey(src))
        .setDestinationBucketName(Utils.getBucketName(dest))
        .setDestinationObjectKey(Utils.getObjectKey(dest))
        .createOptions();
      CopyOptions.getAbortCounters().setInjectionCounter(1);
      // should be one more than retry count.  retries disabled by default
      String msg = null;
      try
      {
        _client.rename(opts).get();
        msg = "expected exception (forcing abort on copy)";
      }
      catch(ExecutionException ex)
      {
        // expected
        Assert.assertTrue(TestUtils.findCause(ex, AbortInjection.class));
        Assert.assertTrue(ex.getMessage().contains("forcing copy abort"));
      }
      Assert.assertNull(msg);

      // file should not be renamed since we aborted
      List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
      Assert.assertFalse(TestUtils.findObject(objs, Utils.getObjectKey(dest)));
      Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(src)));
    }
    finally
    {
      // reset abort injection so other tests aren't affected
      CopyOptions.getAbortCounters().setInjectionCounter(0);
      CopyOptions.getAbortCounters().clearInjectionCounters();
    }
  }


  @Test
  public void testAbortDuringDelete()
    throws Throwable
  {
    try
    {
      // create a small file and upload it
      String rootPrefix = TestUtils.addPrefix("rename-abort-during-delete-");
      int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
      File toUpload = TestUtils.createTextFile(100);
      URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
      StoreFile f = TestUtils.uploadFile(toUpload, dest);
      Assert.assertNotNull(f);
      Assert.assertEquals(originalCount + 1, TestUtils.listObjects(_testBucket, rootPrefix).size());

      // rename the file
      URI src = dest;
      dest = TestUtils.getUri(_testBucket, toUpload.getName() + "-RENAMED", rootPrefix);
      RenameOptions opts = _client.getOptionsBuilderFactory()
        .newRenameOptionsBuilder()
        .setSourceBucketName(Utils.getBucketName(src))
        .setSourceObjectKey(Utils.getObjectKey(src))
        .setDestinationBucketName(Utils.getBucketName(dest))
        .setDestinationObjectKey(Utils.getObjectKey(dest))
        .createOptions();
      DeleteOptions.getAbortCounters().setInjectionCounter(1);
      // should be one more than retry count.  retries disabled by default
      String msg = null;
      try
      {
        _client.rename(opts).get();
        msg = "expected exception (forcing abort on copy)";
      }
      catch(ExecutionException ex)
      {
        // expected
        Assert.assertTrue(TestUtils.findCause(ex, AbortInjection.class));
        Assert.assertTrue(ex.getMessage().contains("forcing delete abort"));
      }
      Assert.assertNull(msg);

      // file should not be renamed since we aborted
      List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
      Assert.assertFalse(TestUtils.findObject(objs, Utils.getObjectKey(dest)));
      Assert.assertTrue(TestUtils.findObject(objs, Utils.getObjectKey(src)));
    }
    finally
    {
      // reset abort injection so other tests aren't affected
      DeleteOptions.getAbortCounters().setInjectionCounter(0);
      DeleteOptions.getAbortCounters().clearInjectionCounters();
    }
  }


  @Test
  public void testSimpleObject()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
    int retryCount = TestUtils.RETRY_COUNT;
    int count = 0;
    while(count < retryCount)
    {
      try
      {
        // create a small file and upload it
        String rootPrefix = TestUtils.addPrefix("rename-simple-" + count);
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        File toUpload = TestUtils.createTextFile(100);
        URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
        StoreFile f = TestUtils.uploadFile(toUpload, dest);
        Assert.assertNotNull(f);
        Assert.assertEquals(originalCount + 1,
          TestUtils.listObjects(_testBucket, rootPrefix).size());

        // rename the file
        URI src = dest;
        String newPrefix = TestUtils.addPrefix("rename-simple-dest-" + count + "/c/d/");
        int newCount = TestUtils.listObjects(_testBucket, newPrefix).size();
        dest = TestUtils.getUri(_testBucket, "new-file.txt", newPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest))
          .createOptions();
        f = _client.rename(opts).get();
        Assert.assertNotNull(f);
        Assert.assertEquals(Utils.getObjectKey(dest), f.getObjectKey());
        Assert.assertEquals(Utils.getBucketName(dest), f.getBucketName());

        // verify that it moved
        Assert.assertEquals(originalCount, TestUtils.listObjects(_testBucket, rootPrefix).size());
        Assert.assertNull(TestUtils.objectExists(Utils.getBucketName(src), Utils.getObjectKey(src)));
        Assert.assertNotNull(
          TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest)));
        List<StoreFile> objs = TestUtils.listObjects(_testBucket, newPrefix);
        Assert.assertEquals(newCount + 1, objs.size());
        Assert.assertTrue(TestUtils.findObject(objs, newPrefix + "new-file.txt"));
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
  public void testDestExists()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
    int retryCount = TestUtils.RETRY_COUNT;
    int count = 0;
    while(count < retryCount)
    {
      try
      {
        // create two small files and upload
        String rootPrefix = TestUtils.addPrefix("rename-dest-exists-" + count);
        File file1 = TestUtils.createTextFile(100);
        URI dest1 = TestUtils.getUri(_testBucket, file1, rootPrefix);
        StoreFile f = TestUtils.uploadFile(file1, dest1);
        Assert.assertNotNull(f);

        File file2 = TestUtils.createTextFile(100);
        URI dest2 = TestUtils.getUri(_testBucket, file2, rootPrefix);
        f = TestUtils.uploadFile(file2, dest2);
        Assert.assertNotNull(f);

        // rename file1 to file2
        URI src = dest1;
        URI dest = dest2;
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest))
          .createOptions();
        String msg = null;
        try
        {
          _client.rename(opts).get();
          msg = "expected exception (dest exists)";
        }
        catch(Exception ex)
        {
          // expected
          checkUsageException(ex, "Cannot overwrite existing destination");
        }
        Assert.assertNull(msg);
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
  public void testSameSourceAndDest()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
    int retryCount = TestUtils.RETRY_COUNT;
    int count = 0;
    while(count < retryCount)
    {
      try
      {
        // create a small files and upload
        String rootPrefix = TestUtils.addPrefix("rename-same-src-dest-" + count);
        File file = TestUtils.createTextFile(100);
        URI dest = TestUtils.getUri(_testBucket, file, rootPrefix);
        StoreFile f = TestUtils.uploadFile(file, dest);
        Assert.assertNotNull(f);

        // rename
        URI src = dest;
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest))
          .createOptions();
        String msg = null;
        try
        {
          _client.rename(opts).get();
          msg = "expected exception (dest exists)";
        }
        catch(Exception ex)
        {
          // expected
          checkUsageException(ex, "Cannot overwrite existing destination");
        }
        Assert.assertNull(msg);
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
  public void testMissingSource()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
    int retryCount = TestUtils.RETRY_COUNT;
    int count = 0;
    while(count < retryCount)
    {
      try
      {
        String rootPrefix = TestUtils.addPrefix("rename-missing-source-" + count);
        URI src = TestUtils.getUri(_testBucket, "rename-missing-file" + System.currentTimeMillis(),
          rootPrefix);
        URI dest = TestUtils.getUri(_testBucket, "dummy.txt", rootPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest))
          .createOptions();
        String msg = null;
        try
        {
          _client.rename(opts).get();
          msg = "expected exception (source missing)";
        }
        catch(Exception ex)
        {
          // expected
          checkUsageException(ex, "does not exist");
        }
        Assert.assertNull(msg);
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
  public void testMoveObjectAcrossBuckets()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
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
        {
          return;
        }

        // create a small file and upload it
        String rootPrefix = TestUtils.addPrefix("rename-move-obj-across-buckets-" + count);
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        File toUpload = TestUtils.createTextFile(100);
        URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
        StoreFile f = TestUtils.uploadFile(toUpload, dest);
        Assert.assertNotNull(f);
        Assert.assertEquals(originalCount + 1,
          TestUtils.listObjects(_testBucket, rootPrefix).size());

        // rename the file
        String bucket2 = TestUtils.createTestBucket();
        URI src = dest;
        String newPrefix = TestUtils.addPrefix(
          "rename-move-obj-across-buckets-dest-" + count + "/c/d/");
        int newCount = TestUtils.listObjects(bucket2, newPrefix).size();
        dest = TestUtils.getUri(bucket2, "new-file.txt", newPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest))
          .createOptions();
        f = _client.rename(opts).get();
        Assert.assertEquals(Utils.getObjectKey(dest), f.getObjectKey());
        Assert.assertEquals(Utils.getBucketName(dest), f.getBucketName());

        // verify that it moved
        Assert.assertNotNull(f);
        Assert.assertEquals(originalCount, TestUtils.listObjects(_testBucket, rootPrefix).size());
        Assert.assertNull(TestUtils.objectExists(Utils.getBucketName(src), Utils.getObjectKey(src)));
        Assert.assertNotNull(
          TestUtils.objectExists(Utils.getBucketName(dest), Utils.getObjectKey(dest)));
        List<StoreFile> objs = TestUtils.listObjects(bucket2, newPrefix);
        Assert.assertEquals(newCount + 1, objs.size());
        Assert.assertTrue(TestUtils.findObject(objs, newPrefix + "new-file.txt"));
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
  public void testRecursiveDirectory()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
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
        File sub = TestUtils.createTmpDir(top);
        File c = TestUtils.createTextFile(sub, 100);
        File d = TestUtils.createTextFile(sub, 100);
        File sub2 = TestUtils.createTmpDir(sub);
        File e = TestUtils.createTextFile(sub2, 100);

        String rootPrefix = TestUtils.addPrefix("rename-dir-recursive-" + count + "/");
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
        Assert.assertEquals(5, uploaded.size());
        int uploadCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        Assert.assertEquals(uploaded.size(), uploadCount);

        // rename the directory
        URI src = dest;
        String newPrefix = TestUtils.addPrefix("rename-dir-recursive-dest-" + count + "/subdir/");
        int newCount = TestUtils.listObjects(_testBucket, newPrefix).size();
        dest = TestUtils.getUri(_testBucket, "subdir2", newPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest) + "/")
          .createOptions();
        List<StoreFile> renamedFiles = _client.renameRecursively(opts).get();

        // verify that everything moved
        Assert.assertEquals(uploadCount, renamedFiles.size());
        for(StoreFile f : renamedFiles)
          Assert.assertEquals(Utils.getBucketName(dest), f.getBucketName());
        List<StoreFile> newObjs = TestUtils.listObjects(_testBucket, newPrefix);
        Assert.assertEquals(newCount + renamedFiles.size(), newObjs.size());
        Assert.assertEquals(uploadCount - renamedFiles.size(),
          TestUtils.listObjects(_testBucket, rootPrefix).size());

        // verify that the structure was replicated correctly
        String topN = newPrefix + "subdir2/";
        String subN = topN + sub.getName() + "/";
        String sub2N = subN + sub2.getName() + "/";
        Assert.assertTrue(TestUtils.findObject(newObjs, topN + a.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, topN + b.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, subN + c.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, subN + d.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, sub2N + e.getName()));
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
  public void testMoveDirectoryAcrossBuckets()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
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
        {
          return;
        }

        // create simple directory structure and upload
        File top = TestUtils.createTmpDir(true);
        File a = TestUtils.createTextFile(top, 100);
        File b = TestUtils.createTextFile(top, 100);
        File sub = TestUtils.createTmpDir(top);
        File c = TestUtils.createTextFile(sub, 100);
        File d = TestUtils.createTextFile(sub, 100);
        File sub2 = TestUtils.createTmpDir(sub);
        File e = TestUtils.createTextFile(sub2, 100);

        String rootPrefix = TestUtils.addPrefix("rename-dir-across-buckets-" + count + "/");
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        URI dest = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, dest);
        Assert.assertEquals(5, uploaded.size());
        int uploadCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        Assert.assertEquals(uploaded.size(), uploadCount);

        // rename the directory
        String bucket2 = TestUtils.createTestBucket();
        URI src = dest;
        String newPrefix = TestUtils.addPrefix(
          "rename-dir-across-buckets-dest-" + count + "/subdir/");
        int newCount = TestUtils.listObjects(bucket2, newPrefix).size();
        dest = TestUtils.getUri(bucket2, "subdir2", newPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest) + "/")
          .createOptions();
        List<StoreFile> renamedFiles = _client.renameRecursively(opts).get();

        // verify that everything moved
        Assert.assertEquals(uploadCount, renamedFiles.size());
        for(StoreFile f : renamedFiles)
          Assert.assertEquals(Utils.getBucketName(dest), f.getBucketName());
        List<StoreFile> newObjs = TestUtils.listObjects(bucket2, newPrefix);
        Assert.assertEquals(newCount + renamedFiles.size(), newObjs.size());
        Assert.assertEquals(uploadCount - renamedFiles.size(),
          TestUtils.listObjects(_testBucket, rootPrefix).size());

        // verify that the structure was replicated correctly
        String topN = newPrefix + "subdir2/";
        String subN = topN + sub.getName() + "/";
        String sub2N = subN + sub2.getName() + "/";
        Assert.assertTrue(TestUtils.findObject(newObjs, topN + a.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, topN + b.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, subN + c.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, subN + d.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, sub2N + e.getName()));
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
  public void testMissingSourceDir()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
    int retryCount = TestUtils.RETRY_COUNT;
    int count = 0;
    while(count < retryCount)
    {
      try
      {
        String rootPrefix = TestUtils.addPrefix("rename-missing-src-dir-" + count);
        URI src = TestUtils.getUri(_testBucket, "rename-missing-dir" + System.currentTimeMillis(),
          rootPrefix);
        URI dest = TestUtils.getUri(_testBucket, "subdir", rootPrefix);
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest) + "/")
          .createOptions();
        String msg = null;
        List<StoreFile> storeFiles = _client.renameRecursively(opts).get();
        Assert.assertTrue(storeFiles.isEmpty());
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
  public void testDirOverwriteFile()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
    int retryCount = TestUtils.RETRY_COUNT;
    int count = 0;
    while(count < retryCount)
    {
      try
      {
        // create a small file and upload it
        String rootPrefix = TestUtils.addPrefix("rename-dir-overwrite-file-" + count);
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        File toUpload = TestUtils.createTextFile(100);
        URI destFile = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
        StoreFile f = TestUtils.uploadFile(toUpload, destFile);
        Assert.assertNotNull(f);
        Assert.assertEquals(originalCount + 1,
          TestUtils.listObjects(_testBucket, rootPrefix).size());

        // create simple directory structure and upload
        File top = TestUtils.createTmpDir(true);
        File a = TestUtils.createTextFile(top, 100);
        File b = TestUtils.createTextFile(top, 100);
        URI destDir = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, destDir);
        Assert.assertEquals(2, uploaded.size());
        int uploadCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        Assert.assertEquals(uploaded.size() + 1, uploadCount);

        // attempt the rename -- should fail
        URI src = destDir;
        URI dest = destFile;
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest) + "/")
          .createOptions();
        String msg = null;
        try
        {
          _client.renameRecursively(opts).get();
          msg = "expected exception (source missing)";
        }
        catch(Exception ex)
        {
          // expected
          checkUsageException(ex, "Cannot overwrite existing destination");
        }
        Assert.assertNull(msg);
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
  public void testMoveFileIntoExistingDir()
    throws Throwable
  {
    // directory copy/upload/rename tests intermittently fail when using minio.  trying to
    // minimize false failure reports by repeating and only failing the test if it consistently
    // reports an error.
    int retryCount = TestUtils.RETRY_COUNT;
    int count = 0;
    while(count < retryCount)
    {
      try
      {
        // create a small file and upload it
        String rootPrefix = TestUtils.addPrefix("move-file-into-dir-" + count);
        int originalCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        File toRename = TestUtils.createTextFile(100);
        URI destFile = TestUtils.getUri(_testBucket, toRename, rootPrefix);
        StoreFile f = TestUtils.uploadFile(toRename, destFile);
        Assert.assertNotNull(f);
        Assert.assertEquals(originalCount + 1,
          TestUtils.listObjects(_testBucket, rootPrefix).size());

        // create simple directory structure and upload
        File top = TestUtils.createTmpDir(true);
        File a = TestUtils.createTextFile(top, 100);
        File b = TestUtils.createTextFile(top, 100);
        URI destDir = TestUtils.getUri(_testBucket, top, rootPrefix);
        List<StoreFile> uploaded = TestUtils.uploadDir(top, destDir);
        Assert.assertEquals(2, uploaded.size());
        int uploadCount = TestUtils.listObjects(_testBucket, rootPrefix).size();
        Assert.assertEquals(uploaded.size() + 1, uploadCount);

        // attempt the rename -- should move the file but leave previous files alone
        URI src = destFile;
        URI dest = destDir;
        RenameOptions opts = _client.getOptionsBuilderFactory()
          .newRenameOptionsBuilder()
          .setSourceBucketName(Utils.getBucketName(src))
          .setSourceObjectKey(Utils.getObjectKey(src))
          .setDestinationBucketName(Utils.getBucketName(dest))
          .setDestinationObjectKey(Utils.getObjectKey(dest))
          .createOptions();
        f = _client.rename(opts).get();
        Assert.assertNotNull(f);
        List<StoreFile> newObjs = TestUtils.listObjects(_testBucket, rootPrefix);
        Assert.assertEquals(uploadCount, newObjs.size());
        String topN = rootPrefix + "/" + top.getName() + "/";
        Assert.assertTrue(TestUtils.findObject(newObjs, topN + a.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, topN + b.getName()));
        Assert.assertTrue(TestUtils.findObject(newObjs, topN + toRename.getName()));

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
  public void testDefaultS3Acl()
    throws Throwable
  {
    // this test makes sense only for AWS S3. Minio doesn't support ACLs and current ACL
    // support on GCS is limited
    Assume.assumeTrue(TestUtils.getService().equalsIgnoreCase("s3") &&
      TestUtils.supportsAcl());

    // create test file and upload it
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("test-rename-acl");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // rename file
    RenameOptions renameOptions = _client.getOptionsBuilderFactory()
      .newRenameOptionsBuilder()
      .setSourceBucketName(_testBucket)
      .setSourceObjectKey(f.getObjectKey())
      .setDestinationBucketName(_testBucket)
      .setDestinationObjectKey(f.getObjectKey() + "-RENAME")
      .createOptions();
    StoreFile rename = _client.rename(renameOptions).get();
    String expectedKey = TestUtils.addPrefix("test-rename-acl/" + toUpload.getName() + "-RENAME");
    Assert.assertEquals(expectedKey, rename.getObjectKey());
    dest = TestUtils.getUri(_testBucket, rename.getObjectKey(), "");
    AclHandler aclHandler = _client.getAclHandler();

    // validate ACL - default
    Acl destObjectAcl = aclHandler.getObjectAcl(Utils.getBucketName(dest),
      Utils.getObjectKey(dest)).get();
    Owner destBucketOwner = _client.getBucket(Utils.getBucketName((dest))).get().getOwner();

    Assert.assertEquals(destObjectAcl.getGrants().size(), 1);
    AclPermission destPerm = destObjectAcl.getGrants().get(0).getPermission();
    Assert.assertEquals(destPerm.toString(), "FULL_CONTROL");

    AclGrantee destGrantee = destObjectAcl.getGrants().get(0).getGrantee();
    Assert.assertEquals(destGrantee.getId(), destBucketOwner.getId());
  }


  @Test
  public void testNonDefaultS3Acl()
    throws Throwable
  {
    // this test makes sense only for AWS S3. Minio doesn't support ACLs and current ACL
    // support on GCS is limited
    Assume.assumeTrue(TestUtils.getService().equalsIgnoreCase("s3") &&
      TestUtils.supportsAcl());

    // create test file and upload it
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("test-rename-acl");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    UploadOptions upOpts = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(toUpload)
      .setBucketName(Utils.getBucketName(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setCannedAcl("authenticated-read")
      .createOptions();
    StoreFile f = _client.upload(upOpts).get();
    Assert.assertNotNull(f);

    // rename file
    RenameOptions renameOptions = _client.getOptionsBuilderFactory()
      .newRenameOptionsBuilder()
      .setSourceBucketName(_testBucket)
      .setSourceObjectKey(f.getObjectKey())
      .setDestinationBucketName(_testBucket)
      .setDestinationObjectKey(f.getObjectKey() + "-RENAME")
      .createOptions();
    StoreFile rename = _client.rename(renameOptions).get();
    String expectedKey = TestUtils.addPrefix("test-rename-acl/" + toUpload.getName() + "-RENAME");
    Assert.assertEquals(expectedKey, rename.getObjectKey());
    dest = TestUtils.getUri(_testBucket, rename.getObjectKey(), "");
    AclHandler aclHandler = _client.getAclHandler();

    // validate ACL - authenticated-read
    Acl destObjectAcl = aclHandler.getObjectAcl(Utils.getBucketName(dest), Utils.getObjectKey(dest))
      .get();
    Owner destBucketOwner = _client.getBucket(Utils.getBucketName((dest))).get().getOwner();

    Assert.assertEquals(destObjectAcl.getGrants().size(), 2);

    AclGrant destFcAclGrant;
    AclGrant destReadAclGrant;
    if(destObjectAcl.getGrants().get(0).getPermission().toString().equals("FULL_CONTROL"))
    {
      destFcAclGrant = destObjectAcl.getGrants().get(0);
      destReadAclGrant = destObjectAcl.getGrants().get(1);
    }
    else
    {
      destFcAclGrant = destObjectAcl.getGrants().get(1);
      destReadAclGrant = destObjectAcl.getGrants().get(0);
    }

    AclPermission destPerm = destFcAclGrant.getPermission();
    Assert.assertEquals(destPerm.toString(), "FULL_CONTROL");
    AclGrantee destGrantee = destFcAclGrant.getGrantee();
    Assert.assertEquals(destGrantee.getId(), destBucketOwner.getId());

    destPerm = destReadAclGrant.getPermission();
    Assert.assertEquals(destPerm.toString(), "READ");
    destGrantee = destReadAclGrant.getGrantee();
    Assert.assertEquals(destGrantee.getId(),
      "http://acs.amazonaws.com/groups/global/AuthenticatedUsers");
  }


  @Test
  public void testNewS3Acl()
    throws Throwable
  {
    // this test makes sense only for AWS S3. Minio doesn't support ACLs and current ACL
    // support on GCS is limited
    Assume.assumeTrue(TestUtils.getService().equalsIgnoreCase("s3") &&
      TestUtils.supportsAcl());

    // create test file and upload it
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    String rootPrefix = TestUtils.addPrefix("test-rename-acl");
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // rename file
    RenameOptions renameOptions = _client.getOptionsBuilderFactory()
      .newRenameOptionsBuilder()
      .setSourceBucketName(_testBucket)
      .setSourceObjectKey(f.getObjectKey())
      .setDestinationBucketName(_testBucket)
      .setDestinationObjectKey(f.getObjectKey() + "-RENAME")
      .setCannedAcl("authenticated-read")
      .createOptions();
    StoreFile rename = _client.rename(renameOptions).get();
    String expectedKey = TestUtils.addPrefix("test-rename-acl/" + toUpload.getName() + "-RENAME");
    Assert.assertEquals(expectedKey, rename.getObjectKey());
    dest = TestUtils.getUri(_testBucket, rename.getObjectKey(), "");
    AclHandler aclHandler = _client.getAclHandler();

    // validate ACL - authenticated-read (specified in rename op)
    Acl destObjectAcl = aclHandler.getObjectAcl(Utils.getBucketName(dest), Utils.getObjectKey(dest))
      .get();
    Owner destBucketOwner = _client.getBucket(Utils.getBucketName((dest))).get().getOwner();

    Assert.assertEquals(destObjectAcl.getGrants().size(), 2);

    AclGrant fcAclGrant;
    AclGrant readAclGrant;
    if(destObjectAcl.getGrants().get(0).getPermission().toString().equals("FULL_CONTROL"))
    {
      fcAclGrant = destObjectAcl.getGrants().get(0);
      readAclGrant = destObjectAcl.getGrants().get(1);
    }
    else
    {
      fcAclGrant = destObjectAcl.getGrants().get(1);
      readAclGrant = destObjectAcl.getGrants().get(0);
    }

    AclPermission destPerm = fcAclGrant.getPermission();
    Assert.assertEquals(destPerm.toString(), "FULL_CONTROL");
    AclGrantee destGrantee = fcAclGrant.getGrantee();
    Assert.assertEquals(destGrantee.getId(), destBucketOwner.getId());

    destPerm = readAclGrant.getPermission();
    Assert.assertEquals(destPerm.toString(), "READ");
    destGrantee = readAclGrant.getGrantee();
    Assert.assertEquals(destGrantee.getId(),
      "http://acs.amazonaws.com/groups/global/AuthenticatedUsers");
  }


  @Test
  public void testUserMetadata()
    throws Throwable
  {
    // support for metadata manipulation on GCS is limited
    Assume.assumeTrue(!TestUtils.getService().equalsIgnoreCase("gs"));

    // create a small file and upload it
    int fileSize = 100;
    String rootPrefix = TestUtils.addPrefix("test-rename-metadata");
    File toUpload = TestUtils.createTextFile(fileSize);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // rename file
    RenameOptions renameOptions = _client.getOptionsBuilderFactory()
      .newRenameOptionsBuilder()
      .setSourceBucketName(_testBucket)
      .setSourceObjectKey(f.getObjectKey())
      .setDestinationBucketName(_testBucket)
      .setDestinationObjectKey(f.getObjectKey() + "-RENAME")
      .setCannedAcl("authenticated-read")
      .createOptions();
    StoreFile rename = _client.rename(renameOptions).get();
    String expectedKey = TestUtils.addPrefix("test-rename-metadata/" + toUpload.getName() +
      "-RENAME");
    Assert.assertEquals(expectedKey, rename.getObjectKey());
    dest = TestUtils.getUri(_testBucket, rename.getObjectKey(), "");

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
  public void testUserMetadataEncrypted()
    throws Throwable
  {
    // support for metadata manipulation on GCS is limited
    Assume.assumeTrue(!TestUtils.getService().equalsIgnoreCase("gs"));

    // generate new public/private key pair
    File keyDir = TestUtils.createTmpDir(true);
    TestUtils.setKeyProvider(keyDir);
    String keyName = "cloud-store-ut-1";
    TestUtils.createEncryptionKey(keyDir, keyName);
    String rootPrefix = TestUtils.addPrefix("test-rename-metadata");

    // create a small file and upload
    int fileSize = 100;
    File toUpload = TestUtils.createTextFile(fileSize);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, keyName);
    Assert.assertNotNull(f);
    long chunkSize = Utils.getDefaultChunkSize(fileSize);

    // rename file
    RenameOptions renameOptions = _client.getOptionsBuilderFactory()
      .newRenameOptionsBuilder()
      .setSourceBucketName(_testBucket)
      .setSourceObjectKey(f.getObjectKey())
      .setDestinationBucketName(_testBucket)
      .setDestinationObjectKey(f.getObjectKey() + "-RENAME")
      .setCannedAcl("authenticated-read")
      .createOptions();
    StoreFile rename = _client.rename(renameOptions).get();
    String expectedKey = TestUtils.addPrefix("test-rename-metadata/" + toUpload.getName() +
      "-RENAME");
    Assert.assertEquals(expectedKey, rename.getObjectKey());
    dest = TestUtils.getUri(_testBucket, rename.getObjectKey(), "");

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

    PrivateKey privKey = _client.getKeyProvider().getPrivateKey(keyName);
    Cipher cipherRSA = Cipher.getInstance("RSA");
    cipherRSA.init(Cipher.DECRYPT_MODE, privKey);
    String symKeyStr = destUserMeta.get("s3tool-symmetric-key");
    // Make sure we can decrypt symmetric key
    cipherRSA.doFinal(DatatypeConverter.parseBase64Binary(symKeyStr));

    Key pubKey = _client.getKeyProvider().getPublicKey(keyName);
    String pubKeyHash = DatatypeConverter.printBase64Binary(DigestUtils.sha256(pubKey.getEncoded()));
    Assert.assertEquals(destUserMeta.get("s3tool-pubkey-hash"), pubKeyHash.substring(0, 8));
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
    else
    {
      Assert.assertTrue(uex.getMessage().contains(expectedMsg));
    }
  }


}
