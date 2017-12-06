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

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;


public class MultiKeyTests
{
  private static String _testBucket = null;
  private static CloudStoreClient _client = null;


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
  }


  @Test
  public void testBasicOperation()
    throws Throwable
  {
    // generate 2 new public/private key pairs
    File keydir = TestUtils.createTmpDir(true);
    String[] keys = {"cloud-store-ut-1",
                     "cloud-store-ut-2",
                     "cloud-store-ut-3",
                     "cloud-store-ut-4"};

    for(String key : keys)
      TestUtils.createEncryptionKey(keydir, key);
    TestUtils.setKeyProvider(keydir);

    // capture files currently in test bucket
    String rootPrefix = TestUtils.addPrefix("");
    List<StoreFile> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, keys[0]);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = TestUtils.listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 1, objs.size());
    String objKey = rootPrefix + toUpload.getName();
    Assert.assertTrue(TestUtils.findObject(objs, objKey));

    // download the file and compare it with the original
    File dlTemp = TestUtils.createTmpFile();
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(dlTemp.exists());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
    dlTemp.delete();

    // add the rest of the keys to the file
    for(int i = 1; i < keys.length; i++)
    {
      f = _client.addEncryptionKey(
        TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, keys[i])).get();
      Assert.assertNotNull(f);
    }

    // hide all .pem files
    File hidden = TestUtils.createTmpDir(true);
    for(String key : keys)
    {
      String fn = key + ".pem";
      TestUtils.moveFile(fn, keydir, hidden);
    }

    // dl should fail
    String msg = null;
    dlTemp = TestUtils.createTmpFile();
    try
    {
      TestUtils.downloadFile(dest, dlTemp);
      msg = "Expected download error (key not found)";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("No eligible private key found"));
    }
    Assert.assertNull(msg);
    Assert.assertFalse(dlTemp.exists());

    int[] keyOrder = {2, 0, 3};
    for(int ki : keyOrder)
    {
      // bring back specific .pem file, dl should succeed
      String fn = keys[ki] + ".pem";
      TestUtils.copyFile(fn, hidden, keydir);
      dlTemp = TestUtils.createTmpFile();
      f = TestUtils.downloadFile(dest, dlTemp);
      Assert.assertNotNull(f.getLocalFile());
      Assert.assertTrue(dlTemp.exists());
      Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
      dlTemp.delete();

      // remove key from the file, dl should fail
      f = _client.removeEncryptionKey(
        TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, keys[ki])).get();
      Assert.assertNotNull(f);
      dlTemp = TestUtils.createTmpFile();
      try
      {
        TestUtils.downloadFile(dest, dlTemp);
        msg = "Expected download error (key not found)";
      }
      catch(Throwable t)
      {
        // expected
        if(ki == 3)
        {
          Assert.assertTrue(t.getMessage().contains("is not available to decrypt"));
        }
        else
        {
          Assert.assertTrue(t.getMessage().contains("No eligible private key found"));
        }
      }
      Assert.assertNull(msg);
      Assert.assertFalse(dlTemp.exists());

      Files.delete((new File(keydir, fn)).toPath());
    }

    // use key1, dl should work
    TestUtils.copyFile(keys[1] + ".pem", hidden, keydir);
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(dlTemp.exists());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
    dlTemp.delete();
  }

  @Test
  public void testPartialKeys()
    throws Throwable
  {
    // generate a new public/private key pair
    String key1 = "cloud-store-ut-1";
    String key2 = "cloud-store-ut-2";
    File keydir = TestUtils.createTmpDir(true);
    String[] keys1 = TestUtils.createEncryptionKey(keydir, key1);
    String privateKey1 = keys1[0];
    String publicKey1 = keys1[1];
    String[] keys2 = TestUtils.createEncryptionKey(keydir, key2);
    String privateKey2 = keys2[0];
    String publicKey2 = keys2[1];
    TestUtils.setKeyProvider(keydir);

    String rootPrefix = TestUtils.addPrefix("");

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // the private part of key1 & the public part of key2 should be
    // sufficient to add key2
    String key1FileName = key1 + ".pem";
    TestUtils.writeToFile(privateKey1, new File(keydir, key1FileName));

    String key2FileName = key2 + ".pem";
    TestUtils.writeToFile(publicKey2, new File(keydir, key2FileName));

    String objKey = rootPrefix + toUpload.getName();
    f = _client.addEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, key2))
      .get();
    Assert.assertNotNull(f);

    // try to decrypt with key2's private part
    TestUtils.writeToFile(privateKey2, new File(keydir, key2FileName));
    File hidden = TestUtils.createTmpDir(true);
    TestUtils.moveFile(key1FileName, keydir, hidden);

    File dlTemp = TestUtils.createTmpFile();
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(dlTemp.exists());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
    dlTemp.delete();
  }

  @Test
  public void testDuplicateKey()
    throws Throwable
  {
    // generate public/private key
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    TestUtils.createEncryptionKey(keydir, key1);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // test adding a key that is already used by the file
    String msg = null;
    try
    {
      _client.addEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, key1))
        .get();
      msg = "Expected error adding key";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("already exists"));
    }
    Assert.assertNull(msg);
  }


  @Test
  public void testAddMissingKey()
    throws Throwable
  {
    // generate public/private key
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    String key2 = "cloud-store-ut-2";
    TestUtils.createEncryptionKey(keydir, key1);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // test adding key that doesn't exist
    String msg = null;
    try
    {
      _client.addEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, key2))
        .get();
      msg = "Expected error adding key";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("Missing encryption key"));
    }
    Assert.assertNull(msg);
  }


  @Test
  public void testRemoveMissingKey()
    throws Throwable
  {
    // generate public/private key
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    String key2 = "cloud-store-ut-2";
    TestUtils.createEncryptionKey(keydir, key1);
    TestUtils.createEncryptionKey(keydir, key2);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // test removing key that doesn't exist for encrypted file
    String msg = null;
    try
    {
      _client.removeEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, key2))
        .get();
      msg = "Expected error removing key";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("Cannot remove the last remaining key"));
    }
    Assert.assertNull(msg);
  }


  @Test
  public void testUnencryptedAddKey()
    throws Throwable
  {
    // generate public/private key
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    TestUtils.createEncryptionKey(keydir, key1);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // test add key for unencrypted file
    String msg = null;
    try
    {
      _client.addEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, key1))
        .get();
      msg = "Expected exception";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("Object doesn't seem to be encrypted"));
    }
    Assert.assertNull(msg);
  }


  @Test
  public void testUnencryptedRemoveKey()
    throws Throwable
  {
    // generate public/private key
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    TestUtils.createEncryptionKey(keydir, key1);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // test removing key for unencrypted file
    String msg = null;
    try
    {
      _client.removeEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, key1))
        .get();
      msg = "Expected error removing key";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("Object doesn't seem to be encrypted"));
    }
    Assert.assertNull(msg);
  }


  @Test
  public void testRemoveLastKey()
    throws Throwable
  {
    // generate public/private keys
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    String key2 = "cloud-store-ut-2";
    TestUtils.createEncryptionKey(keydir, key1);
    TestUtils.createEncryptionKey(keydir, key2);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // add the second key
    f = _client.addEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, key2))
      .get();
    Assert.assertNotNull(f);

    // removing first key should be OK
    f = _client.removeEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, key1))
      .get();
    Assert.assertNotNull(f);

    // removing last key should fail
    String msg = null;
    try
    {
      _client.removeEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, key2))
        .get();
      msg = "Expected error removing key";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("Cannot remove the last remaining key"));
    }
    Assert.assertNull(msg);
  }


  @Test
  public void testMaxKeys()
    throws Throwable
  {
    // generate public/private keys
    File keydir = TestUtils.createTmpDir(true);
    int maxKeys = 4;
    int keyCount = maxKeys + 1;
    String[] keys = new String[keyCount];
    for(int i = 0; i < keyCount; ++i)
    {
      keys[i] = "cloud-store-ut-" + i;
      TestUtils.createEncryptionKey(keydir, keys[i]);
    }
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    StoreFile f = TestUtils.uploadEncryptedFile(toUpload, dest, keys[0]);
    Assert.assertNotNull(f);

    // should be OK
    for(int i = 1; i < maxKeys; ++i)
      _client.addEncryptionKey(TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, keys[i]))
        .get();

    // one more should fail
    String msg = null;
    try
    {
      _client.addEncryptionKey(
        TestUtils.buildEncryptionKeyOptions(_testBucket, objKey, keys[maxKeys])).get();
      msg = "Expected exception";
    }
    catch(Throwable t)
    {
      // expected
      Assert.assertTrue(t.getMessage().contains("No more than 4 keys are allowed"));
    }
    Assert.assertNull(msg);
  }

}
