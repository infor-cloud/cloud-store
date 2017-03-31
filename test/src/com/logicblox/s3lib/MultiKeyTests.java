package com.logicblox.s3lib;

import java.io.File;
import java.net.URI;
import java.util.List;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


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


  private void createKey(String keyName, File keyDir)
    throws Throwable
  {
    KeyGenCommand kgc = new KeyGenCommand("RSA", 2048);
    File kf = new File(keyDir, keyName + ".pem");
    kgc.savePemKeypair(kf);
  }
  

  @Test
  public void testBasicOperation()
    throws Throwable
  {
    // generate 2 new public/private key pairs
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    String key2 = "cloud-store-ut-2";
    createKey(key1, keydir);
    createKey(key2, keydir);
    TestUtils.setKeyProvider(keydir);

    // capture files currently in test bucket
    String rootPrefix = TestUtils.addPrefix("");
    List<S3File> objs = TestUtils.listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
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

    // add a second key to the file
    f = _client.addEncryptionKey(_testBucket, objKey, key2).get();
    Assert.assertNotNull(f);

    // hide key1 and key2, download should fail
    File hidden = TestUtils.createTmpDir(true);
    String fn1 = key1 + ".pem";
    String fn2 = key2 + ".pem";
    TestUtils.moveFile(fn1, keydir, hidden);
    TestUtils.moveFile(fn2, keydir, hidden);
    try
    {
      TestUtils.downloadFile(dest, dlTemp);
      Assert.fail("Expected download error (key not found)");
    }
    catch(Throwable t)
    {
      // expected
    }
    Assert.assertFalse(dlTemp.exists());

    // replace key2, download should work
    TestUtils.moveFile(fn2, hidden, keydir);
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(dlTemp.exists());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
    dlTemp.delete();

    // hide key2 and replace key1, dl should work
    TestUtils.moveFile(fn1, hidden, keydir);
    TestUtils.moveFile(fn2, keydir, hidden);    
    f = TestUtils.downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(dlTemp.exists());
    Assert.assertTrue(TestUtils.compareFiles(toUpload, f.getLocalFile()));
    dlTemp.delete();

    // remove key1 from the file, dl should fail
    f = _client.removeEncryptionKey(_testBucket, objKey, key1).get();
    Assert.assertNotNull(f);
    try
    {
      TestUtils.downloadFile(dest, dlTemp);
      Assert.fail("Expected download error (key not found)");
    }
    catch(Throwable t)
    {
      // expected
    }
    Assert.assertFalse(dlTemp.exists());

    // replace key2, dl should work
    TestUtils.moveFile(fn2, hidden, keydir);
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
    createKey(key1, keydir);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // test adding a key that is already used by the file
    try
    {
      _client.addEncryptionKey(_testBucket, objKey, key1).get();
      Assert.fail("Expected error adding key");
    }
    catch(Throwable t)
    {
      // expected
    }

  }


  @Test
  public void testAddMissingKey()
    throws Throwable
  {
    // generate public/private key
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    String key2 = "cloud-store-ut-2";
    createKey(key1, keydir);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // test adding key that doesn't exist
    try
    {
      _client.addEncryptionKey(_testBucket, objKey, key2).get();
      Assert.fail("Expected error adding key");
    }
    catch(Throwable t)
    {
      // expected
    }
  }


  @Test
  public void testRemoveMissingKey()
    throws Throwable
  {
    // generate public/private key
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    String key2 = "cloud-store-ut-2";
    createKey(key1, keydir);
    createKey(key2, keydir);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // test removing key that doesn't exist for encrypted file
    try
    {
      _client.removeEncryptionKey(_testBucket, objKey, key2).get();
      Assert.fail("Expected error removing key");
    }
    catch(Throwable t)
    {
      // expected
    }
  }


  @Test
  public void testUnencryptedAddKey()
    throws Throwable
  {
    // generate public/private key
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    createKey(key1, keydir);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // test add key for unencrypted file
    try
    {
      _client.addEncryptionKey(_testBucket, objKey, key1).get();
      Assert.fail("Expected exception");
    }
    catch(Throwable t)
    {
      // expected
    }
  }


  @Test
  public void testUnencryptedRemoveKey()
    throws Throwable
  {
    // generate public/private key
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    createKey(key1, keydir);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // test removing key for unencrypted file
    try
    {
      _client.removeEncryptionKey(_testBucket, objKey, key1).get();
      Assert.fail("Expected error removing key");
    }
    catch(Throwable t)
    {
      // expected
    }
  }


  @Test
  public void testRemoveLastKey()
    throws Throwable
  {
    // generate public/private keys
    File keydir = TestUtils.createTmpDir(true);
    String key1 = "cloud-store-ut-1";
    String key2 = "cloud-store-ut-2";
    createKey(key1, keydir);
    createKey(key2, keydir);
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadEncryptedFile(toUpload, dest, key1);
    Assert.assertNotNull(f);

    // add the second key
    f = _client.addEncryptionKey(_testBucket, objKey, key2).get();
    Assert.assertNotNull(f);

    // removing first key should be OK
    f = _client.removeEncryptionKey(_testBucket, objKey, key1).get();
    Assert.assertNotNull(f);

    // removing last key should fail
    try
    {
      _client.removeEncryptionKey(_testBucket, objKey, key2).get();
      Assert.fail("Expected error removing key");
    }
    catch(Throwable t)
    {
      // expected
    }
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
      createKey(keys[i], keydir);
    }
    TestUtils.setKeyProvider(keydir);

    // create a small file and upload
    File toUpload = TestUtils.createTextFile(100);
    String rootPrefix = TestUtils.addPrefix("");
    String objKey = rootPrefix + toUpload.getName();
    URI dest = TestUtils.getUri(_testBucket, toUpload, rootPrefix);
    S3File f = TestUtils.uploadEncryptedFile(toUpload, dest, keys[0]);
    Assert.assertNotNull(f);

    // should be OK
    for(int i = 1; i < maxKeys; ++i)
      _client.addEncryptionKey(_testBucket, objKey, keys[i]).get();

    // one more should fail
    try
    {
      _client.addEncryptionKey(_testBucket, objKey, keys[maxKeys]).get();
      Assert.fail("Expected exception");
    }
    catch(Throwable t)
    {
      // expected
    }
  }


}
