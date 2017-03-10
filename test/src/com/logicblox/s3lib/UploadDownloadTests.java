package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;


public class UploadDownloadTests
{
  private static CloudStoreClient _client = null;
  private static String _testBucket = null;


  @BeforeClass
  public static void setUp()
    throws Throwable
  {
    _client = TestOptions.createClient(0);
    _testBucket = TestOptions.createBucket(_client);
  }


  @AfterClass
  public static void tearDown()
    throws Throwable
  {
    TestOptions.destroyBucket(_client, _testBucket);
    TestOptions.destroyClient(_client);
    _testBucket = null;
    _client = null;
  }


  @Test
  public void testSimpleUploadDownload()
    throws Throwable
  {
    // should be no files initially in bucket
    ListOptions lsOpts = new ListOptionsBuilder()
      .setBucket(_testBucket)
      .setRecursive(true)
      .setIncludeVersions(false)
      .setExcludeDirs(false)
      .createListOptions();
    List<S3File> objs = _client.listObjects(lsOpts).get();
    int originalCount = objs.size();

    // create a small file and upload it
    File toUpload = createTextFile(100);
    String rootPrefix = "a/b/";
    URI dest = getUri(toUpload, rootPrefix);
    UploadOptions upOpts = new UploadOptionsBuilder()
      .setFile(toUpload)
      .setBucket(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .createUploadOptions();
    S3File f = _client.upload(upOpts).get();
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = _client.listObjects(lsOpts).get();
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = rootPrefix + toUpload.getName();
    boolean found = false;
    for(S3File o : objs)
    {
      if(o.getKey().equals(key))
      {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);

    // download the file and compare it with the original
    File dlTemp = createTmpFile();
    DownloadOptions dlOpts = new DownloadOptionsBuilder()
      .setFile(dlTemp)
      .setUri(dest)
      .setRecursive(false)
      .setOverwrite(true)
      .createDownloadOptions();
    f = _client.download(dlOpts).get();
    Assert.assertNotNull(f.getLocalFile());
    compareFiles(toUpload, f.getLocalFile());

    // test a few other misc cloud-store functions
    Assert.assertNotNull(_client.exists(dest).get());
    Assert.assertNotNull(_client.exists(_testBucket, key).get());
    Assert.assertEquals(
      dest.toString(), _client.getUri(_testBucket, key).toString());

    Assert.assertNotNull(_client.delete(dest).get());
    Assert.assertNull(_client.exists(dest).get());
    Assert.assertNotNull(_client.delete(_testBucket, key).get());
       // this should succeed even though the file is gone, according to the interface
  }


  @Test
  public void testUploadAttributes()
    throws Throwable
  {
    // create a small file and upload it
    long fileSize = 100;
    File toUpload = createTextFile(fileSize);
    URI dest = getUri(toUpload, "");
    UploadOptions upOpts = new UploadOptionsBuilder()
      .setFile(toUpload)
      .setBucket(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .createUploadOptions();
    S3File f = _client.upload(upOpts).get();
    Assert.assertNotNull(f);
    Assert.assertEquals(
      toUpload.getAbsolutePath(), f.getLocalFile().getAbsolutePath());
    Assert.assertNotNull(f.getETag());
    Assert.assertEquals(toUpload.getName(), f.getKey());
    Assert.assertEquals(_testBucket, f.getBucketName());
    Assert.assertFalse(f.getVersionId().isPresent());
// WHY?
    Assert.assertFalse(f.getSize().isPresent());
// WHY?
    Assert.assertFalse(f.getTimestamp().isPresent());
// WHY?

    ListOptions lsOpts = new ListOptionsBuilder()
      .setBucket(_testBucket)
      .setRecursive(true)
      .setIncludeVersions(false)
      .setExcludeDirs(false)
      .createListOptions();
    List<S3File> objs = _client.listObjects(lsOpts).get();
    for(S3File o : objs)
    {
      if(o.getKey() == toUpload.getName())
      {
        Assert.assertNull(o.getLocalFile());
        Assert.assertEquals("", o.getETag());
// WHY?
        Assert.assertEquals(_testBucket, o.getBucketName());
        Assert.assertFalse(o.getVersionId().isPresent());
// WHY?
        Assert.assertEquals(fileSize, o.getSize().get().longValue());
        Assert.assertFalse(o.getTimestamp().isPresent());
// WHY?
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
    File toUpload = createTextFile(fileSize);
    URI dest = getUri(toUpload, "");
    UploadOptions upOpts = new UploadOptionsBuilder()
      .setFile(toUpload)
      .setBucket(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .createUploadOptions();
    _client.upload(upOpts).get();

    // now download it
    File dlTemp = createTmpFile();
    DownloadOptions dlOpts = new DownloadOptionsBuilder()
      .setFile(dlTemp)
      .setUri(dest)
      .setRecursive(false)
      .setOverwrite(true)
      .createDownloadOptions();
    S3File f = _client.download(dlOpts).get();
    Assert.assertNotNull(f);
    Assert.assertEquals(
      dlTemp.getAbsolutePath(), f.getLocalFile().getAbsolutePath());
    Assert.assertNotNull(f.getETag());
    Assert.assertEquals(toUpload.getName(), f.getKey());
    Assert.assertEquals(_testBucket, f.getBucketName());
    Assert.assertFalse(f.getVersionId().isPresent());
// WHY?
    Assert.assertFalse(f.getSize().isPresent());
// WHY?
    Assert.assertFalse(f.getTimestamp().isPresent());
// WHY?
  }


  private void compareFiles(File f1, File f2)
    throws FileNotFoundException, IOException
  {
    Assert.assertEquals(f1.length(), f2.length());
    FileReader r1 = new FileReader(f1);
    FileReader r2 = new FileReader(f2);
    int c = -1;
    while((c = r1.read()) != -1)
      Assert.assertEquals(c, r2.read());
  }


  private URI getUri(File src, String prefix)
    throws URISyntaxException
  {
    if(!prefix.startsWith("/"))
      prefix = "/" + prefix;
    if(!prefix.endsWith("/"))
      prefix = prefix + "/";
    return new URI(TestOptions.getService() + "://" + _testBucket + prefix + src.getName());
  }


  private File createTextFile(long size)
    throws IOException
  {
    File f = createTmpFile(".txt");
    FileWriter fw = new FileWriter(f);
    int a = 'a';
    for(long i = 0; i < size; ++i)
      fw.append((char) (a + (i % 26)));
    fw.close();
    return f;
  }


  private File createTmpFile()
    throws IOException
  {
    return createTmpFile(null);
  }


  private File createTmpFile(String ext)
    throws IOException
  {
    File f = File.createTempFile("cloud-store-ut", ext);
    f.deleteOnExit();
    return f;
  }


}
