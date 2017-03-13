package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;


public class UploadDownloadTests
  implements OverallProgressListenerFactory, OverallProgressListener
{
  private static CloudStoreClient _client = null;
  private static String _testBucket = null;
  private static String _testBucket2 = null;
  private static Random _rand = null;

  private Set<String> _partSet = new HashSet<String>();


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
    _client = TestOptions.createClient(0);
    _testBucket = TestOptions.createBucket(_client);
    _rand = new Random(System.currentTimeMillis());
  }


  @AfterClass
  public static void tearDown()
    throws Throwable
  {
    TestOptions.destroyBucket(_client, _testBucket);
    TestOptions.destroyBucket(_client, _testBucket2);
    TestOptions.destroyClient(_client);
    _testBucket = null;
    _testBucket2 = null;
    _client = null;
  }


  @Test
  public void testSimpleUploadDownload()
    throws Throwable
  {
    String rootPrefix = "a/b/";
    List<S3File> objs = listObjects(_testBucket, rootPrefix);
    int originalCount = objs.size();

    // create a small file and upload it
    File toUpload = createTextFile(100);
    URI dest = getUri(toUpload, rootPrefix);
    S3File f = uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = listObjects(_testBucket, rootPrefix);
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = rootPrefix + toUpload.getName();
    Assert.assertTrue(findObject(objs, key));

    // download the file and compare it with the original
    File dlTemp = createTmpFile();
    f = downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(compareFiles(toUpload, f.getLocalFile()));

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

    Assert.assertEquals(fileSize, toUpload.length());
    URI dest = getUri(toUpload, "");
    S3File f = uploadFile(toUpload, dest);
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

    List<S3File> objs = listObjects();
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
    uploadFile(toUpload, dest);

    // now download it
    File dlTemp = createTmpFile();
    S3File f = downloadFile(dest, dlTemp);
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


  @Test
  public void testEmptyFile()
    throws Throwable
  {
    List<S3File> objs = listObjects();
    int originalCount = objs.size();

    // upload a file
    File toUpload = createTextFile(0);
    Assert.assertEquals(0, toUpload.length());
    URI dest = getUri(toUpload, "");
    S3File f = uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = listObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = toUpload.getName();
    Assert.assertTrue(findObject(objs, key));

    // download, overwriting a larger file
    File dlTemp = createTextFile(100);
    Assert.assertEquals(100, dlTemp.length());
    f = downloadFile(dest, dlTemp);
    Assert.assertEquals(0, dlTemp.length());
  }


  @Test
  public void testDownloadNoOverwriteFile()
    throws Throwable
  {
    List<S3File> objs = listObjects();
    int originalCount = objs.size();

    // upload a file
    File toUpload = createTextFile(100);
    URI dest = getUri(toUpload, "");
    S3File f = uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = listObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    String key = toUpload.getName();
    Assert.assertTrue(findObject(objs, key));

    // download without overwrite to make sure it fails
    File dlTemp = createTextFile(100);
    Assert.assertFalse(compareFiles(toUpload, dlTemp));
    try
    {
      f = downloadFile(dest, dlTemp, false);
      Assert.fail("Expected download exception");
    }
    catch(Throwable t)
    {
      // expected
    }

    // now download with overwite to make sure it replaces the file
    f = downloadFile(dest, dlTemp, true);
    Assert.assertTrue(compareFiles(toUpload, dlTemp));
  }


  @Test
  public void testDirectoryUploadDownload()
    throws Throwable
  {
    File top = null;
    File dlDir = null;
    File dlDir2 = null;
    try
    {
      // create simple directory structure with a few files
      top = createTmpDir();
      File a = createTextFile(top, 100);
      File b = createTextFile(top, 100);
      File sub = createTmpDir(top);
      File c = createTextFile(sub, 100);
      File d = createTextFile(sub, 100);
      File sub2 = createTmpDir(sub);
      File e = createTextFile(sub2, 100);

      String rootPrefix = "dir-ul-dl/";
      List<S3File> objs = listObjects(_testBucket, rootPrefix);
      int originalCount = objs.size();

      // upload the directory
      URI dest = getUri(top, rootPrefix);
      List<S3File> uploaded = uploadDir(top, dest);
      Assert.assertEquals(5, uploaded.size());

      // verify that the structure was replicated
      objs = listObjects(_testBucket, rootPrefix);
      Assert.assertEquals(originalCount + 5, objs.size());
      String topN = rootPrefix + top.getName() + "/";
      String subN = topN + sub.getName() + "/";
      String sub2N = subN + sub2.getName() + "/";
      Assert.assertTrue(findObject(objs, topN + a.getName()));
      Assert.assertTrue(findObject(objs, topN + b.getName()));
      Assert.assertTrue(findObject(objs, subN + c.getName()));
      Assert.assertTrue(findObject(objs, subN + d.getName()));
      Assert.assertTrue(findObject(objs, sub2N + e.getName()));

      // non-recursive directory download
      dlDir = createTmpDir();
      List<S3File> downloaded = downloadDir(dest, dlDir, false);
      Assert.assertEquals(2, downloaded.size());
      Assert.assertEquals(2, dlDir.list().length);
      Assert.assertTrue(compareFiles(a, new File(dlDir, a.getName())));
      Assert.assertTrue(compareFiles(b, new File(dlDir, b.getName())));

      // recursive directory download
      dlDir2 = createTmpDir();
      downloaded = downloadDir(dest, dlDir2, true);
      Assert.assertEquals(5, downloaded.size());
      Assert.assertEquals(3, dlDir2.list().length);
      Assert.assertTrue(compareFiles(a, new File(dlDir2, a.getName())));
      Assert.assertTrue(compareFiles(b, new File(dlDir2, b.getName())));

      File dlsub = new File(dlDir2, sub.getName());
      Assert.assertTrue(dlsub.exists() && dlsub.isDirectory());
      Assert.assertEquals(3, dlsub.list().length);
      Assert.assertTrue(compareFiles(c, new File(dlsub, c.getName())));
      Assert.assertTrue(compareFiles(d, new File(dlsub, d.getName())));

      File dlsub2 = new File(dlsub, sub2.getName());
      Assert.assertTrue(dlsub2.exists() && dlsub2.isDirectory());
      Assert.assertEquals(1, dlsub2.list().length);
      Assert.assertTrue(compareFiles(e, new File(dlsub2, e.getName())));

    }
    finally
    {
      destroyDir(top);
      destroyDir(dlDir);
      destroyDir(dlDir2);
    }
  }


  @Test
  public void testTinyChunk()
    throws Throwable
  {
    List<S3File> objs = listObjects();
    int originalCount = objs.size();

    // create test file
    int fileSize = 100;
    int chunkSize = 10;
    File toUpload = createTextFile(fileSize);
    URI dest = getUri(toUpload, "");

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
    Assert.assertEquals(fileSize / chunkSize, partCount);
    objs = listObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(findObject(objs, toUpload.getName()));

    // download the file and compare it with the original
    File dlTemp = createTmpFile();
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
    Assert.assertEquals(fileSize / chunkSize, partCount);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(compareFiles(toUpload, f.getLocalFile()));
  }


  @Test
  public void testSimpleCopy()
    throws Throwable
  {
    List<S3File> objs = listObjects();
    int originalCount = objs.size();

    // create test file and upload it
    int fileSize = 100;
    File toUpload = createTextFile(fileSize);
    URI dest = getUri(toUpload, "");
    S3File f = uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = listObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(findObject(objs, toUpload.getName()));

    // copy file
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f.getKey())
       .setDestinationBucketName(_testBucket)
       .setDestinationKey(f.getKey() + "-COPY")
       .createCopyOptions();
    S3File copy = _client.copy(copyOpts).get();

    // check for the copy
    objs = listObjects();
    Assert.assertEquals(originalCount + 2, objs.size());
    Assert.assertTrue(findObject(objs, toUpload.getName()));
    Assert.assertTrue(findObject(objs, toUpload.getName() + "-COPY"));

    // download and compare copy
    File dlTemp = createTmpFile();
    dest = new URI(dest.toString() + "-COPY");
    f = downloadFile(dest, dlTemp);
    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(compareFiles(toUpload, f.getLocalFile()));
  }


  @Test
  public void testCrossBucketCopy()
    throws Throwable
  {
    List<S3File> objs = listObjects();
    int originalCount = objs.size();

    // create test file and upload it
    int fileSize = 100;
    File toUpload = createTextFile(fileSize);
    URI dest = getUri(toUpload, "");
    S3File f = uploadFile(toUpload, dest);
    Assert.assertNotNull(f);

    // make sure file was uploaded
    objs = listObjects();
    Assert.assertEquals(originalCount + 1, objs.size());
    Assert.assertTrue(findObject(objs, toUpload.getName()));

    // copy file
    String bucket2 = createTestBucket2();
    CopyOptions copyOpts = new CopyOptionsBuilder()
       .setSourceBucketName(_testBucket)
       .setSourceKey(f.getKey())
       .setDestinationBucketName(bucket2)
       .setDestinationKey(f.getKey())
       .createCopyOptions();
    S3File copy = _client.copy(copyOpts).get();

    // check for the copy in 1st bucket, should be the same
    List<S3File> copyObjs = listObjects();
    Assert.assertEquals(objs.size(), copyObjs.size());
    for(S3File sf : copyObjs)
      Assert.assertTrue(findObject(objs, sf.getKey()));

    // check for the copy in 2nd bucket
    copyObjs = listObjects(bucket2);
    Assert.assertEquals(1, copyObjs.size());
    Assert.assertTrue(findObject(copyObjs, toUpload.getName()));

    // download and compare copy
    File dlTemp = createTmpFile();
    URI src = new URI(TestOptions.getService() + "://" + bucket2 + "/"
                      + toUpload.getName());
    DownloadOptions dlOpts = new DownloadOptionsBuilder()
      .setFile(dlTemp)
      .setUri(src)
      .setRecursive(false)
      .setOverwrite(true)
      .createDownloadOptions();
    f = _client.download(dlOpts).get();

    Assert.assertNotNull(f.getLocalFile());
    Assert.assertTrue(compareFiles(toUpload, f.getLocalFile()));
  }


  @Test
  public void testCopyDir()
    throws Throwable
  {
    File top = null;
    File dlDir = null;
    File dlDir2 = null;
    try
    {
      // create simple directory structure with a few files
      top = createTmpDir();
      File a = createTextFile(top, 100);
      File b = createTextFile(top, 100);
      File sub = createTmpDir(top);
      File c = createTextFile(sub, 100);
      File d = createTextFile(sub, 100);
      File sub2 = createTmpDir(sub);
      File e = createTextFile(sub2, 100);

      String rootPrefix = "copy-dir/";
      List<S3File> objs = listObjects(_testBucket, rootPrefix);
      int originalCount = objs.size();

      // upload the directory
      URI dest = getUri(top, rootPrefix);
      List<S3File> uploaded = uploadDir(top, dest);
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
      List<S3File> copyObjs = listObjects(_testBucket, rootPrefix);
      int currentSize = copyObjs.size();
      Assert.assertEquals(originalCount + 5 + 2, currentSize);
          // original files plus 5 uploaded plus 2 copied
      Assert.assertTrue(findObject(copyObjs, copyTopN + a.getName()));
      Assert.assertTrue(findObject(copyObjs, copyTopN + b.getName()));

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
      copyObjs = listObjects(_testBucket, rootPrefix);
      int lastSize = copyObjs.size();
      Assert.assertEquals(currentSize + 5, lastSize);
         // previous size plus 5 copies
      Assert.assertTrue(findObject(copyObjs, copyTopN2 + a.getName()));
      Assert.assertTrue(findObject(copyObjs, copyTopN2 + b.getName()));
      Assert.assertTrue(findObject(copyObjs, subN + c.getName()));
      Assert.assertTrue(findObject(copyObjs, subN + d.getName()));
      Assert.assertTrue(findObject(copyObjs, sub2N + e.getName()));
    }
    finally
    {
      destroyDir(top);
      destroyDir(dlDir);
      destroyDir(dlDir2);
    }
  }

  @Test
  public void testCrossBucketCopyDir()
    throws Throwable
  {
    File top = null;
    File dlDir = null;
    File dlDir2 = null;
    try
    {
      // create simple directory structure with a few files
      top = createTmpDir();
      File a = createTextFile(top, 100);
      File b = createTextFile(top, 100);
      File sub = createTmpDir(top);
      File c = createTextFile(sub, 100);
      File d = createTextFile(sub, 100);
      File sub2 = createTmpDir(sub);
      File e = createTextFile(sub2, 100);

      String rootPrefix = "copy-dir-bucket/";
      List<S3File> objs = listObjects(_testBucket, rootPrefix);
      int originalCount = objs.size();

      // upload the directory
      URI dest = getUri(top, rootPrefix);
      List<S3File> uploaded = uploadDir(top, dest);
      Assert.assertEquals(5, uploaded.size());

      // non-recursive copy
      String bucket2 = createTestBucket2();
      List<S3File> objs2 = listObjects(bucket2, rootPrefix);
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
      objs = listObjects(_testBucket, rootPrefix);
      Assert.assertEquals(originalCount + 5, objs.size());
         // original files plus 5 uploaded

      List<S3File> copyObjs = listObjects(bucket2, rootPrefix);
      Assert.assertEquals(originalCount2 + 2, copyObjs.size());
         // original files plus 2 copied
      Assert.assertTrue(findObject(copyObjs, topN + a.getName()));
      Assert.assertTrue(findObject(copyObjs, topN + b.getName()));

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
      objs = listObjects(_testBucket, rootPrefix);
      Assert.assertEquals(originalCount + 5, objs.size());
         // same original plus 5 uploaded
      int lastSize = copyObjs.size();
      copyObjs = listObjects(bucket2, rootPrefix);
      Assert.assertEquals(lastSize + 5, copyObjs.size());
         // previous size plus 5 copies
      Assert.assertTrue(findObject(copyObjs, copyTopN + a.getName()));
      Assert.assertTrue(findObject(copyObjs, copyTopN + b.getName()));
      Assert.assertTrue(findObject(copyObjs, subN + c.getName()));
      Assert.assertTrue(findObject(copyObjs, subN + d.getName()));
      Assert.assertTrue(findObject(copyObjs, sub2N + e.getName()));
    }
    finally
    {
      destroyDir(top);
      destroyDir(dlDir);
      destroyDir(dlDir2);
    }
  }


  private boolean findObject(List<S3File> objs, String key)
  {
    for(S3File o : objs)
    {
      if(o.getKey().equals(key))
        return true;
    }
    return false;
  }


  private S3File uploadFile(File src, URI dest)
    throws Throwable
  {
    UploadOptions upOpts = new UploadOptionsBuilder()
      .setFile(src)
      .setBucket(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .createUploadOptions();
    return _client.upload(upOpts).get();
  }


  private List<S3File> uploadDir(File src, URI dest)
    throws Throwable
  {
    UploadOptions upOpts = new UploadOptionsBuilder()
      .setFile(src)
      .setBucket(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .createUploadOptions();
    return _client.uploadDirectory(upOpts).get();
  }


  private S3File downloadFile(URI src, File dest)
    throws Throwable
  {
    return downloadFile(src, dest, true);
  }


  private S3File downloadFile(URI src, File dest, boolean overwrite)
    throws Throwable
  {
    DownloadOptions dlOpts = new DownloadOptionsBuilder()
      .setFile(dest)
      .setUri(src)
      .setRecursive(false)
      .setOverwrite(overwrite)
      .createDownloadOptions();
    return _client.download(dlOpts).get();
  }


  private List<S3File> downloadDir(URI src, File dest, boolean recursive)
    throws Throwable
  {
    DownloadOptions dlOpts = new DownloadOptionsBuilder()
      .setFile(dest)
      .setUri(src)
      .setRecursive(recursive)
      .setOverwrite(true)
      .createDownloadOptions();
    return _client.downloadDirectory(dlOpts).get();
  }


  private List<S3File> listObjects()
    throws Throwable
  {
    return listObjects(_testBucket);
  }


  private List<S3File> listObjects(String bucket)
    throws Throwable
  {
    ListOptions lsOpts = new ListOptionsBuilder()
      .setBucket(bucket)
      .setRecursive(true)
      .setIncludeVersions(false)
      .setExcludeDirs(false)
      .createListOptions();
    return _client.listObjects(lsOpts).get();
  }


  private List<S3File> listObjects(String bucket, String key)
    throws Throwable
  {
    ListOptions lsOpts = new ListOptionsBuilder()
      .setBucket(bucket)
      .setRecursive(true)
      .setIncludeVersions(false)
      .setExcludeDirs(false)
      .setObjectKey(key)
      .createListOptions();
    return _client.listObjects(lsOpts).get();
  }


  private boolean compareFiles(File f1, File f2)
    throws FileNotFoundException, IOException
  {
    if(f1.length() != f2.length())
      return false;
    FileReader r1 = new FileReader(f1);
    FileReader r2 = new FileReader(f2);
    int c = -1;
    while((c = r1.read()) != -1)
    {
      if(c != r2.read())
        return false;
    }
    return true;
  }


  private URI getUri(File src, String prefix)
    throws URISyntaxException
  {
    if(!prefix.startsWith("/"))
      prefix = "/" + prefix;
    if(!prefix.endsWith("/"))
      prefix = prefix + "/";
    String end = "";
    if(src.isDirectory())
      end = "/";
    return new URI(TestOptions.getService() + "://" + _testBucket + prefix + src.getName() + end);
  }


  private File createTextFile(long size)
    throws IOException
  {
    return createTextFile(null, size);
  }


  private File createTextFile(File parent, long size)
    throws IOException
  {
    File f = createTmpFile(parent, ".txt");
    FileWriter fw = new FileWriter(f);
    int start = _rand.nextInt(26) + (int) 'a';
    for(long i = 0; i < size; ++i)
      fw.append((char) (((start + i) % 26) + 'a'));
    fw.close();
    return f;
  }


  private File createTmpFile()
    throws IOException
  {
    return createTmpFile(null, null);
  }


  private File createTmpFile(File parent, String ext)
    throws IOException
  {
    File f = File.createTempFile("cloud-store-ut", ext, parent);
    f.deleteOnExit();
    return f;
  }


  private File createTmpDir()
    throws IOException
  {
    return Files.createTempDirectory("cloud-store-ut").toFile();
  }


  private File createTmpDir(File parent)
    throws IOException
  {
    return Files.createTempDirectory(parent.toPath(), "cloud-store-ut").toFile();
  }


  private void destroyDir(File dirF)
    throws IOException
  {
    if((null == dirF) || !dirF.exists())
      return;

    Path directory = dirF.toPath();
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() 
    {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) 
        throws IOException 
      {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) 
        throws IOException
      {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }


  private synchronized int getPartCount()
  {
    return _partSet.size();
  }


  private synchronized void clearParts()
  {
    _partSet.clear();
  }


  private String createTestBucket2()
  {
    if(null == _testBucket2)
      _testBucket2 = TestOptions.createBucket(_client);
    return _testBucket2;
  }

}
