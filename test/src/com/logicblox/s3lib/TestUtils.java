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

package com.logicblox.s3lib;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.Class;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.InterruptedException;
import java.net.URI;
import java.net.URL;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.ExecutionException;


public class TestUtils
{
  private static String _service = "s3";
  private static String _endpoint = null;
  private static URI _destUri = null;
  private static CloudStoreClient _client = null;
  private static String _prefix = null;
  private static String _testBucket = null;
  private static int _defaultRetryCount = 0;
  private static Random _rand = null;
  private static Set<File> _autoDeleteDirs = new HashSet<File>();
  private static Set<String> _bucketsToDestroy = new HashSet<String>();

  static boolean SKIP_CLEANUP = false;
  static final int RETRY_COUNT = 5;


  // handle all input parameters
  public static void parseArgs(String[] args)
  {
    String destPrefix = null;
    for(int i = 0; i < args.length; ++i)
    {
      if(args[i].equals("--help") || args[i].equals("-h"))
      {
        usage();
        System.exit(0);
      }
      else if(args[i].equals("--service"))
      {
        ++i;
        _service = args[i];
      }
      else if(args[i].equals("--endpoint"))
      {
        ++i;
        _endpoint = args[i];
      }
      else if(args[i].equals("--dest-prefix"))
      {
        ++i;
        destPrefix = args[i];
      }
      else if(args[i].equals("--keydir"))
      {
        ++i;
        Utils.setDefaultKeyDir(args[i]);
      }
      else
      {
        System.out.println("Error:  '" + args[i] + "' unexpected");
        usage();
        System.exit(1);
      }
    }

    if(null != destPrefix)
    {
      try
      {
        setDest(destPrefix);
      }
      catch(Throwable t)
      {
        System.out.println("Error: " + t.getMessage());
        System.exit(1);
      }
    }

    if(!_service.equals("s3") && !_service.equals("gs"))
    {
      System.out.println("Error:  --service must be s3 or gs");
      System.exit(1);
    }

  }


  public static void setUp()
    throws Throwable
  {
    _client = createClient(_defaultRetryCount);
    URI destUri = getDestUri();
    if(null == destUri)
    {
      _testBucket = createTestBucket();
    }
    else
    {
      _testBucket = Utils.getBucket(destUri);
      _prefix = Utils.getObjectKey(destUri);
      if(!_prefix.endsWith("/"))
        _prefix = _prefix + "/";
    }
  }

  public static void tearDown()
    throws Throwable
  {
    destroyDirs();

    if(null != _prefix)
      clearBucket(_testBucket, _prefix);

    destroyBuckets();
    _testBucket = null;

    destroyClient(_client);
    _client = null;
  }
  

  public static CloudStoreClient getClient()
  {
    return _client;
  }


  public static String getTestBucket()
  {
    return _testBucket;
  }


  public static String getPrefix()
  {
    return _prefix;
  }
  
  
  public static void resetRetryCount()
  {
    _client.setRetryCount(_defaultRetryCount);
  }
  

  // return URL representing the endpoint used to connect to storage 
  // service, or null if not passed as a command line parameter
  public static URL getEndpoint()
    throws MalformedURLException
  {
    if(null == _endpoint)
      return null;
    else
      return new URL(_endpoint);
  }


  // return String representing the endpoint used to connect to storage 
  // service, or null if not passed as a command line parameter
  public static String getEndpointString()
    throws MalformedURLException
  {
    if(null == _endpoint)
      return null;
    else
      return getEndpoint().toString();
  }


  // return the name of the storage service (currently either s3 or gs)
  public static String getService()
  {
    return _service;
  }


  public static URI getDestUri()
  {
    return _destUri;
  }


  // return a string that describes the storage service used for the tests
  public static String getServiceDescription()
  {
    String ep = _endpoint;
    if((null == ep) && _service.equals("s3"))
      ep = "AWS";
    else if((null == ep) && _service.equals("gs"))
      ep = "GCS";
    String svc =_service + " (" + ep + ")";
    if(null != _destUri)
       svc = svc + ", in " + _destUri;
    return svc;
  }


  // create a new CloudStoreClient from service name and endpoint parameters.
  // use default retry count
  public static CloudStoreClient createClient()
    throws MalformedURLException, URISyntaxException, GeneralSecurityException,
           IOException
  {
    return Utils.createCloudStoreClient(getService(), getEndpointString());
  }


  // create a new CloudStoreClient from service name and endpoint parameters.
  // use specified retry count
  public static CloudStoreClient createClient(int retryCount)
    throws MalformedURLException, URISyntaxException, GeneralSecurityException,
           IOException
  {
    CloudStoreClient client =
      Utils.createCloudStoreClient(getService(), getEndpointString());
    client.setRetryCount(retryCount);
    return client;
  }


  // shutdown the CloudStoreClient
  public static void destroyClient(CloudStoreClient client)
  {
    if(null != client)
      client.shutdown();
  }


  // create a new unique bucket to be used for test cases.  this will fail
  // if it cannot create a new bucket in 1000 tries.  returns the name of
  // the bucket.
  public static String createBucket()
  {
    // give up if we can't find a unique bucket name in 1000 tries....
    Throwable lastException = null;
    for(int b = 0; b < 1000; ++b)
    {
      String bucket = "cloud-store-ut-bucket-" + System.currentTimeMillis() + "-" + b;
      try
      {
        _client.createBucket(bucket);
        return bucket;
      }
      catch(Throwable t)
      {
        // ignore
        lastException = t;
      }
    }
    throw new RuntimeException(
      "failed to create a test bucket: " + lastException.getMessage(), 
      lastException);
  }


  // destroy the bucket created by testing.  first have to delete any
  // objects that still exist in the bucket
  public static void destroyBucket(String bucket)
    throws InterruptedException, ExecutionException
  {
    if(SKIP_CLEANUP || (null == bucket) || bucket.isEmpty())
      return;

    clearBucket(bucket, null);
    _client.destroyBucket(bucket);
  }


  public static void clearBucket(String bucket, String prefix)
    throws InterruptedException, ExecutionException
  {
    if(SKIP_CLEANUP)
      return;
      
    ListOptionsBuilder builder = _client.getOptionsBuilderFactory()
      .newListOptionsBuilder()
      .setBucketName(bucket)
      .setRecursive(true)
      .setIncludeVersions(false)
      .setExcludeDirs(false);
    if((null != prefix) && !prefix.isEmpty())
      builder.setObjectKey(prefix);
    ListOptions lsOpts = builder.createOptions();
    List<StoreFile> objs = _client.listObjects(lsOpts).get();
    for(StoreFile f : objs)
      deleteObject(bucket, f.getKey());
  }

  public static StoreFile deleteObject(String bucket, String key)
    throws InterruptedException, ExecutionException
  {
    DeleteOptions opts = _client.getOptionsBuilderFactory()
      .newDeleteOptionsBuilder()
      .setBucketName(bucket)
      .setObjectKey(key)
      .createOptions();
    return _client.delete(opts).get();
  }

  public static StoreFile deleteObject(URI uri)
    throws InterruptedException, ExecutionException
  {
    DeleteOptions opts = _client.getOptionsBuilderFactory()
      .newDeleteOptionsBuilder()
      .setBucketName(Utils.getBucket(uri))
      .setObjectKey(Utils.getObjectKey(uri))
      .createOptions();
    return _client.delete(opts).get();
  }
  
  public static boolean findObject(List<StoreFile> objs, String key)
  {
    for(StoreFile o : objs)
    {
      if(o.getKey().equals(key))
        return true;
    }
    return false;
  }

  public static Metadata objectExists(String bucket, String key)
    throws ExecutionException, InterruptedException
  {
    ExistsOptions opts = _client.getOptionsBuilderFactory()
      .newExistsOptionsBuilder()
      .setBucketName(bucket)
      .setObjectKey(key)
      .createOptions();

    return _client.exists(opts).get();
  }


  public static StoreFile updateObjectUserMetadata(String bucket, String key,
                                                Map<String,String> userMetadata)
    throws InterruptedException, ExecutionException
  {
    CopyOptions options = _client.getOptionsBuilderFactory()
      .newCopyOptionsBuilder()
      .setSourceBucketName(bucket)
      .setSourceObjectKey(key)
      .setDestinationBucketName(bucket)
      .setDestinationObjectKey(key)
      // .setS3Acl(acl)
      .setUserMetadata(userMetadata)
      .createOptions();

    return _client.copy(options).get();
  }


  public static StoreFile uploadFile(File src, URI dest)
    throws Throwable
  {
    UploadOptions upOpts = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(src)
      .setBucketName(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .createOptions();
    return _client.upload(upOpts).get();
  }


  public static StoreFile uploadEncryptedFile(File src, URI dest, String keyName)
      throws Throwable
  {
    UploadOptions upOpts = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(src)
      .setBucketName(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .setEncKey(keyName)
      .createOptions();
    return _client.upload(upOpts).get();
  }


  public static List<StoreFile> uploadDir(File src, URI dest)
    throws Throwable
  {
    UploadOptions upOpts = _client.getOptionsBuilderFactory()
      .newUploadOptionsBuilder()
      .setFile(src)
      .setBucketName(Utils.getBucket(dest))
      .setObjectKey(Utils.getObjectKey(dest))
      .createOptions();
    return _client.uploadDirectory(upOpts).get();
  }


  public static StoreFile downloadFile(URI src, File dest)
    throws Throwable
  {
    return downloadFile(src, dest, true);
  }


  public static StoreFile downloadFile(URI src, File dest, boolean overwrite)
      throws Throwable
  {

    DownloadOptions dlOpts = _client.getOptionsBuilderFactory()
      .newDownloadOptionsBuilder()
      .setFile(dest)
      .setBucketName(Utils.getBucket(src))
      .setObjectKey(Utils.getObjectKey(src))
      .setRecursive(false)
      .setOverwrite(overwrite)
      .createOptions();
    return _client.download(dlOpts).get();
  }


  public static List<StoreFile> downloadDir(URI src, File dest, boolean recursive)
      throws Throwable
  {
    DownloadOptions dlOpts = _client.getOptionsBuilderFactory()
      .newDownloadOptionsBuilder()
      .setFile(dest)
      .setBucketName(Utils.getBucket(src))
      .setObjectKey(Utils.getObjectKey(src))
      .setRecursive(recursive)
      .setOverwrite(true)
      .createOptions();
    return _client.downloadDirectory(dlOpts).get();
  }


  public static List<StoreFile> downloadDir(URI src, File dest, boolean recursive, boolean overwrite)
      throws Throwable
  {
    DownloadOptions dlOpts = _client.getOptionsBuilderFactory()
      .newDownloadOptionsBuilder()
      .setFile(dest)
      .setBucketName(Utils.getBucket(src))
      .setObjectKey(Utils.getObjectKey(src))
      .setRecursive(recursive)
      .setOverwrite(overwrite)
      .createOptions();
    return _client.downloadDirectory(dlOpts).get();
  }


  public static List<StoreFile> listTestBucketObjects()
    throws Throwable
  {
    return listObjects(_testBucket, _prefix);
  }

  
  public static List<StoreFile> listObjects(String bucket)
    throws Throwable
  {
    return listObjects(bucket, null);
  }


  public static List<StoreFile> listObjects(String bucket, String key)
    throws Throwable
  {
    ListOptionsBuilder builder = _client.getOptionsBuilderFactory()
      .newListOptionsBuilder()
      .setBucketName(bucket)
      .setRecursive(true)
      .setIncludeVersions(false)
      .setExcludeDirs(false);
    if(null != key)
      builder.setObjectKey(key);
    ListOptions lsOpts = builder.createOptions();
    return _client.listObjects(lsOpts).get();
  }

  
  public static boolean compareFiles(File f1, File f2)
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


  public static URI getUri(String bucket, File src, String prefix)
    throws URISyntaxException
  {
    if(!prefix.startsWith("/"))
      prefix = "/" + prefix;
    if(!prefix.endsWith("/"))
      prefix = prefix + "/";
    String end = "";
    if(src.isDirectory())
      end = "/";
    return new URI(TestUtils.getService() + "://" + bucket + prefix + src.getName() + end);
  }


  public static URI getUri(String bucket, String filePath, String prefix)
    throws URISyntaxException
  {
    if(!prefix.startsWith("/"))
      prefix = "/" + prefix;
    if(!prefix.endsWith("/"))
      prefix = prefix + "/";
    String end = "";
    return new URI(TestUtils.getService() + "://" + bucket + prefix + filePath + end);
  }


  // GCS implementation currently doesn't support multipart uploads
  public static boolean supportsMultiPart()
  {
    return !getService().equalsIgnoreCase("gs");
  }


  public static int getExpectedPartCount(int fileSize, int chunkSize)
  {
    if(supportsMultiPart())
      return (int) Math.ceil(((double) fileSize) / chunkSize);
    else
      return 1;
  }


  public static File createTextFile(long size)
    throws IOException
  {
    return createTextFile(null, size);
  }


  public static File createTextFile(File parent, long size)
    throws IOException
  {
    File f = createTmpFile(parent, ".txt");
    FileWriter fw = new FileWriter(f);
    int start = getRand().nextInt(26) + (int) 'a';
    for(long i = 0; i < size; ++i)
      fw.append((char) (((start + i) % 26) + 'a'));
    fw.close();
    return f;
  }


  public static File createTmpFile()
    throws IOException
  {
    return createTmpFile(null, null);
  }


  public static File createTmpFile(File parent, String ext)
    throws IOException
  {
    File f = File.createTempFile("cloud-store-ut", ext, parent);
    f.deleteOnExit();
    return f;
  }


  public static File createTmpDir()
    throws IOException
  {
    return createTmpDir(false);
  }


  public static File createTmpDir(boolean autoDelete)
    throws IOException
  {
    File tmp = Files.createTempDirectory("cloud-store-ut").toFile();
    if(autoDelete)
      _autoDeleteDirs.add(tmp);
    return tmp;
  }


  public static File createTmpDir(File parent)
    throws IOException
  {
    return createTmpDir(parent, false);
  }


  public static File createTmpDir(File parent, boolean autoDelete)
    throws IOException
  {
    File tmp = Files.createTempDirectory(parent.toPath(), "cloud-store-ut").toFile();
    if(autoDelete)
      _autoDeleteDirs.add(tmp);
    return tmp;
  }


  public static void destroyDirs()
    throws IOException
  {
    for(File dir : _autoDeleteDirs)
      destroyDir(dir);
    _autoDeleteDirs.clear();
  }  


  public static void destroyDir(File dirF)
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


  public static String createTestBucket()
  {
    return createTestBucket(true);
  }

  
  public static String createTestBucket(boolean destroy)
  {
    String bucket = createBucket();
    if(destroy)
      _bucketsToDestroy.add(bucket);
    return bucket;
  }


  public static void destroyBuckets()
    throws InterruptedException, ExecutionException
  {
    for(String bucket : _bucketsToDestroy)
       destroyBucket(bucket);
    _bucketsToDestroy.clear();
  }


  public static void setKeyProvider(File keydir)
    throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
  {
    Class cls = _client.getClass();
    Method meth = cls.getDeclaredMethod("setKeyProvider", KeyProvider.class);
    meth.invoke(_client, Utils.createKeyProvider(keydir.getAbsolutePath()));
  }

  public static EncryptionKeyOptions buildEncryptionKeyOptions(
    String bucket, String objectKey, String encryptionKey)
  {
    EncryptionKeyOptions options = _client.getOptionsBuilderFactory()
      .newEncryptionKeyOptionsBuilder()
      .setBucketName(bucket)
      .setObjectKey(objectKey)
      .setEncryptionKey(encryptionKey)
      .createOptions();

    return options;
  }


  public static void writeToFile(String data, File f)
    throws IOException
  {
    if(f.exists())
      f.delete();
    FileWriter fw = new FileWriter(f);
    fw.write(data);
    fw.close();
  }
  

  public static String[] parsePem(File keyfile)
    throws Throwable
  {
    final String privateBegin = "-----BEGIN PRIVATE KEY-----";
    final String privateEnd = "-----END PRIVATE KEY-----";
    final String publicBegin = "-----BEGIN PUBLIC KEY-----";
    final String publicEnd = "-----END PUBLIC KEY-----";

    BufferedReader r = new BufferedReader(new FileReader(keyfile));
    String line = null;
    boolean inPrivateSection = false;
    boolean inPublicSection = false;
    StringBuilder privateKey = new StringBuilder();
    StringBuilder publicKey = new StringBuilder();
    while((line = r.readLine()) != null)
    {
      if(inPrivateSection)
      {
        privateKey.append(line).append("\n");
        inPrivateSection = !line.equals(privateEnd);
      }
      else if(inPublicSection)
      {
        publicKey.append(line).append("\n");
        inPublicSection = !line.equals(publicEnd);
      }
      else
      {
        inPrivateSection = line.equals(privateBegin);
        if(inPrivateSection)
          privateKey.append(line).append("\n");
        inPublicSection = line.equals(publicBegin);
        if(inPublicSection)
          publicKey.append(line).append("\n");
      }
    }
    r.close();

    String[] keys = new String[2];
    keys[0] = privateKey.toString();
    keys[1] = publicKey.toString();
    return keys;
  }

  
  // add _prefix to this if it is defined to create files within a
  // specific location in a pre-existing bucket.  assumes _prefix ends
  // with a / and the path passed in does not start with a /
  public static String addPrefix(String path)
  {
    if(null == _prefix)
      return path;
    else
      return _prefix + path;
  }


  /**
   * Create a new .pem key file in the specified directory that contains a new
   * RSA public/private key pair.  Returns a String array of length two, the
   * first string containing the private key and the second holding the public
   * key.
   */
  public static String[] createEncryptionKey(File keydir, String keyName)
    throws Throwable
  {
    KeyGenCommand kgc = new KeyGenCommand("RSA", 2048);
    File keyfile = new File(keydir, keyName + ".pem");
    kgc.savePemKeypair(keyfile);
    String[] keys = parsePem(keyfile);
    return keys;
  }


  public static void moveFile(String fname, File srcDir, File destDir)
  {
    File src = new File(srcDir, fname);
    File dest = new File(destDir, fname);
    src.renameTo(dest);
  }


  public static void copyFile(String fname, File srcDir, File destDir)
    throws IOException
  {
    File src = new File(srcDir, fname);
    File dest = new File(destDir, fname);
    Files.copy(src.toPath(), dest.toPath());
  }


  public static boolean findCause(Throwable t, Class cls)
  {
    while(t.getCause() != null)
    {
      if(t.getCause().getClass().equals(cls))
        return true;
      t = t.getCause();
    }
    return false;
  }

  
  private static void setDest(String destPrefix)
    throws Throwable
  {
    _destUri = null;
    if((null == destPrefix) || destPrefix.isEmpty())
      return;

    // strip trailing slash, if exists, for consistency
    if(destPrefix.endsWith("/"))
      destPrefix = destPrefix.substring(0, destPrefix.length() - 1);

    // make sure that the bucket exists but folder does not if --dest-prefix
    // is passed in so we don't accidentally trash an existing folder
    CloudStoreClient client = null;
    try
    {
      URI destUri = null;
      try
      {
        destUri = Utils.getURI(destPrefix);
      }
      catch(URISyntaxException ex)
      {
        throw new RuntimeException("could not parse --dest-prefix URL ["
          + ex.getMessage() + "]");
      }

      _service = destUri.getScheme();
      client = createClient(_defaultRetryCount);

      String key = Utils.getObjectKey(destUri) + "/";
      String bucket = Utils.getBucket(destUri);
      ListOptionsBuilder builder = client.getOptionsBuilderFactory()
        .newListOptionsBuilder()
        .setBucketName(bucket)
        .setRecursive(false)
        .setIncludeVersions(false)
        .setExcludeDirs(false)
        .setObjectKey(key);
      List<StoreFile> matches =
        client.listObjects(builder.createOptions()).get();
      boolean found = false;
      for(StoreFile f : matches)
      {
        if(f.getKey().equals(key))
        {
          found = true;
          break;
        }
      }
      if(found)
        throw new RuntimeException(
           "Folder '" + destPrefix + "' specified by --dest-prefix already exists.");

     // set the URI that will be used as a prefix by test functions
     _destUri = destUri;

    }
    finally
    {
      destroyClient(client);
    }
  }


  private static Random getRand()
  {
    if(_rand == null)
      _rand = new Random(System.currentTimeMillis());
    return _rand;
  }


  private static void usage()
  {
    System.out.println("usage:  TestRunner {--help || -h} {--service s3|gs} {--endpoint url} {--dest-prefix url} {--keydir dir}");
  }

}
