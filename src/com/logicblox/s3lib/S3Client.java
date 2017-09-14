package com.logicblox.s3lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;


/**
 * Provides the client for accessing the Amazon S3 or S3-compatible web
 * service.
 * <p>
 * Captures the full configuration independent of concrete operations like
 * uploads or downloads.
 * <p>
 * For more information about Amazon S3, please see
 * <a href="http://aws.amazon.com/s3">http://aws.amazon.com/s3</a>
 */
public class S3Client implements CloudStoreClient {
  /**
   * Responsible for executing S3 HTTP API calls asynchronously. It, also,
   * determines the level of parallelism of an operation, e.g. number of
   * threads used for uploading/downloading a file.
   */
  ListeningExecutorService _s3Executor;

  /**
   * Responsible for executing internal s3lib tasks asynchronously. Such
   * tasks include file I/O, file encryption, file splitting and error handling.
   */
  ListeningScheduledExecutorService _executor;

  /** The AWS credentials to use when making requests to the service. */
  AWSCredentialsProvider _credentials;

  /**
   * Low-level AWS S3 client responsible for authentication, proxying and
   * HTTP requests.
   */
  AmazonS3Client _client;

  /**
   * The provider of key-pairs used to encrypt/decrypt files during
   * upload/download.
   */
  KeyProvider _keyProvider;

  /** Whether or not to retry client side exception unconditionally. */
  boolean _retryClientException = false;

  /** The number of times we want to retry in case of an error. */
  int _retryCount = 15;

  /**
   * Constructs a new high-level S3 client to invoke operations on S3 or
   * compatible service.
   * <p>
   * Objects created by this constructor will:
   * <ul>
   *   <li>use a thread pool of 10 threads to execute S3 HTTP API calls
   *   asynchronously</li>
   *   <li>use a thread pool of 50 threads to execute internal tasks
   *   asynchronously</li>
   *   <li>use a 5MB chunk-size for multi-part upload/download operations</li>
   *   <li>search {@code ~/.s3lib-keys} for specified cryptographic key-pair
   *   used to encrypt/decrypt files during upload/download</li>
   *   <li>retry a task 10 times in case of an error</li>
   * </ul>
   *
   * @param s3Client Low-level AWS S3 client responsible for authentication,
   *                 proxying and HTTP requests
   * @see S3Client#S3Client(AmazonS3Client, ListeningExecutorService,
   * ListeningScheduledExecutorService, KeyProvider)
   */
  public S3Client(AmazonS3Client s3Client)
  {
    this(s3Client,
        Utils.createApiExecutor(10),
        Utils.createInternalExecutor(50),
        Utils.createKeyProvider(Utils.getDefaultKeyDirectory()));
    this.setRetryCount(10);
  }

  /**
   * Constructs a new high-level S3 client to invoke operations on S3 or
   * compatible service.
   *
   * @param s3Client    Low-level AWS S3 client responsible for authentication,
   *                    proxying and HTTP requests
   * @param keyProvider The provider of key-pairs used to encrypt/decrypt files
   *                    during upload/download.
   * @see S3Client#S3Client(AmazonS3Client)
   * @see S3Client#S3Client(AmazonS3Client, ListeningExecutorService,
   * ListeningScheduledExecutorService, KeyProvider)
   */
  public S3Client(AmazonS3Client s3Client,
                  KeyProvider keyProvider)
  {
    this(s3Client,
        Utils.createApiExecutor(10),
        Utils.createInternalExecutor(50),
        keyProvider);
    this.setRetryCount(10);
  }

  /**
   * Constructs a new high-level S3 client to invoke operations on S3 or
   * compatible service.
   *
   * @param credentials The AWS credentials to use when making requests to the
   *                    service.
   * @param s3Executor  Responsible for executing S3 HTTP API calls
   *                    asynchronously. It, also, determines the level of
   *                    parallelism of an operation, e.g. number of threads
   *                    used for uploading/downloading a file.
   * @param executor    Responsible for executing internal s3lib tasks
   *                    asynchrounsly. Such tasks include file I/O, file
   *                    encryption, file splitting and error handling.
   * @param keyProvider The provider of key-pairs used to encrypt/decrypt files
   *                    during upload/download.
   * @see S3Client#S3Client(AmazonS3Client)
   * @see S3Client#S3Client(AmazonS3Client, ListeningExecutorService,
   * ListeningScheduledExecutorService, KeyProvider)
   */
  public S3Client(
    AWSCredentialsProvider credentials,
    ListeningExecutorService s3Executor,
    ListeningScheduledExecutorService executor,
    KeyProvider keyProvider)
  {
    _executor = executor;
    _s3Executor = s3Executor;
    _keyProvider = keyProvider;
    _credentials = credentials;
    if(_credentials != null)
      _client = new AmazonS3Client(_credentials);
    else
      _client = new AmazonS3Client();
  }

  /**
   * Constructs a new high-level S3 client to invoke operations on S3 or
   * compatible service.
   *
   * @param s3Client    Low-level AWS S3 client responsible for authentication,
   *                    proxying and HTTP requests
   * @param s3Executor  Responsible for executing S3 HTTP API calls
   *                    asynchronously. It, also, determines the level of
   *                    parallelism of an operation, e.g. number of threads used
   *                    for uploading/downloading a file.
   * @param executor    Responsible for executing internal s3lib tasks
   *                    asynchrounsly. Such tasks include file I/O, file
   *                    encryption, file splitting and error handling.
   * @param keyProvider The provider of key-pairs used to encrypt/decrypt files
   *                    during upload/download.
   * @see S3Client#S3Client(AmazonS3Client)
   */
  public S3Client(
      AmazonS3Client s3Client,
      ListeningExecutorService s3Executor,
      ListeningScheduledExecutorService executor,
      KeyProvider keyProvider)
  {
    _executor = executor;
    _s3Executor = s3Executor;
    _keyProvider = keyProvider;
    _client = s3Client;
  }

  /**
   * Canned ACLs handling
   */

  public static final String defaultCannedACL = "bucket-owner-full-control";

  public static final List<String> allCannedACLs = initCannedACLs();

  /**
   * {@code cannedACLsDescConst} has to be a compile-time String constant
   * expression. That's why e.g. we cannot re-use {@code allCannedACLs} to
   * construct it.
   */
  static final String cannedACLsDescConst = "For Amazon S3, choose one of: " +
      "private, public-read, public-read-write, authenticated-read, " +
      "bucket-owner-read, bucket-owner-full-control (default: " +
      "bucket-owner-full-control).";

  public static boolean isValidCannedACL(String aclStr)
  {
    return allCannedACLs.contains(aclStr);
  }

  private static List<String> initCannedACLs()
  {
    List<String> l = new ArrayList<>();
    for (CannedAccessControlList acl : CannedAccessControlList.values())
      l.add(acl.toString());
    return l;
  }

  /**
   * {@code storageClassesDescConst} has to be a compile-time String constant
   * expression. That's why e.g. we cannot re-use {@code StorageClass.values()}
   * to construct it.
   */
  static final String storageClassesDescConst = "For Amazon S3, choose one of: " +
      "STANDARD, REDUCED_REDUNDANCY, GLACIER, STANDARD_IA.";

  @Override
  public void setRetryCount(int retryCount)
  {
    _retryCount = retryCount;
  }

  @Override
  public void setRetryClientException(boolean retry)
  {
    _retryClientException = retry;
  }

  @Override
  public void setEndpoint(String endpoint)
  {
    _client.setEndpoint(endpoint);
  }

  @Override
  public String getScheme()
  {
    return "s3";
  }

  @Override
  public ListeningExecutorService getApiExecutor()
  {
    return _s3Executor;
  }

  @Override
  public ListeningScheduledExecutorService getInternalExecutor()
  {
    return _executor;
  }

  @Override
  public KeyProvider getKeyProvider()
  {
    return _keyProvider;
  }

  void configure(Command cmd)
  {
    cmd.setRetryClientException(_retryClientException);
    cmd.setRetryCount(_retryCount);
    cmd.setAmazonS3Client(_client);
    cmd.setScheme("s3://");
  }

  @Override
  public ListenableFuture<S3File> upload(UploadOptions options)
      throws IOException
  {
    UploadCommand cmd = new UploadCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<S3File>> uploadDirectory(UploadOptions options)
      throws IOException, ExecutionException, InterruptedException {
    UploadDirectoryCommand cmd = new UploadDirectoryCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<S3File>> deleteDir(DeleteOptions options)
    throws InterruptedException, ExecutionException
  {
    DeleteDirCommand cmd = new DeleteDirCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<S3File> delete(DeleteOptions options)
  {
    DeleteCommand cmd = new DeleteCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<Bucket>> listBuckets()
  {
      List<Bucket> result = _client.listBuckets();
      return Futures.immediateFuture(result);
  }

  @Override
  public ListenableFuture<ObjectMetadata> exists(String bucket, String object)
  {
    ExistsCommand cmd = new ExistsCommand(_s3Executor, _executor);
    configure(cmd);
    return cmd.run(bucket, object);
  }

  @Override
  public ListenableFuture<S3File> download(DownloadOptions options)
  throws IOException
  {
    DownloadCommand cmd = new DownloadCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<S3File>> downloadDirectory(DownloadOptions
                                                                options)
  throws IOException, ExecutionException, InterruptedException
  {
    DownloadDirectoryCommand cmd = new DownloadDirectoryCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<S3File> copy(CopyOptions options)
  {
    CopyCommand cmd = new CopyCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<S3File>> copyToDir(CopyOptions options) throws
      InterruptedException, ExecutionException, IOException {
    CopyToDirCommand cmd = new CopyToDirCommand(options);
    configure(cmd);
    return cmd.run();
  }
  
  @Override
  public ListenableFuture<S3File> rename(RenameOptions options)
  {
    RenameCommand cmd = new RenameCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<S3File>> renameDirectory(RenameOptions options)
    throws InterruptedException, ExecutionException, IOException
  {
    RenameDirectoryCommand cmd = new RenameDirectoryCommand(options);
    configure(cmd);
    return cmd.run();
  }
  
  @Override
  public ListenableFuture<List<S3File>> listObjects(ListOptions options) {
    ListenableFuture<List<S3File>> results = null;
    if (options.versionsIncluded()) {
      ListVersionsCommand cmd = new ListVersionsCommand(options);
      configure(cmd);
      results = cmd.run();
    } else {
      ListCommand cmd = new ListCommand(options);
      configure(cmd);
      results = cmd.run();
    }
    return results;
  }

  @Override
  public ListenableFuture<List<Upload>> listPendingUploads(PendingUploadsOptions options)
  {
      ListPendingUploadsCommand cmd = new ListPendingUploadsCommand(options);
      configure(cmd);
      return cmd.run();
  }

  @Override
  public ListenableFuture<List<Void>> abortPendingUploads(PendingUploadsOptions options)
  {
      AbortPendingUploadsCommand cmd = new AbortPendingUploadsCommand(options);
      configure(cmd);
      return cmd.run();
  }

  @Override
  public ListenableFuture<S3File> addEncryptionKey(EncryptionKeyOptions options)
      throws IOException
  {
    AddEncryptionKeyCommand cmd = createAddKeyCommand(options);
    return cmd.run();
  }

  protected AddEncryptionKeyCommand createAddKeyCommand(EncryptionKeyOptions options)
      throws IOException
  {
    AddEncryptionKeyCommand cmd = new AddEncryptionKeyCommand(options);
    configure(cmd);
    return cmd;
  }

  @Override
  public ListenableFuture<S3File> removeEncryptionKey(EncryptionKeyOptions options)
    throws IOException
  {
    RemoveEncryptionKeyCommand cmd = createRemoveKeyCommand(options);
    return cmd.run();
  }

  protected RemoveEncryptionKeyCommand createRemoveKeyCommand(EncryptionKeyOptions options)
    throws IOException
  {
    RemoveEncryptionKeyCommand cmd = new RemoveEncryptionKeyCommand(options);
    configure(cmd);
    return cmd;
  }


  /**
   * Returns s3lib package version in this format:
   * "Version.RevNum-RevHash_BuildDatetime"
   * <p>
   * Example:
   *   "1.0.272-2dab7d1c9c69_201502181542"
   */
  public static String version()
  {
    Package p = S3Client.class.getPackage();
    String v = p.getSpecificationVersion() + '.' + p.getImplementationVersion();
    
    return v;
  }

  @Override
  public void shutdown()
  {
    try
    {
      _client.shutdown();
    }
    catch(Exception exc)
    {
      exc.printStackTrace();
    }

    try
    {
      _s3Executor.shutdown();
    }
    catch(Exception exc)
    {
      exc.printStackTrace();
    }

    try
    {
      _executor.shutdown();
    }
    catch(Exception exc)
    {
      exc.printStackTrace();
    }
  }

  @Override
  public boolean hasBucket(String bucketName)
  {
    return _client.doesBucketExist(bucketName);
  }

  @Override
  public void createBucket(String bucketName)
  {
    _client.createBucket(new CreateBucketRequest(bucketName));
  }

  @Override
  public void destroyBucket(String bucketName)
  {
    _client.deleteBucket(bucketName);
  }

  // needed for testing
  void setKeyProvider(KeyProvider kp)
  {
    _keyProvider = kp;
  }
}
