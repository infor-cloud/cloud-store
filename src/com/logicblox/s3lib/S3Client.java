package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;

import com.google.common.base.Optional;
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
        Utils.getHttpExecutor(10),
        Utils.getInternalExecutor(50),
        Utils.getKeyProvider(Utils.getDefaultKeyDirectory()));
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
        Utils.getHttpExecutor(10),
        Utils.getInternalExecutor(50),
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
  public URI getUri(String bucket, String object) throws URISyntaxException
  {
    return new URI("s3://" + bucket + "/" + object);
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
    File file = options.getFile();
    long chunkSize = options.getChunkSize();
    String acl = options.getAcl().or("bucket-owner-full-control");
    String encKey = options.getEncKey().orNull();
    Optional<OverallProgressListenerFactory> progressListenerFactory = options
        .getOverallProgressListenerFactory();

    UploadCommand cmd =
        new UploadCommand(_s3Executor, _executor, file, chunkSize, encKey,
            _keyProvider, acl, progressListenerFactory);
    configure(cmd);

    return cmd.run(options.getBucket(), options.getObjectKey());
  }

  @Override
  public ListenableFuture<S3File> upload(File file, String bucket, String
      object, String key, CannedAccessControlList acl)
  throws IOException
  {
    UploadOptions options = new UploadOptionsBuilder()
        .setFile(file)
        .setBucket(bucket)
        .setObjectKey(object)
        .setEncKey(key)
        .setAcl(acl.toString())
        .createUploadOptions();

    return upload(options);
  }

  @Override
  public ListenableFuture<S3File> upload(File file, URI s3url)
  throws IOException
  {
    return upload(file, s3url, null);
  }

  @Override
  public ListenableFuture<S3File> upload(File file, URI s3url, String key)
      throws IOException
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return upload(file, bucket, object, key);
  }

  @Override
  public ListenableFuture<S3File> upload(File file, String bucket, String
      object)
  throws IOException
  {
    return upload(file, bucket, object, null);
  }

  @Override
  public ListenableFuture<S3File> upload(File file, String bucket, String
      object, String key)
  throws IOException
  {
    UploadOptions options = new UploadOptionsBuilder()
        .setFile(file)
        .setBucket(bucket)
        .setObjectKey(object)
        .setEncKey(key)
        .createUploadOptions();

    return upload(options);
  }

  @Override
  public ListenableFuture<List<S3File>> uploadDirectory(UploadOptions options)
      throws IOException, ExecutionException, InterruptedException {

    File directory = options.getFile();
    String bucket = options.getBucket();
    String object = options.getObjectKey();
    long chunkSize = options.getChunkSize();
    String encKey = options.getEncKey().orNull();
    String acl = options.getAcl().or("bucket-owner-full-control");
    OverallProgressListenerFactory progressListenerFactory = options
        .getOverallProgressListenerFactory().orNull();

    UploadDirectoryCommand cmd = new UploadDirectoryCommand(_s3Executor,
        _executor, this);
    configure(cmd);
    return cmd.run(directory, bucket, object, chunkSize,  encKey, acl,
      progressListenerFactory);
  }

  @Override
  public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
      s3url, String encKey)
          throws IOException, ExecutionException, InterruptedException {
    UploadOptions options = new UploadOptionsBuilder()
        .setFile(directory)
        .setUri(s3url)
        .setEncKey(encKey)
        .setAcl("bucket-owner-full-control")
        .createUploadOptions();

    return this.uploadDirectory(options);
  }

  @Override
  public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
      s3url, String encKey, CannedAccessControlList acl)
          throws IOException, ExecutionException, InterruptedException {
    UploadOptions options = new UploadOptionsBuilder()
        .setFile(directory)
        .setUri(s3url)
        .setEncKey(encKey)
        .setAcl(acl.toString())
        .createUploadOptions();

    return this.uploadDirectory(options);
  }

  @Override
  public ListenableFuture<S3File> delete(String bucket, String object)
  {
    DeleteCommand cmd =
      new DeleteCommand(_s3Executor, _executor);
    configure(cmd);
    return cmd.run(bucket, object);
  }

  @Override
  public ListenableFuture<S3File> delete(URI s3url)
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return delete(bucket, object);
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
  public ListenableFuture<ObjectMetadata> exists(URI s3url)
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return exists(bucket, object);
  }

  @Override
  public ListenableFuture<S3File> download(DownloadOptions options)
  throws IOException
  {
    File file = options.getFile();
    boolean overwrite = options.doesOverwrite();
    OverallProgressListenerFactory progressListenerFactory = options
        .getOverallProgressListenerFactory().orNull();

    DownloadCommand cmd = new DownloadCommand(this, _s3Executor, _executor, file,
        overwrite, _keyProvider, progressListenerFactory);
    configure(cmd);
    String bucket = options.getBucket();
    String key = options.getObjectKey();
    String version = options.getVersion();
    return cmd.run(bucket, key, version);
  }

  @Override
  public ListenableFuture<S3File> download(File file, String bucket, String
      object)
      throws IOException
  {
    DownloadOptions options = new DownloadOptionsBuilder()
        .setFile(file)
        .setBucket(bucket)
        .setObjectKey(object)
        .createDownloadOptions();

    return download(options);
  }

  @Override
  public ListenableFuture<S3File> download(File file, URI s3url)
          throws IOException
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return download(file, bucket, object);
  }

  @Override
  public ListenableFuture<List<S3File>> downloadDirectory(DownloadOptions
                                                                options)
  throws IOException, ExecutionException, InterruptedException
  {
    DownloadDirectoryCommand cmd = new DownloadDirectoryCommand(_s3Executor,
        _executor, this);
    configure(cmd);

    File directory = options.getFile();
    String bucket = options.getBucket();
    String object = options.getObjectKey();
    boolean recursive = options.isRecursive();
    boolean overwrite = options.doesOverwrite();
    OverallProgressListenerFactory progressListenerFactory = options
        .getOverallProgressListenerFactory().orNull();

    return cmd.run(directory, bucket, object, recursive, overwrite,
        progressListenerFactory);
  }

  @Override
  public ListenableFuture<List<S3File>> downloadDirectory(
      File directory, URI s3url, boolean recursive, boolean overwrite)
  throws IOException, ExecutionException, InterruptedException
  {
    DownloadOptions options = new DownloadOptionsBuilder()
        .setFile(directory)
        .setUri(s3url)
        .setRecursive(recursive)
        .setOverwrite(overwrite)
        .createDownloadOptions();

    return this.downloadDirectory(options);
  }

  @Override
  public ListenableFuture<S3File> copy(CopyOptions options)
  throws IOException
  {
    String cannedAcl = options.getCannedAcl().or("bucket-owner-full-control");
    OverallProgressListenerFactory progressListenerFactory = options
        .getOverallProgressListenerFactory().orNull();

    CopyCommand cmd = new CopyCommand(_s3Executor, _executor, cannedAcl,
        progressListenerFactory);
    configure(cmd);
    return cmd.run(options.getSourceBucketName(), options.getSourceKey(),
        options.getDestinationBucketName(), options.getDestinationKey());
  }

  @Override
  public ListenableFuture<List<S3File>> copyToDir(CopyOptions options) throws
      InterruptedException, ExecutionException, IOException,
      URISyntaxException {
    CopyToDirCommand cmd = new CopyToDirCommand(_s3Executor, _executor, this);
    configure(cmd);

    return cmd.run(options);
  }
  
  @Override
  public ListenableFuture<List<S3File>> listObjects(ListOptions lsOptions) {
    ListenableFuture<List<S3File>> results = null;
    if (lsOptions.versionsIncluded()) {
      ListVersionsCommand cmd = new ListVersionsCommand(_s3Executor, _executor);
      configure(cmd);
      results = cmd.run(lsOptions);
    } else {
      ListCommand cmd = new ListCommand(_s3Executor, _executor);
      configure(cmd);
      results = cmd.run(lsOptions);
    }
    return results;
  }

  @Override
  public ListenableFuture<List<Upload>> listPendingUploads(
      String bucket, String prefix)
  {
      ListPendingUploadsCommand cmd =
          new ListPendingUploadsCommand(_s3Executor, _executor);
      configure(cmd);
      return cmd.run(bucket, prefix);
  }

  @Override
  public ListenableFuture<Void> abortPendingUpload(
      String bucket, String key, String uploadId)
  {
      AbortPendingUploadCommand cmd =
          new AbortPendingUploadCommand(_s3Executor, _executor);
      configure(cmd);
      return cmd.run(bucket, key, uploadId);
  }

  @Override
  public ListenableFuture<List<Void>> abortOldPendingUploads(
      String bucket, String prefix, Date date)
      throws InterruptedException, ExecutionException, URISyntaxException {
      AbortOldPendingUploadsCommand cmd =
          new AbortOldPendingUploadsCommand(_s3Executor, _executor, this);
      configure(cmd);
      return cmd.run(bucket, prefix, date);
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
  @Override
  public void setKeyProvider(KeyProvider kp)
  {
    _keyProvider = kp;
  }
}
