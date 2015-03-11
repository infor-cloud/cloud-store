package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.CannedAccessControlList;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.amazonaws.services.s3.model.S3ObjectSummary;


/**
 * Provides the client for accessing the Amazon S3 or S3-compatible web
 * service.
 * <p/>
 * Captures the full configuration independent of concrete operations like
 * uploads or downloads.
 * <p/>
 * For more information about Amazon S3, please see <a href="http://aws
 * .amazon.com/s3">http://aws.amazon.com/s3</a>
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

  /** The size of an upload/download chunk (part) in bytes. */
  long _chunkSize;

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
   * <p/>
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
   * ListeningScheduledExecutorService, long, KeyProvider)
   */
  public S3Client(AmazonS3Client s3Client)
  {
    this(s3Client,
        Utils.getHttpExecutor(10),
        Utils.getInternalExecutor(50),
        Utils.getDefaultChunkSize(),
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
   * ListeningScheduledExecutorService, long, KeyProvider)
   */
  public S3Client(AmazonS3Client s3Client,
                  KeyProvider keyProvider)
  {
    this(s3Client,
        Utils.getHttpExecutor(10),
        Utils.getInternalExecutor(50),
        Utils.getDefaultChunkSize(),
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
   * @param chunkSize   The size of an upload/download chunk (part) in bytes.
   * @param keyProvider The provider of key-pairs used to encrypt/decrypt files
   *                    during upload/download.
   * @see S3Client#S3Client(AmazonS3Client)
   * @see S3Client#S3Client(AmazonS3Client, ListeningExecutorService,
   * ListeningScheduledExecutorService, long, KeyProvider)
   */
  public S3Client(
    AWSCredentialsProvider credentials,
    ListeningExecutorService s3Executor,
    ListeningScheduledExecutorService executor,
    long chunkSize,
    KeyProvider keyProvider)
  {
    _executor = executor;
    _s3Executor = s3Executor;
    _chunkSize = chunkSize;
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
   * @param chunkSize   The size of an upload/download chunk (part) in bytes.
   * @param keyProvider The provider of key-pairs used to encrypt/decrypt files
   *                    during upload/download.
   * @see S3Client#S3Client(AmazonS3Client)
   */
  public S3Client(
      AmazonS3Client s3Client,
      ListeningExecutorService s3Executor,
      ListeningScheduledExecutorService executor,
      long chunkSize,
      KeyProvider keyProvider)
  {
    _executor = executor;
    _s3Executor = s3Executor;
    _chunkSize = chunkSize;
    _keyProvider = keyProvider;
    _client = s3Client;
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
    String acl = options.getAcl().or("bucket-owner-full-control");
    String encKey = options.getEncKey().orNull();
    S3ProgressListenerFactory progressListenerFactory = options
        .getS3ProgressListenerFactory().orNull();

    UploadCommand cmd =
        new UploadCommand(_s3Executor, _executor, file, _chunkSize, encKey,
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
    UploadDirectoryCommand cmd = new UploadDirectoryCommand(_s3Executor,
        _executor, this);
    configure(cmd);

    File directory = options.getFile();
    String bucket = options.getBucket();
    String object = options.getObjectKey();
    String encKey = options.getEncKey().orNull();
    String acl = options.getAcl().or("bucket-owner-full-control");
    S3ProgressListenerFactory progressListenerFactory = options
        .getS3ProgressListenerFactory().orNull();

    return cmd.run(directory, bucket, object, encKey, acl, progressListenerFactory);
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
    S3ProgressListenerFactory progressListenerFactory = options
        .getS3ProgressListenerFactory().orNull();

    DownloadCommand cmd = new DownloadCommand(_s3Executor, _executor, file,
        _keyProvider, progressListenerFactory);
    configure(cmd);
    return cmd.run(options.getBucket(), options.getObjectKey());
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
    S3ProgressListenerFactory progressListenerFactory = options
        .getS3ProgressListenerFactory().orNull();

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
  public ListenableFuture<List<S3ObjectSummary>> listObjects(
      String bucket, String prefix, boolean recursive)
  {
    ListObjectsCommand cmd =
            new ListObjectsCommand(_s3Executor, _executor);
    configure(cmd);
    return cmd.run(bucket, prefix, recursive);
  }

  @Override
  public ListenableFuture<List<S3File>> listObjectsAndDirs(
      String bucket, String prefix, boolean recursive)
  {
    ListObjectsAndDirsCommand cmd =
            new ListObjectsAndDirsCommand(_s3Executor, _executor);
    configure(cmd);
    return cmd.run(bucket, prefix, recursive);
  }

  /**
   * Returns s3lib package version in this format:
   * "Version.RevNum-RevHash_BuildDatetime"
   * </p>
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
}
