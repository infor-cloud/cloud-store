package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.amazonaws.ClientConfiguration;
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
public class S3Client
{
  /**
   * Responsible for executing S3 HTTP API calls asynchronously. It, also,
   * determines the level of parallelism of an operation, e.g. number of
   * threads used for uploading/downloading a file.
   */
  ListeningExecutorService _s3Executor;

  /**
   * Responsible for executing internal s3lib tasks asynchrounsly. Such
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

  /**
   * Sets the number of retries after a failure.
   *
   * @param retryCount Number of retries
   */
  public void setRetryCount(int retryCount)
  {
    _retryCount = retryCount;
  }

  public void setRetryClientException(boolean retry)
  {
    _retryClientException = retry;
  }

  /**
   * Sets the API endpoint this client should issue the requests to. If it's
   * not set, then the underlying {@code s3Client} is going to use its
   * default one for accessing Amazon S3.
   * </p>
   * This method is mostly useful if we would like to use this client with
   * services other than S3 (e.g. Google Cloud Storage) that provide a
   * S3-compatible API or for testing reasons.
   *
   * @param endpoint API endpoint
   */
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

  /**
   * Uploads the specified {@code file} to Amazon S3 under the specified
   * {@code bucket} and {@code object} name. The specified {@code acl} is
   * applied to the uploaded file. If the {@code key} is not null, the {@code
   * keyProvider} will be asked to provide a public key with that name. This
   * key will be used to encrypt the {@code file} at the client side. If
   * {@code progress} is enabled, then progress notifications will be printed.
   * </p>
   * The specified bucket must already exist and the caller must
   * have write permission to the bucket to upload an object.
   * </p>
   * By default, the upload is multi-part with each part being {@code chunkSize}
   * bytes.
   * </p>
   * The level of parallelism depends on the {@code s3Executor}. Ideally,
   * different parts are uploaded by different threads.
   * </p>
   * The client automatically does checksum validation both for each part and
   * for the final object, making sure there is no file corruption during the
   * transfer.
   * </p>
   * Low-level upload operations (e.g. file type recognition) are taken care of
   * by the underlying low-level {@code s3Client}.
   * </p>
   * If during this call an exception wasn't thrown, the entire object has
   * supposedly been stored successfully.
   *
   * @param file     File to upload
   * @param bucket   Bucket to upload to
   * @param object   Path in bucket to upload to
   * @param key      Name of encryption key to use
   * @param acl      Access control list to use
   * @param progress Enable progress indicator
   */
  public ListenableFuture<S3File> upload(File file, String bucket, String
      object, String key, String acl, boolean progress)
      throws IOException
  {
    UploadCommand cmd =
        new UploadCommand(_s3Executor, _executor, file, _chunkSize, key,
            _keyProvider, acl, progress);
    configure(cmd);
    return cmd.run(bucket, object);
  }

  /**
   * Uploads the specified {@code file} to Amazon S3 under the specified
   * {@code bucket} and {@code object} name. The specified {@code acl} is
   * applied to the uploaded file. If the {@code key} is not null, the {@code
   * keyProvider} will be asked to provide a public key with that name. This
   * key will be used to encrypt the {@code file} at the client side.
   *
   * @param file     File to upload
   * @param bucket   Bucket to upload to
   * @param object   Path in bucket to upload to
   * @param key      Name of encryption key to use
   * @param acl      Access control list to use
   * @see S3Client#upload(File, String, String, String, String, boolean)
   */
  public ListenableFuture<S3File> upload(File file, String bucket, String
      object, String key, String acl)
      throws IOException
  {
    return upload(file, bucket, object, key, acl, false);
  }

  /**
   * Uploads the specified {@code file} to Amazon S3 under the specified {@code
   * bucket} and {@code object} name. If the {@code key} is not null, the {@code
   * keyProvider} will be asked to provide a public key with that name. This key
   * will be used to encrypt the {@code file} at the client side. It applies
   * {@code acl} permissions on the uploaded object.
   *
   * @param file   File to upload
   * @param bucket Bucket to upload to
   * @param object Path in bucket to upload to
   * @param key    Name of encryption key to use
   * @param acl    Access control list to use
   * @see S3Client#upload(File, String, String, String, String, boolean)
   */
  @Deprecated
  public ListenableFuture<S3File> upload(File file, String bucket, String
      object, String key, CannedAccessControlList acl)
  throws IOException
  {
    return upload(file, bucket, object, key, acl.toString(), false);
  }

  /**
   * Uploads the specified {@code file} to Amazon S3 under the specified
   * {@code s3url}.
   * </p>
   * By default, the canned ACL "bucket-owner-full-control" is applied to the
   * uploaded object.
   *
   * @param file    File to upload
   * @param s3url   Object URI (e.g. s3://bucket/key)
   * @see S3Client#upload(File, String, String, String, String, boolean)
   */
  public ListenableFuture<S3File> upload(File file, URI s3url)
  throws IOException
  {
    return upload(file, s3url, null);
  }

  /**
   * Uploads the specified {@code file} to Amazon S3 under the specified
   * {@code s3url}. If the {@code key} is not null, the {@code keyProvider}
   * will be asked to provide a public key with that name. This key will be
   * used to encrypt the {@code file} at the client side.
   * </p>
   * By default, the canned ACL "bucket-owner-full-control" is applied to the
   * uploaded object.
   *
   * @param file    File to upload
   * @param s3url   Object URI (e.g. s3://bucket/key)
   * @param key     Name of encryption key to use
   * @see S3Client#upload(File, String, String, String, String, boolean)
   */
  public ListenableFuture<S3File> upload(File file, URI s3url, String key)
      throws IOException
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return upload(file, bucket, object, key);
  }


  /**
   * Uploads the specified {@code file} to Amazon S3 under the specified
   * {@code bucket} and {@code object} name.
   * </p>
   * By default, the canned ACL "bucket-owner-full-control" is applied to the
   * uploaded object.
   *
   * @param file    File to upload
   * @param bucket  Bucket to upload to
   * @param object  Path in bucket to upload to
   * @see S3Client#upload(File, String, String, String, String, boolean)
   */
  public ListenableFuture<S3File> upload(File file, String bucket, String
      object)
  throws IOException
  {
    return upload(file, bucket, object, null);
  }

  /**
   * Uploads the specified {@code file} to Amazon S3 under the specified
   * {@code bucket} and {@code object} name. If the {@code key} is not null,
   * the {@code keyProvider} will be asked to provide a public key with that
   * name. This key will be used to encrypt the {@code file} at the client side.
   * </p>
   * By default, the canned ACL "bucket-owner-full-control" is applied to the
   * uploaded object.
   *
   * @param file    File to upload
   * @param bucket  Bucket to upload to
   * @param object  Path in bucket to upload to
   * @param key     Name of encryption key to use
   * @see S3Client#upload(File, String, String, String, String, boolean)
   */
  public ListenableFuture<S3File> upload(File file, String bucket, String
      object, String key)
  throws IOException
  {
    return upload(file, bucket, object, key, "bucket-owner-full-control", false);
  }

  /**
   * Finds every file under {@code directory} recursively and uploads it to S3.
   * The specified {@code acl} is applied to each created object. If {@code
   * progress} is enabled, then progress notifications will be printed.
   * </p>
   * Each individual file is uploaded with {@link S3Client#upload(File,
   * String,String, String, CannedAccessControlList)}.
   * </p>
   * Symbolic links are not supported.
   *
   * @param directory Directory to upload
   * @param s3url     S3 URL to upload to
   * @param encKey    Encryption key to use
   * @param acl       Access control list to use
   * @param progress  Enable progress indicator
   */
  public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
      s3url, String encKey, String acl, boolean progress)
      throws IOException, ExecutionException, InterruptedException {
    UploadDirectoryCommand cmd = new UploadDirectoryCommand(_s3Executor,
        _executor, this);
    configure(cmd);

    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return cmd.run(directory, bucket, object, encKey, acl, progress);
  }

  /**
   * Finds every file under {@code directory} recursively and uploads it to S3.
   * </p>
   * By default, the canned ACL "bucket-owner-full-control" is applied to each
   * uploaded object.
   *
   * @param directory Directory to upload
   * @param s3url     S3 URL to upload to
   * @param encKey    Encryption key to use
   * @see S3Client#uploadDirectory(File, URI, String, String, boolean)
   */
  public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
      s3url, String encKey)
          throws IOException, ExecutionException, InterruptedException {
    return this.uploadDirectory(directory, s3url, encKey,
        "bucket-owner-full-control", false);
  }

  /**
   * Finds every file under {@code directory} recursively and uploads it to S3.
   * The specified {@code acl} is applied to each uploaded object.
   *
   * @param directory Directory to upload
   * @param s3url     S3 URL to upload to
   * @param encKey    Encryption key to use
   * @param acl       Access control list to use
   * @see S3Client#uploadDirectory(File, URI, String, String, boolean)
   */
  @Deprecated
  public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
      s3url, String encKey, CannedAccessControlList acl)
          throws IOException, ExecutionException, InterruptedException {
    return this.uploadDirectory(directory, s3url, encKey, acl.toString(),
        false);
  }

  /**
   * Deletes the specified {@code object} under S3 {@code bucket}.
   * </p>
   * No error is returned if the file doesn't exist. If you care, use the
   * {@link S3Client#exists} methods to check.
   *
   * @param bucket  Bucket to delete from
   * @param object  Object to be deleted from bucket
   */
  public ListenableFuture<S3File> delete(String bucket, String object)
  {
    DeleteCommand cmd =
      new DeleteCommand(_s3Executor, _executor);
    configure(cmd);
    return cmd.run(bucket, object);
  }

  /**
   * Deletes the specified {@code s3url} from S3.
   *
   * @param s3url Object URI (e.g. s3://bucket/key)
   * @see S3Client#delete(String, String)
   */
  public ListenableFuture<S3File> delete(URI s3url)
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return delete(bucket, object);
  }

  /** Lists all S3 buckets visible for this account. */
  public ListenableFuture<List<Bucket>> listBuckets()
  {
      List<Bucket> result = _client.listBuckets();
      return Futures.immediateFuture(result);
  }

  /**
   * Checks if the specified {@code object} exists in the S3 {@code bucket}.
   * </p>
   * Returns a null future if the file does not exist.
   * </p>
   * Throws an exception if the metadata of the file could not be fetched for
   * different reasons.
   *
   * @param bucket  Bucket to check
   * @param object  Path in bucket to check
   */
  public ListenableFuture<ObjectMetadata> exists(String bucket, String object)
  {
    ExistsCommand cmd = new ExistsCommand(_s3Executor, _executor);
    configure(cmd);
    return cmd.run(bucket, object);
  }

  /**
   * Checks if the specified {@code s3url} exists in the S3.
   *
   * @param s3url Object URI (e.g. s3://bucket/key)
   * @see S3Client#exists(String, String)
   */
  public ListenableFuture<ObjectMetadata> exists(URI s3url)
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return exists(bucket, object);
  }

  /**
   * Downloads the specified {@code object}, under {@code bucket}, from S3 to
   * a local {@code file}. If {@code progress} is enabled, then progress
   * notifications will be printed.
   * </p>
   * If the {@code object} was encrypted by s3lib at the client-side, this
   * method will try to decrypt it automatically. It knows which key to use by
   * checking {@code object}'s metadata.
   * </p>
   * The specified bucket must already exist and the caller must have read
   * permission to the {@code bucket}.
   * </p>
   * By default, if {@code object}'s size is bigger than the {@code
   * chunkSize} chosen during the upload, then multiple range GETs will be
   * issued, effectively dowloading different parts of the file concurrently.
   * </p>
   * The level of parallelism depends on the {@code s3Executor}. Ideally,
   * different parts are downloaded by different threads.
   * </p>
   * Since a file can be uploaded, updated and/or copied by any tool is not
   * always safe and efficient to validate its checksum, since there is no
   * easy way to detect each tool's chosen chunk size (which affects object's
   * ETag).
   *
   * Currently, this client tries to do checksum validation when it has good
   * indication that a file was uploaded by s3lib and/or the file can been fully
   * downloaded using a single range GET.
   * </p>
   * If during this call an exception wasn't thrown, the entire object has
   * supposedly been stored successfully.
   *
   * @param file     File to store the object
   * @param bucket   Bucket to download from
   * @param object   Path in bucket to download
   * @param progress Enable progress indicator
   */
  public ListenableFuture<S3File> download(File file, String bucket, String
      object, boolean progress)
  throws IOException
  {
    DownloadCommand cmd = new DownloadCommand(_s3Executor, _executor, file,
        _keyProvider, progress);
    configure(cmd);
    return cmd.run(bucket, object);
  }

  /**
   * Downloads the specified {@code object}, under {@code bucket}, from S3 to
   * a local {@code file}.
   *
   * @param file      File to download
   * @param bucket    Bucket to download from
   * @param object    Path in bucket to download
   * @see S3Client#download(File, String, String, boolean)
   */
  public ListenableFuture<S3File> download(File file, String bucket, String
      object)
      throws IOException
  {
    return download(file, bucket, object, false);
  }

  /**
   * Downloads the specified {@code s3url} from S3 to a local {@code file}.
   *
   * @param file    File to download
   * @param s3url   S3 object URL to download from
   * @see S3Client#download(File, String, String, boolean)
   */
  public ListenableFuture<S3File> download(File file, URI s3url)
          throws IOException
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return download(file, bucket, object);
  }

  /**
   * Downloads the specified {@code s3url} from S3 to a local {@code file}.
   * If {@code progress} is enabled, then progress notifications will be
   * printed.
   *
   * @param file     File to download
   * @param s3url    S3 object URL to download from
   * @param progress Enable progress indicator
   * @see S3Client#download(File, String, String, boolean)
   */
  public ListenableFuture<S3File> download(File file, URI s3url, boolean
      progress)
          throws IOException
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return download(file, bucket, object, progress);
  }

  /**
   * Downloads the conceptual S3 directory, under {@code s3url}, to the local
   * file system, under {@code directory}. If {@code progress} is enabled,
   * then progress notifications will be printed.
   * </p>
   * If {@code recursive} is true, then all objects under {@code s3url} will
   * be downloaded. Otherwise, only the first-level objects will be
   * downloaded.
   * </p>
   * If {@code overwrite} is true, then newly downloaded
   * files is possible to overwrite existing local files.
   * </p>
   * Each individual file is downloaded with {@link S3Client#download(File,
   * String, String)}.
   *
   * @param directory Directory to download to
   * @param s3url     Object URI that represents a directory (e.g.
   *                  s3://bucket/key/)
   * @param recursive Download all files recursively under s3url
   * @param overwrite Overwrite a file if it already exists
   * @param progress  Enable progress indicator
   */
  public ListenableFuture<List<S3File>> downloadDirectory(
    File directory, URI s3url, boolean recursive, boolean overwrite, boolean
      progress)
  throws IOException, ExecutionException, InterruptedException
  {
    DownloadDirectoryCommand cmd = new DownloadDirectoryCommand(_s3Executor,
        _executor, this);
    configure(cmd);

    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return cmd.run(directory, bucket, object, recursive, overwrite, progress);
  }

  /**
   * Downloads the conceptual S3 directory, under {@code s3url}, to the local
   * file system, under {@code directory}.
   *
   * @param directory Directory to download to
   * @param s3url     Object URI that represents a directory (e.g.
   *                  s3://bucket/key/)
   * @param recursive Download all files recursively under s3url
   * @param overwrite Overwrite a file if it already exists

   * @see S3Client#downloadDirectory(File, URI, boolean, boolean, boolean)
   */
  public ListenableFuture<List<S3File>> downloadDirectory(
    File directory, URI s3url, boolean recursive, boolean overwrite)
  throws IOException, ExecutionException, InterruptedException
  {
    return this.downloadDirectory(directory, s3url, recursive, overwrite,
        false);
  }

  /**
   * List object in S3
   *
   * TODO: LB-1187
   */
  public ListenableFuture<List<S3ObjectSummary>> listObjects(
    String bucket, String prefix, boolean recursive)
  {
    ListObjectsCommand cmd =
            new ListObjectsCommand(_s3Executor, _executor);
    configure(cmd);
    return cmd.run(bucket, prefix, recursive);
  }

  /**
   * List objects and (first-level) directories in S3
   *
   * TODO: LB-1187
   */
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

  /**
   * Makes sure all pending tasks have been completed and shuts down all
   * internal machinery properly.
   */
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
