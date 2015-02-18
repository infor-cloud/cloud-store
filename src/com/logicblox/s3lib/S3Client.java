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
 * Captures the full configuration independent of concrete uploads and
 * downloads.
 */
public class S3Client
{
  ListeningExecutorService _s3Executor;
  ListeningScheduledExecutorService _executor;
  long _chunkSize;
  AWSCredentialsProvider _credentials;
  AmazonS3Client _client;
  KeyProvider _keyProvider;
  boolean _retryClientException = false;
  int _retryCount = 15;

  /**
   * @param credentials   AWS Credentials
   * @param s3Executor    Executor for executing S3 API calls
   * @param executor      Executor for internally initiating uploads
   * @param chunkSize     Size of chunks
   * @param keyProvider   Provider of encryption keys
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

  public void setRetryCount(int retryCount)
  {
    _retryCount = retryCount;
  }

  public void setRetryClientException(boolean retry)
  {
    _retryClientException = retry;
  }

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
   * Upload file to S3 without encryption.
   *
   * @param file    File to upload
   * @param s3url   S3 object URL (using same syntax as s3cmd)
   */
  public ListenableFuture<S3File> upload(File file, URI s3url)
  throws IOException
  {
    return upload(file, s3url, null);
  }

  /**
   * Upload file to S3 without encryption.
   *
   * @param file    File to upload
   * @param bucket  Bucket to upload to
   * @param object  Path in bucket to upload to
   */
  public ListenableFuture<S3File> upload(File file, String bucket, String object)
  throws IOException
  {
    return upload(file, bucket, object, null);
  }

  /**
   * Upload file to S3.
   *
   * @param file    File to upload
   * @param bucket  Bucket to upload to
   * @param object  Path in bucket to upload to
   * @param key     Name of encryption key to use
   */
  @Deprecated
  public ListenableFuture<S3File> upload(File file, String bucket, String object, String key)
  throws IOException
  {
    return upload(file, bucket, object, key, "bucket-owner-full-control", false);
  }

  /**
   * Upload file to S3.
   *
   * @param file    File to upload
   * @param bucket  Bucket to upload to
   * @param object  Path in bucket to upload to
   * @param key     Name of encryption key to use
   * @param acl     Access control list to use
   */
  @Deprecated
  public ListenableFuture<S3File> upload(File file, String bucket, String object, String key, CannedAccessControlList acl)
          throws IOException
  {
    return upload(file, bucket, object, key, acl.toString(), false);
  }

  /**
   * Upload file to S3.
   *
   * @param file    File to upload
   * @param bucket  Bucket to upload to
   * @param object  Path in bucket to upload to
   * @param key     Name of encryption key to use
   * @param acl     Access control list to use
   */
  public ListenableFuture<S3File> upload(File file, String bucket, String object, String key, String acl)
  throws IOException
  {
    return upload(file, bucket, object, key, acl, false);
  }

  /**
   * Upload file to S3.
   *
   * @param file      File to upload
   * @param bucket    Bucket to upload to
   * @param object    Path in bucket to upload to
   * @param key       Name of encryption key to use
   * @param acl       Access control list to use
   * @param progress  Enable progress indicator
   */
  public ListenableFuture<S3File> upload(File file, String bucket, String object, String key, String acl, boolean progress)
  throws IOException
  {
    UploadCommand cmd =
      new UploadCommand(_s3Executor, _executor, file, _chunkSize, key, _keyProvider, acl, progress);
    configure(cmd);
    return cmd.run(bucket, object);
  }

  /**
   * Upload file to S3.
   *
   * @param file    File to upload
   * @param s3url   S3 object URL to upload to
   * @param key     Name of encryption key to use
   * @throws IllegalArgumentException If the s3url is not a valid S3 URL.
   */
  public ListenableFuture<S3File> upload(File file, URI s3url, String key)
  throws IOException
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return upload(file, bucket, object, key);
  }

  /**
   * Upload directory from S3
   *
   * @param file      Directory to upload
   * @param s3url     S3 URL to upload to
   * @param encKey    Encryption key to use
   * @param acl       Access control list to use
   * @param progress  Enable progress indicator
   * @throws IllegalArgumentException If the s3url is not a valid S3 URL.
   */
  @Deprecated
  public ListenableFuture<List<S3File>> uploadDirectory(File file, URI s3url, String encKey, CannedAccessControlList acl,
                                                        boolean progress)
          throws IOException, ExecutionException, InterruptedException {
    return this.uploadDirectory(file, s3url, encKey, acl.toString(), progress);
  }

  /**
   * Upload directory from S3
   *
   * @param file    Directory to upload
   * @param s3url   S3 URL to upload to
   * @param encKey  Encryption key to use
   * @param acl     Access control list to use
   * @throws IllegalArgumentException If the s3url is not a valid S3 URL.
   */
  public ListenableFuture<List<S3File>> uploadDirectory(File file, URI s3url, String encKey, String acl,
                                                        boolean progress)
          throws IOException, ExecutionException, InterruptedException {
    UploadDirectoryCommand cmd = new UploadDirectoryCommand(_s3Executor, _executor, this);
    configure(cmd);

    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return cmd.run(file, bucket, object, encKey, acl, progress);
  }

  /**
   * Delete a file from S3.  Note that this doesn't return an error if the
   * file doesn't exist.  If you care, use the exists() functions to check.
   *
   * @param bucket  Bucket to delete from
   * @param object  Path to be deleted from bucket
   */
  public ListenableFuture<S3File> delete(String bucket, String object)
  {
    DeleteCommand cmd =
      new DeleteCommand(_s3Executor, _executor);
    configure(cmd);
    return cmd.run(bucket, object);
  }

  /**
   * Delete a file from S3.  Note that this doesn't return an error if the
   * file doesn't exist.  If you care, use the exists() functions to check.
   *
   * @param s3url   Identifier of file to delete (e.g. s3://bucket/object)
   */
  public ListenableFuture<S3File> delete(URI s3url)
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return delete(bucket, object);
  }

  /**
   * List available buckets
   *
   */
  public ListenableFuture<List<Bucket>> listBuckets()
  {
      List<Bucket> result = _client.listBuckets();
      return Futures.immediateFuture(result);
  }

  /**
   * Check if a file exists in the bucket
   *
   * Returns a null future if the file does not exist, and an
   * exception if the metadata of the file could not be fetched for
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
   * Check if a file exists in the bucket
   *
   * Returns a null future if the file does not exist, and an
   * exception if the metadata of the file could not be fetched for
   * different reasons.
   */
  public ListenableFuture<ObjectMetadata> exists(URI s3url)
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return exists(bucket, object);
  }

  /**
   * Download file from S3
   *
   * @param file      File to download
   * @param bucket    Bucket to download from
   * @param object    Path in bucket to download
   * @param progress  Enable progress indicator
   */
  public ListenableFuture<S3File> download(File file, String bucket, String object, boolean progress)
  throws IOException
  {
    DownloadCommand cmd = new DownloadCommand(_s3Executor, _executor, file, _keyProvider, progress);
    configure(cmd);
    return cmd.run(bucket, object);
  }

  /**
   * Download file from S3
   *
   * @param file      File to download
   * @param bucket    Bucket to download from
   * @param object    Path in bucket to download
   */
  public ListenableFuture<S3File> download(File file, String bucket, String object)
          throws IOException
  {
    return download(file, bucket, object, false);
  }

  /**
   * Download file from S3
   *
   * @param file    File to download
   * @param s3url   S3 object URL to download from
   * @throws IllegalArgumentException If the s3url is not a valid S3 URL.
   */
  public ListenableFuture<S3File> download(File file, URI s3url)
          throws IOException
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return download(file, bucket, object);
  }

  /**
   * Download file from S3
   *
   * @param file    File to download
   * @param s3url   S3 object URL to download from
   * @throws IllegalArgumentException If the s3url is not a valid S3 URL.\
   * @param progress  Enable progress indicator
   */
  public ListenableFuture<S3File> download(File file, URI s3url, boolean progress)
          throws IOException
  {
    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return download(file, bucket, object, progress);
  }

  /**
   * Download directory from S3
   *
   * @param file    Directory to download
   * @param s3url   S3 object URL to download from
   * @throws IllegalArgumentException If the s3url is not a valid S3 URL.
   */
  public ListenableFuture<List<S3File>> downloadDirectory(
    File file, URI s3url, boolean recursive, boolean overwrite, boolean progress)
  throws IOException, ExecutionException, InterruptedException
  {
    DownloadDirectoryCommand cmd = new DownloadDirectoryCommand(_s3Executor, _executor, this);
    configure(cmd);

    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return cmd.run(file, bucket, object, recursive, overwrite, progress);
  }

  /**
   * List object in S3
   *
   * @param bucket  Bucket to check
   * @param object  Path in bucket to check
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
   * @param bucket  Bucket to check
   * @param object  Path in bucket to check
   */
  public ListenableFuture<List<S3File>> listObjectsAndDirs(
    String bucket, String prefix, boolean recursive)
  {
    ListObjectsAndDirsCommand cmd =
            new ListObjectsAndDirsCommand(_s3Executor, _executor);
    configure(cmd);
    return cmd.run(bucket, prefix, recursive);
  }

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
