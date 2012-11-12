package com.logicblox.s3lib;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * Captures the full configuration independent of concrete uploads and
 * downloads.
 */
public class S3Client
{
  private ListeningExecutorService _s3Executor;
  private ListeningExecutorService _executor;
  private long _chunkSize;
  private AWSCredentialsProvider _credentials;
  private KeyProvider _keyProvider;

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
    ListeningExecutorService executor,
    long chunkSize,
    KeyProvider keyProvider)
  {
    _executor = executor;
    _s3Executor = s3Executor;
    _chunkSize = chunkSize;
    _keyProvider = keyProvider;
    _credentials = credentials;
  }

  /**
   * Upload file to S3 without encryption.
   *
   * @param file    File to upload
   * @param s3url   S3 object URL (using same syntax as s3cmd)
   */
  public ListenableFuture<?> upload(File file, URI s3url)
  throws FileNotFoundException, IOException
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
  public ListenableFuture<?> upload(File file, String bucket, String object)
  throws FileNotFoundException, IOException
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
  public ListenableFuture<?> upload(File file, String bucket, String object, String key)
  throws FileNotFoundException, IOException
  {
    UploadCommand cmd =
      new UploadCommand(_s3Executor, _executor, file, _chunkSize, key, _keyProvider);
    cmd.setAWSCredentials(_credentials);
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
  public ListenableFuture<?> upload(File file, URI s3url, String key)
  throws FileNotFoundException, IOException
  {
    if(!"s3".equals(s3url.getScheme()))
      throw new IllegalArgumentException("S3 object URL needs to have 's3' as scheme");

    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    return upload(file, bucket, object, key);
  }

  /**
   * Download file from S3
   *
   * @param file    File to download
   * @param bucket  Bucket to download from
   * @param object  Path in bucket to download
   */
  public ListenableFuture<?> download(File file, String bucket, String object)
  throws IOException
  {
    DownloadCommand cmd = new DownloadCommand(_s3Executor, _executor, file, _keyProvider);
    
    cmd.setAWSCredentials(_credentials);
    return cmd.run(bucket, object); 
  }

  /**
   * Download file from S3
   *
   * @param file    File to download
   * @param s3url   S3 object URL to download from
   * @throws IllegalArgumentException If the s3url is not a valid S3 URL.
   */
  public ListenableFuture<?> download(File file, URI s3url)
  throws IOException
  {
    if(!"s3".equals(s3url.getScheme()))
      throw new IllegalArgumentException("S3 object URL needs to have 's3' as scheme");

    DownloadCommand cmd = new DownloadCommand(_s3Executor, _executor, file, _keyProvider);

    String bucket = Utils.getBucket(s3url);
    String object = Utils.getObjectKey(s3url);
    
    cmd.setAWSCredentials(_credentials);
    return cmd.run(bucket, object); 
  }
}