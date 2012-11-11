package com.logicblox.s3lib;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
    
  public ListenableFuture<?> upload(File file, String bucket, String object, String key)
  throws FileNotFoundException, IOException
  {
    UploadCommand cmd =
      new UploadCommand(_s3Executor, _executor, file, _chunkSize, key, _keyProvider);
    cmd.setAWSCredentials(_credentials);
    return cmd.run(bucket, object); 
  }

  public ListenableFuture<?> download(File file, String bucket, String object)
  throws IOException
  {
    DownloadCommand cmd = new DownloadCommand(_s3Executor, _executor, file, _keyProvider);
    
    cmd.setAWSCredentials(_credentials);
    return cmd.run(bucket, object); 
  }
}