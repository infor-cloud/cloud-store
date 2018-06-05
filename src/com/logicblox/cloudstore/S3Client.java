/*
  Copyright 2018, Infor Inc.

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

package com.logicblox.cloudstore;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;


/**
 * Provides the client for accessing the Amazon S3 or S3-compatible web service.
 * <p>
 * Captures the full configuration independent of concrete operations like uploads or downloads.
 * <p>
 * For more information about Amazon S3, please see <a href="http://aws.amazon.com/s3">http://aws
 * .amazon.com/s3</a>
 */
public class S3Client
  implements CloudStoreClient
{
  /**
   * Responsible for executing S3 HTTP API calls asynchronously. It, also, determines the level of
   * parallelism of an operation, e.g. number of threads used for uploading/downloading a file.
   */
  ListeningExecutorService _s3Executor;

  /**
   * Responsible for executing internal cloud-store tasks asynchronously. Such tasks include file
   * I/O, file encryption, file splitting and error handling.
   */
  ListeningScheduledExecutorService _executor;

  /** The AWS credentials to use when making requests to the service. */
  AWSCredentialsProvider _credentials;

  /**
   * Low-level AWS S3 client responsible for authentication, proxying and HTTP requests.
   */
  AmazonS3 _client;

  /**
   * The provider of key-pairs used to encrypt/decrypt files during upload/download.
   */
  KeyProvider _keyProvider;

  S3AclHandler _aclHandler;
  private S3StorageClassHandler _storageClassHandler;

  /** Whether or not to retry client side exception unconditionally. */
  boolean _retryClientException = false;

  /** The number of times we want to retry in case of an error. */
  int _retryCount = 15;

  /**
   * Constructs a new high-level S3 client to invoke operations on S3 or compatible service.
   * <p>
   * Objects created by this constructor will:
   * <ul>
   *   <li>use a thread pool of 10 threads to execute S3 HTTP API calls asynchronously</li>
   *   <li>use a thread pool of 50 threads to execute internal tasks asynchronously</li>
   *   <li>use a 5MB chunk-size for multi-part upload/download operations</li>
   *   <li>search {@code ~/.cloudstore-keys} for specified cryptographic key-pair used to
   *   encrypt/decrypt files during upload/download</li>
   *   <li>retry a task 10 times in case of an error</li>
   * </ul>
   *
   * @param s3Client Low-level AWS S3 client responsible for authentication, proxying and HTTP
   *                 requests
   * @see S3Client#S3Client(AmazonS3, ListeningExecutorService,
   * ListeningScheduledExecutorService, KeyProvider)
   */
  S3Client(AmazonS3 s3Client)
  {
    this(s3Client, Utils.createApiExecutor(10), Utils.createInternalExecutor(50),
      Utils.createKeyProvider(Utils.getDefaultKeyDirectory()));
    this.setRetryCount(10);
  }

  /**
   * Constructs a new high-level S3 client to invoke operations on S3 or compatible service.
   *
   * @param s3Client    Low-level AWS S3 client responsible for authentication, proxying and HTTP
   *                    requests
   * @param keyProvider The provider of key-pairs used to encrypt/decrypt files during
   *                    upload/download.
   * @see S3Client#S3Client(AmazonS3)
   * @see S3Client#S3Client(AmazonS3, ListeningExecutorService, ListeningScheduledExecutorService, KeyProvider)
   */
  S3Client(AmazonS3 s3Client, KeyProvider keyProvider)
  {
    this(s3Client, Utils.createApiExecutor(10), Utils.createInternalExecutor(50), keyProvider);
    this.setRetryCount(10);
  }

  /**
   * Constructs a new high-level S3 client to invoke operations on S3 or compatible service.
   *
   * @param credentials The AWS credentials to use when making requests to the service.
   * @param s3Executor  Responsible for executing S3 HTTP API calls asynchronously. It, also,
   *                    determines the level of parallelism of an operation, e.g. number of threads
   *                    used for uploading/downloading a file.
   * @param executor    Responsible for executing internal cloud-store tasks asynchrounsly. Such
   *                    tasks include file I/O, file encryption, file splitting and error handling.
   * @param keyProvider The provider of key-pairs used to encrypt/decrypt files during
   *                    upload/download.
   * @see S3Client#S3Client(AmazonS3)
   * @see S3Client#S3Client(AmazonS3, ListeningExecutorService, ListeningScheduledExecutorService, KeyProvider)
   */
  S3Client(
    AWSCredentialsProvider credentials, ListeningExecutorService s3Executor,
    ListeningScheduledExecutorService executor, KeyProvider keyProvider)
  {
    _executor = executor;
    _s3Executor = s3Executor;
    _keyProvider = keyProvider;
    _credentials = credentials;
    if(_credentials != null)
    {
      _client = new AmazonS3Client(_credentials);
    }
    else
    {
      _client = new AmazonS3Client();
    }
  }

  /**
   * Constructs a new high-level S3 client to invoke operations on S3 or compatible service.
   *
   * @param s3Client    Low-level AWS S3 client responsible for authentication, proxying and HTTP
   *                    requests
   * @param s3Executor  Responsible for executing S3 HTTP API calls asynchronously. It, also,
   *                    determines the level of parallelism of an operation, e.g. number of threads
   *                    used for uploading/downloading a file.
   * @param executor    Responsible for executing internal cloud-store tasks asynchrounsly. Such
   *                    tasks include file I/O, file encryption, file splitting and error handling.
   * @param keyProvider The provider of key-pairs used to encrypt/decrypt files during
   *                    upload/download.
   * @see S3Client#S3Client(AmazonS3)
   */
  S3Client(
    AmazonS3 s3Client, ListeningExecutorService s3Executor,
    ListeningScheduledExecutorService executor, KeyProvider keyProvider)
  {
    _executor = executor;
    _s3Executor = s3Executor;
    _keyProvider = keyProvider;
    _client = s3Client;
    _aclHandler = new S3AclHandler(_client);
    _storageClassHandler = new S3StorageClassHandler();
  }

  /**
   * Canned ACLs handling
   */
  public static final List<String> ALL_CANNED_ACLS = initCannedAcls();

  /**
   * {@code CANNED_ACLS_DESC_CONST} has to be a compile-time String constant expression. That's why
   * e.g. we cannot re-use {@code ALL_CANNED_ACLS} to construct it.
   */
  static final String CANNED_ACLS_DESC_CONST = "For Amazon S3, choose one of: " +
    "private, public-read, public-read-write, authenticated-read, " +
    "bucket-owner-read, bucket-owner-full-control.";

  private static List<String> initCannedAcls()
  {
    List<String> l = new ArrayList<>();
    for(CannedAccessControlList acl : CannedAccessControlList.values())
      l.add(acl.toString());
    return l;
  }

  /**
   * {@code STORAGE_CLASSES_DESC_CONST} has to be a compile-time String constant expression. That's
   * why e.g. we cannot re-use {@code StorageClass.values()} to construct it.
   */
  static final String STORAGE_CLASSES_DESC_CONST = "For Amazon S3, choose one of: " +
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
  public OptionsBuilderFactory getOptionsBuilderFactory()
  {
    return new OptionsBuilderFactory(this);
  }

  @Override
  public KeyProvider getKeyProvider()
  {
    return _keyProvider;
  }

  @Override
  public AclHandler getAclHandler()
  {
    return _aclHandler;
  }

  @Override
  public StorageClassHandler getStorageClassHandler()
  {
    return _storageClassHandler;
  }

  static AccessControlList getObjectAcl(AmazonS3 client, String bucket, String key)
    throws AmazonS3Exception
  {
    return client.getObjectAcl(bucket, key);
  }

  static CannedAccessControlList getCannedAcl(String value)
  {
    for(CannedAccessControlList acl : CannedAccessControlList.values())
    {
      if(acl.toString().equals(value))
      {
        return acl;
      }
    }
    return null;
  }

  void configure(Command cmd)
  {
    cmd.setRetryClientException(_retryClientException);
    cmd.setRetryCount(_retryCount);
    cmd.setS3Client(_client);
    cmd.setScheme("s3://");
  }

  @Override
  public ListenableFuture<StoreFile> upload(UploadOptions options)
    throws IOException
  {
    S3UploadCommand cmd = new S3UploadCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<StoreFile>> uploadRecursively(UploadOptions options)
    throws IOException, ExecutionException, InterruptedException
  {
    UploadRecursivelyCommand cmd = new UploadRecursivelyCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<StoreFile>> deleteDirectory(DeleteOptions options)
    throws InterruptedException, ExecutionException
  {
    DeleteDirCommand cmd = new DeleteDirCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<StoreFile> delete(DeleteOptions options)
  {
    S3DeleteCommand cmd = new S3DeleteCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<Bucket>> listBuckets()
  {
    List<com.amazonaws.services.s3.model.Bucket> s3Buckets = _client.listBuckets();
    List<Bucket> buckets = new ArrayList<Bucket>();
    for(com.amazonaws.services.s3.model.Bucket b : s3Buckets)
    {
      buckets.add(new Bucket(b.getName(), b.getCreationDate(),
        new Owner(b.getOwner().getId(), b.getOwner().getDisplayName())));
    }
    return Futures.immediateFuture(buckets);
  }

  @Override
  public ListenableFuture<Bucket> getBucket(String bucketName)
    throws ExecutionException, InterruptedException
  {
    Optional<Bucket> bucket = listBuckets().get()
      .stream()
      .filter(p -> p.getName().equals(bucketName))
      .findFirst();

    Bucket b = bucket.orElseThrow(
      () -> new UsageException("Bucket \"" + bucketName + "\" not found"));
    return Futures.immediateFuture(b);
  }

  @Override
  public ListenableFuture<Metadata> exists(ExistsOptions options)
  {
    S3ExistsCommand cmd = new S3ExistsCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<StoreFile> download(DownloadOptions options)
    throws IOException
  {
    S3DownloadCommand cmd = new S3DownloadCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<StoreFile>> downloadRecursively(DownloadOptions options)
    throws IOException, ExecutionException, InterruptedException
  {
    DownloadRecursivelyCommand cmd = new DownloadRecursivelyCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<StoreFile> copy(CopyOptions options)
  {
    S3CopyCommand cmd = new S3CopyCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<StoreFile>> copyRecursively(CopyOptions options)
    throws InterruptedException, ExecutionException, IOException
  {
    S3CopyRecursivelyCommand cmd = new S3CopyRecursivelyCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<StoreFile> rename(RenameOptions options)
  {
    RenameCommand cmd = new RenameCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<StoreFile>> renameDirectory(RenameOptions options)
    throws InterruptedException, ExecutionException, IOException
  {
    RenameDirectoryCommand cmd = new RenameDirectoryCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<StoreFile>> listObjects(ListOptions options)
  {
    ListenableFuture<List<StoreFile>> results = null;
    if(options.versionsIncluded())
    {
      S3ListVersionsCommand cmd = new S3ListVersionsCommand(options);
      configure(cmd);
      results = cmd.run();
    }
    else
    {
      S3ListCommand cmd = new S3ListCommand(options);
      configure(cmd);
      results = cmd.run();
    }
    return results;
  }

  @Override
  public ListenableFuture<List<Upload>> listPendingUploads(PendingUploadsOptions options)
  {
    S3ListPendingUploadsCommand cmd = new S3ListPendingUploadsCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<List<Void>> abortPendingUploads(PendingUploadsOptions options)
  {
    S3AbortPendingUploadsCommand cmd = new S3AbortPendingUploadsCommand(options);
    configure(cmd);
    return cmd.run();
  }

  @Override
  public ListenableFuture<StoreFile> addEncryptionKey(EncryptionKeyOptions options)
    throws IOException
  {
    S3AddEncryptionKeyCommand cmd = createAddKeyCommand(options);
    return cmd.run();
  }

  protected S3AddEncryptionKeyCommand createAddKeyCommand(EncryptionKeyOptions options)
    throws IOException
  {
    S3AddEncryptionKeyCommand cmd = new S3AddEncryptionKeyCommand(options);
    configure(cmd);
    return cmd;
  }

  @Override
  public ListenableFuture<StoreFile> removeEncryptionKey(EncryptionKeyOptions options)
    throws IOException
  {
    S3RemoveEncryptionKeyCommand cmd = createRemoveKeyCommand(options);
    return cmd.run();
  }

  protected S3RemoveEncryptionKeyCommand createRemoveKeyCommand(EncryptionKeyOptions options)
    throws IOException
  {
    S3RemoveEncryptionKeyCommand cmd = new S3RemoveEncryptionKeyCommand(options);
    configure(cmd);
    return cmd;
  }


  /**
   * Returns cloudstore package version in this format: "Version.RevNum-RevHash_BuildDatetime"
   * <p>
   * Example: "1.0.272-2dab7d1c9c69_201502181542"
   *
   * @return library version
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
