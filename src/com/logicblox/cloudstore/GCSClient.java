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

package com.logicblox.cloudstore;

import com.amazonaws.services.s3.AmazonS3;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * Provides the client for accessing the Google Cloud Storage web service.
 * <p>
 * Captures the full configuration independent of concrete operations like uploads or downloads.
 * <p>
 * For more information about Google Cloud Storage, please see
 * <a href="https://cloud.google.com/storage/">https://cloud.google
 * .com/storage/</a>
 */
public class GCSClient
  implements CloudStoreClient
{
  private static final String _GCS_JSON_API_ENDPOINT = "https://www.googleapis.com";
  private static final String _GCS_XML_API_ENDPOINT = "https://storage.googleapis.com";


  private final Storage _gcsClient;
  private final S3ClientDelegatee _s3Client;
  private final GCSAclHandler _aclHandler;
  private final GCSStorageClassHandler _storageClassHandler;

  /**
   * @param internalGCSClient Low-level GCS-client
   * @param internalS3Client  Low-level S3-client
   * @param apiExecutor       Executor for executing GCS API calls
   * @param internalExecutor  Executor for internally initiating uploads
   * @param keyProvider       Provider of encryption keys
   */
  GCSClient(
    Storage internalGCSClient, AmazonS3 internalS3Client,
    ListeningExecutorService apiExecutor, ListeningScheduledExecutorService internalExecutor,
    KeyProvider keyProvider)
  {
    _s3Client = new S3ClientDelegatee(internalS3Client, apiExecutor, internalExecutor, keyProvider);
    _gcsClient = internalGCSClient;
    setEndpoint(_GCS_XML_API_ENDPOINT);
    _aclHandler = new GCSAclHandler();
    _storageClassHandler = new GCSStorageClassHandler();
  }

  /**
   * Canned ACLs handling
   */
  public static final List<String> ALL_CANNED_ACLS = Arrays.asList("projectPrivate", "private",
    "publicRead", "publicReadWrite", "authenticatedRead", "bucketOwnerRead",
    "bucketOwnerFullControl");

  /**
   * {@code CANNED_ACLS_DESC_CONST} has to be a compile-time String constant expression. That's why
   * e.g. we cannot re-use {@code ALL_CANNED_ACLS} to construct it.
   */
  static final String CANNED_ACLS_DESC_CONST = "For Google Cloud Storage, " +
    "choose one of: projectPrivate, private, publicRead, publicReadWrite," +
    " authenticatedRead, bucketOwnerRead, bucketOwnerFullControl.";

  @Override
  public void setRetryCount(int retryCount)
  {
    _s3Client.setRetryCount(retryCount);
  }

  @Override
  public void setRetryClientException(boolean retry)
  {
    _s3Client.setRetryClientException(retry);
  }

  @Override
  public void setEndpoint(String endpoint)
  {
    _s3Client.setEndpoint(endpoint);
  }

  @Override
  public String getScheme()
  {
    return "gs";
  }

  @Override
  public ListeningExecutorService getApiExecutor()
  {
    return _s3Client.getApiExecutor();
  }

  @Override
  public ListeningScheduledExecutorService getInternalExecutor()
  {
    return _s3Client.getInternalExecutor();
  }

  @Override
  public OptionsBuilderFactory getOptionsBuilderFactory()
  {
    return new OptionsBuilderFactory(this);
  }

  @Override
  public KeyProvider getKeyProvider()
  {
    return _s3Client.getKeyProvider();
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

  static void patchMetaData(
    Storage gcsStorage, String bucket, String key, Map<String, String> userMetadata)
    throws IOException
  {
    StorageObject sobj = new StorageObject().setName(key).setMetadata(userMetadata);
    Storage.Objects.Patch cmd = gcsStorage.objects().patch(bucket, key, sobj);
    cmd.execute();
  }

  @Override
  public ListenableFuture<StoreFile> upload(UploadOptions options)
    throws IOException
  {
    return _s3Client.upload(options);
  }

  @Override
  public ListenableFuture<List<StoreFile>> uploadDirectory(UploadOptions options)
    throws IOException, ExecutionException, InterruptedException
  {
    return _s3Client.uploadDirectory(options);
  }

  @Override
  public ListenableFuture<List<StoreFile>> deleteDir(DeleteOptions opts)
    throws InterruptedException, ExecutionException
  {
    return _s3Client.deleteDir(opts);
  }

  @Override
  public ListenableFuture<StoreFile> delete(DeleteOptions opts)
  {
    return _s3Client.delete(opts);
  }

  @Override
  public ListenableFuture<List<Bucket>> listBuckets()
  {
    return _s3Client.listBuckets();
  }

  @Override
  public ListenableFuture<Metadata> exists(ExistsOptions options)
  {
    return _s3Client.exists(options);
  }

  @Override
  public ListenableFuture<StoreFile> download(DownloadOptions options)
    throws IOException
  {
    return _s3Client.download(options);
  }

  @Override
  public ListenableFuture<List<StoreFile>> downloadDirectory(DownloadOptions options)
    throws IOException, ExecutionException, InterruptedException
  {
    return _s3Client.downloadDirectory(options);
  }

  @Override
  public ListenableFuture<StoreFile> copy(CopyOptions options)
  {
    return _s3Client.copy(options);
  }

  @Override
  public ListenableFuture<List<StoreFile>> copyToDir(CopyOptions options)
    throws InterruptedException, ExecutionException, IOException
  {
    return _s3Client.copyToDir(options);
  }

  @Override
  public ListenableFuture<StoreFile> rename(RenameOptions options)
  {
    return _s3Client.rename(options);
  }

  @Override
  public ListenableFuture<List<StoreFile>> renameDirectory(RenameOptions options)
    throws InterruptedException, ExecutionException, IOException
  {
    return _s3Client.renameDirectory(options);
  }

  @Override
  public ListenableFuture<List<StoreFile>> listObjects(ListOptions lsOptions)
  {
    return _s3Client.listObjects(lsOptions);
  }

  @Override
  public ListenableFuture<List<Upload>> listPendingUploads(PendingUploadsOptions options)
  {
    throw new UnsupportedOperationException("listPendingUploads is not " + "supported.");
  }

  @Override
  public ListenableFuture<List<Void>> abortPendingUploads(PendingUploadsOptions options)
  {
    throw new UnsupportedOperationException("abortPendingUpload is not " + "supported.");
  }

  @Override
  public ListenableFuture<StoreFile> addEncryptionKey(EncryptionKeyOptions options)
    throws IOException
  {
    return _s3Client.addEncryptionKey(options);
  }

  @Override
  public ListenableFuture<StoreFile> removeEncryptionKey(EncryptionKeyOptions options)
    throws IOException
  {
    return _s3Client.removeEncryptionKey(options);
  }

  @Override
  public void shutdown()
  {
    _s3Client.shutdown();
  }

  private class S3ClientDelegatee
    extends S3Client
  {
    public S3ClientDelegatee(
      AmazonS3 internalS3Client, ListeningExecutorService apiExecutor,
      ListeningScheduledExecutorService internalExecutor, KeyProvider keyProvider)
    {
      super(internalS3Client, apiExecutor, internalExecutor, keyProvider);
    }

    void configure(Command cmd)
    {
      cmd.setRetryClientException(_retryClientException);
      cmd.setRetryCount(_retryCount);
      cmd.setS3Client(_client);
      cmd.setGCSClient(_gcsClient);
      cmd.setScheme("gs://");
    }

    /**
     * Upload file to GCS.
     *
     * @param options Upload options
     */
    @Override
    public ListenableFuture<StoreFile> upload(UploadOptions options)
      throws IOException
    {
      GCSUploadCommand cmd = new GCSUploadCommand(options);
      _s3Client.configure(cmd);
      return cmd.run();
    }

    /**
     * Upload directory to GCS.
     *
     * @param options Upload options
     */
    @Override
    public ListenableFuture<List<StoreFile>> uploadDirectory(UploadOptions options)
      throws IOException, ExecutionException, InterruptedException
    {
      UploadDirectoryCommand cmd = new UploadDirectoryCommand(options);
      _s3Client.configure(cmd);
      return cmd.run();
    }

    @Override
    public ListenableFuture<List<StoreFile>> listObjects(ListOptions options)
    {
      GCSListCommand cmd = new GCSListCommand(options);
      configure(cmd);
      return cmd.run();
    }

    @Override
    public ListenableFuture<StoreFile> copy(CopyOptions options)
    {
      GCSCopyCommand cmd = new GCSCopyCommand(options);
      configure(cmd);
      return cmd.run();
    }

    @Override
    public ListenableFuture<List<StoreFile>> copyToDir(CopyOptions options)
      throws IOException
    {
      GCSCopyDirCommand cmd = new GCSCopyDirCommand(options);
      configure(cmd);
      return cmd.run();
    }

    @Override
    protected S3AddEncryptionKeyCommand createAddKeyCommand(EncryptionKeyOptions options)
      throws IOException
    {
      S3AddEncryptionKeyCommand cmd = super.createAddKeyCommand(options);
      configure(cmd);
      return cmd;
    }

    @Override
    protected S3RemoveEncryptionKeyCommand createRemoveKeyCommand(EncryptionKeyOptions options)
      throws IOException
    {
      S3RemoveEncryptionKeyCommand cmd = super.createRemoveKeyCommand(options);
      configure(cmd);
      return cmd;
    }


  }

  @Override
  public boolean hasBucket(String bucketName)
  {
    throw new UnsupportedOperationException("FIXME - not yet implemented");
  }

  @Override
  public void createBucket(String bucketName)
  {
    throw new UnsupportedOperationException("FIXME - not yet implemented");
  }

  @Override
  public void destroyBucket(String bucketName)
  {
    throw new UnsupportedOperationException("FIXME - not yet implemented");
  }

  // needed for testing
  void setKeyProvider(KeyProvider kp)
  {
    _s3Client.setKeyProvider(kp);
  }
}
