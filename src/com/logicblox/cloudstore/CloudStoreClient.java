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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * This interface provides a general client API for accessing cloud stores like Amazon S3 or 
 * Google Cloud Storage.  This should be considered the primary public interface used by clients 
 * of the cloud-store library.  Use the {@link Utils#createCloudStoreClient(String)} method or 
 * another of the {@code Utils.createCloudStoreClient} overloads to create an appropriate 
 * object that implements this interface for one of the supported
 * cloud storage services, for example {@code Utils.createCloudStoreClient("s3")}.
 */
public interface CloudStoreClient
{
  /**
   * Sets the number of times an operation can fail and be retried before the operation will be 
   * cancelled.
   *
   * @param retryCount Number of retries
   */
  void setRetryCount(int retryCount);

  /**
   * By default, client-side errors (like HTTP 404) will not cause operations to be retried. Setting
   * retryClientException to true will cause client-side errors to be retried in the same manner as
   * server-side errors.
   */
  void setRetryClientException(boolean retry);

  /**
   * Sets the API endpoint this client should issue the requests to.
   * <p>
   * This is mostly useful to test different cloud store services with compatible APIs or for
   * unit-testing purposes (e.g. mocks or minio).  Setting an endpoint is not necessary when using
   * standard AWS S3 or GCS stores.
   *
   * @param endpoint API endpoint, i.e, "http://127.0.0.1:9000/"
   */
  void setEndpoint(String endpoint);

  /**
   * Returns the scheme of the backend storage service (e.g. "s3" or "gs")
   */
  String getScheme();

  /**
   * Returns the executor responsible for issuing HTTP API calls against the backend storage service
   * asynchronously. It also determines the level of parallelism of an operation, e.g. number of
   * threads used for uploading/downloading a file.
   */
  ListeningExecutorService getApiExecutor();

  /**
   * Returns the executor responsible for executing internal cloud-store tasks asynchronously. Such
   * tasks include file I/O, file encryption, file splitting, and error handling.
   */
  ListeningScheduledExecutorService getInternalExecutor();

  /**
   * Returns a factory used to create builder objects for all command options.
   */
  OptionsBuilderFactory getOptionsBuilderFactory();

  /**
   * Returns the provider of encryption key pairs used to encrypt or decrypt files during
   * upload or download.
   */
  KeyProvider getKeyProvider();

  /**
   * Returns a service-specific object used to abstract over access control functionality
   * provided by different services.
   */
  AclHandler getAclHandler();

  /**
   * Returns a service-specific object used to query a service about storage classes.
   */
  StorageClassHandler getStorageClassHandler();

  /**
   * Upload a file from the local file system to a cloud store service.
   * <p>
   * The destination bucket must already exist and the caller must have write permission
   * to the bucket to upload a file.  Checksum validation is done after the transfer of the
   * file (and each part if the service uses multi-part transfers) to ensure no data corruption
   * occurs during the transfer.  If no exceptions are thrown, a future is returned that when
   * completed means that the file has been successfully copied to the service.  The
   * {@link StoreFile} wrapped by the future will contain metadata about the uploaded file.
   * <p>
   * For services that support multi-part uploads, the chunk size in the specified options
   * will control the size of each part to be uploaded.  The level of parallelism is controlled
   * by the executor used to create the CloudStoreClient interface.  See {@link #getApiExecutor()}
   * and {@link Utils#createCloudStoreClient(String)}.
   *
   * @param options Set of options that control the upload operation
   */
  ListenableFuture<StoreFile> upload(UploadOptions options)
    throws IOException;

  /**
   * Upload a set of files from a directory in the local file system to a cloud store service.
   * <p>
   * The destination bucket must already exist and the caller must have write permission
   * to the bucket to upload a file.  Checksum validation is done after the transfer of each
   * file (and each part if the service uses multi-part transfers) to ensure no data corruption
   * occurs during the transfer.  If no exceptions are thrown, a future is returned that when
   * completed means that all the files have been successfully copied to the service. The list of
   * {@link StoreFile StoreFiles} wrapped by the future will contain metadata about the 
   * uploaded files.
   * <p>
   * For services that support multi-part uploads, the chunk size in the specified options
   * will control the size of each part to be uploaded.  The level of parallelism is controlled
   * by the executor used to create the CloudStoreClient interface.  See {@link #getApiExecutor()}
   * and {@link Utils#createCloudStoreClient(String)}.
   * <p>
   * Note that uploading from directories that are symbolically linked to another directory
   * is not supported.
   *
   * @param options Set of options that control the upload operation
   */
  ListenableFuture<List<StoreFile>> uploadDirectory(UploadOptions options)
    throws IOException, ExecutionException, InterruptedException;

  /**
   * Delete a single file from a cloud store service.
   * <p>
   * This operation returns a future that when completed successfully will contain a
   * {@link StoreFile} with metadata about the deleted file.  If a file matching the
   * options is not found, an exception will be thrown.
   *
   * @param opts DeleteOptions that specify what to delete
   */
  ListenableFuture<StoreFile> delete(DeleteOptions opts);

  /**
   * Delete a set of files from a cloud store service, where all the files match
   * a particular prefix.
   *
   * @param opts DeleteOptions that specify what to delete
   */
  ListenableFuture<List<StoreFile>> deleteDir(DeleteOptions opts)
    throws InterruptedException, ExecutionException;

  /** 
   * List all buckets visible to the current user, returning a future that when completed
   * will contain a list of {@link Bucket Buckets} with name and owner of each bucket.
   */
  ListenableFuture<List<Bucket>> listBuckets();

  /**
   * Checks existance of a specific file in a cloud store service.
   * <p>
   * Returns a null future if the file does not exist.
   * <p>
   * Throws an exception if the metadata of the file could not be fetched for different reasons.
   *
   * @param options ExistsOptions
   */
  ListenableFuture<Metadata> exists(ExistsOptions options);

  /**
   * Downloads a file according to {@code options}. For more details check {@link
   * com.logicblox.cloudstore.DownloadOptions}.
   * <p>
   * If the {@code object} was encrypted by cloud-store at the client-side, this method will try to
   * decrypt it automatically. It knows which key to use by checking {@code object}'s metadata.
   * <p>
   * The specified bucket must already exist and the caller must have read permission to the {@code
   * bucket}.
   * <p>
   * By default, if {@code object}'s size is bigger than the {@code chunkSize} chosen during the
   * upload, then multiple range GETs will be issued, effectively downloading different parts of the
   * file concurrently.
   * <p>
   * The level of parallelism depends on the {@code s3Executor}. Ideally, different parts are
   * downloaded by different threads.
   * <p>
   * Since a file can be uploaded, updated and/or copied by any tool is not always safe and
   * efficient to validate its checksum, since there is no easy way to detect each tool's chosen
   * chunk size (which affects object's ETag).
   * <p>
   * Currently, this client tries to do checksum validation when it has good indication that a file
   * was uploaded by cloud-store and/or the file can been fully downloaded using a single range
   * GET.
   * <p>
   * If during this call an exception wasn't thrown, the entire object has supposedly been stored
   * successfully.
   *
   * @param options Download options
   * @see DownloadOptions
   */
  ListenableFuture<StoreFile> download(DownloadOptions options)
    throws IOException;

  /**
   * Downloads a conceptual directory according to {@code options}. For more details check {@link
   * com.logicblox.cloudstore.DownloadOptions}.
   * <p>
   * If {@code recursive} is true, then all objects under {@code uri} will be downloaded. Otherwise,
   * only the first-level objects will be downloaded.
   * <p>
   * If {@code overwrite} is true, then newly downloaded files is possible to overwrite existing
   * local files.
   * <p>
   * Each individual file is downloaded with {@link CloudStoreClient#download(DownloadOptions)}.
   *
   * @param options Download options
   */
  ListenableFuture<List<StoreFile>> downloadDirectory(DownloadOptions options)
    throws IOException, ExecutionException, InterruptedException;

  /**
   * Copies an object according to {@code options}. For more details check {@link
   * com.logicblox.cloudstore.CopyOptions}.
   * <p>
   * The source bucket must already exist and the caller must have read permission to it.
   * Respectively, the destination bucket must already exist and the caller must have write
   * permission to it.
   *
   * @param options CopyOptions
   * @see CloudStoreClient#copyToDir(CopyOptions)
   * @see CopyOptions
   */
  ListenableFuture<StoreFile> copy(CopyOptions options);

  /**
   * Copies all keys that would be returned by the list operation on the source prefix URI,
   * according to {@code options}. The destination URI has to be a directory (i.e. end with '/').
   * For more details check {@link com.logicblox.cloudstore.CopyOptions}.
   * <p>
   * The source bucket must already exist and the caller must have read permission to it.
   * Respectively, the destination bucket must already exist and the caller must have write
   * permission to it.
   *
   * @param options CopyOptions
   * @see CloudStoreClient#copy(CopyOptions)
   * @see CopyOptions
   */
  ListenableFuture<List<StoreFile>> copyToDir(CopyOptions options)
    throws InterruptedException, ExecutionException, IOException;

  /**
   * Renames an object according to {@code options}. For more details check {@link
   * com.logicblox.cloudstore.RenameOptions}.  Note that this is equivalent to a copy operation
   * followed by a delete operation.
   * <p>
   * The source bucket must already exist and the caller must have read permission to it.
   * Respectively, the destination bucket must already exist and the caller must have write
   * permission to it.
   *
   * @param options RenameOptions - provides parameters for the rename operation
   * @see CloudStoreClient#renameDirectory(RenameOptions)
   * @see RenameOptions
   */
  ListenableFuture<StoreFile> rename(RenameOptions options);

  /**
   * Renames all keys that share a prefix to have another prefix -- in other words, rename a folder.
   * All keys that would be returned by a list operation on the source key will be renamed.
   * <p>
   * The source bucket must already exist and the caller must have read permission to it.
   * Respectively, the destination bucket must already exist and the caller must have write
   * permission to it.
   * <p>
   * Both the source and destination keys should be folders.  There must be some keys that match the
   * source founder, but the destination folder does not need to exist.
   *
   * @param options RenameOptions
   * @see CloudStoreClient#rename(RenameOptions)
   * @see RenameOptions
   */
  ListenableFuture<List<StoreFile>> renameDirectory(RenameOptions options)
    throws InterruptedException, ExecutionException, IOException;

  /**
   * Returns a list of summary information about the objects whose keys start with {@code prefix}
   * and belong in the specified {@code bucket}.
   * <p>
   * If {@code recursive} is enabled, then all objects of all subdirectories are going to be
   * included in the results too.
   * <p>
   * List results are returned in lexicographic order.
   * <p>
   * If {@code exclude_dirs} is enabled, then Directories are <i>not</i> included in the results.
   * <p>
   * If {@code include-versions} is enabled, then versions of objects will be included in the
   * results.
   *
   * @param lsOptions Class contains all needed options for ls command
   * @see CloudStoreClient#listObjects(ListOptions)
   */
  ListenableFuture<List<StoreFile>> listObjects(ListOptions lsOptions);

  /**
   * Returns a list of the pending uploads to object keys that start with {@code
   * options.getObjectKey()}, inside {@code options.getBucketName()}.
   * <p>
   * Returned uploads' {@code Upload#getId()} and {@code Upload#getInitiationDate()} are useful to
   * abort them via {@link CloudStoreClient#abortPendingUploads(PendingUploadsOptions)}
   *
   * @param options Bucket and key/prefix are specified here
   */
  ListenableFuture<List<Upload>> listPendingUploads(PendingUploadsOptions options);

  /**
   * Aborts pending uploads depending on the passed {@code options}.
   * <p>
   * If only the upload id is specified, then only this specific pending upload will be aborted.
   * Such ids can be found via {@link CloudStoreClient#listPendingUploads}.
   * <p>
   * If only the date is specified ({@code options.getDate()}), then all pending uploads that were
   * initiated before this date will be aborted.
   * <p>
   * If both upload id and date are specified, then only this specific pending upload will be
   * aborted, provided it has been initiated before the specified date.
   *
   * @param options PendingUploadsOptions
   * @see CloudStoreClient#listPendingUploads(PendingUploadsOptions)
   */
  ListenableFuture<List<Void>> abortPendingUploads(PendingUploadsOptions options);

  /**
   * Adds a new encryption key.
   *
   * @param options EncryptionKeyOptions
   */
  ListenableFuture<StoreFile> addEncryptionKey(EncryptionKeyOptions options)
    throws IOException;

  /**
   * Removes an existing encryption key.
   *
   * @param options EncryptionKeyOptions
   */
  ListenableFuture<StoreFile> removeEncryptionKey(EncryptionKeyOptions options)
    throws IOException;

  /**
   * Makes sure all pending tasks have been completed and shuts down all internal machinery
   * properly.
   */
  void shutdown();


  /**
   * Query for the existence of a bucket.
   *
   * @param bucketName The name of the bucket
   */
  boolean hasBucket(String bucketName);


  /**
   * Create a new bucket with default permissions.  An exception will be thrown if a bucket with
   * that name already exists.
   *
   * @param bucketName The name of the bucket
   */
  void createBucket(String bucketName);


  /**
   * Delete a bucket.  An exception will be thrown if the bucket does not exist.
   *
   * @param bucketName The name of the bucket
   */
  void destroyBucket(String bucketName);

}
