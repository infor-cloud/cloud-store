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
   *
   * @param retry Retry flag
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
   *
   * @return Storage scheme
   */
  String getScheme();

  /**
   * Returns the executor responsible for issuing HTTP API calls against the backend storage service
   * asynchronously. It also determines the level of parallelism of an operation, e.g. number of
   * threads used for uploading/downloading a file.
   *
   * @return API executor
   */
  ListeningExecutorService getApiExecutor();

  /**
   * Returns the executor responsible for executing internal cloud-store tasks asynchronously. Such
   * tasks include file I/O, file encryption, file splitting, and error handling.
   *
   * @return Internal task executor
   */
  ListeningScheduledExecutorService getInternalExecutor();

  /**
   * Returns a factory used to create builder objects for all command options.
   *
   * @return Factory to create options builders.
   */
  OptionsBuilderFactory getOptionsBuilderFactory();

  /**
   * Returns the provider of encryption key pairs used to encrypt or decrypt files during
   * upload or download.
   *
   * @return Provider of key pairs
   */
  KeyProvider getKeyProvider();

  /**
   * Returns a service-specific object used to abstract over access control functionality
   * provided by different services.
   *
   * @return Access control list interface for a service
   */
  AclHandler getAclHandler();

  /**
   * Returns a service-specific object used to query a service about storage classes.
   *
   * @return Storage class interface for a service
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
   * @return Future containing StoreFile with uploaded file information
   * @throws IOException -
   */
  ListenableFuture<StoreFile> upload(UploadOptions options)
    throws IOException;

  /**
   * Upload a file or a directory of files in the local file system to a cloud store service
   * recursively.
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
   * @return Future containing list of StoreFiles with uploaded file information
   * @throws IOException -
   * @throws ExecutionException -
   * @throws InterruptedException -
   */
  ListenableFuture<List<StoreFile>> uploadRecursively(UploadOptions options)
    throws IOException, ExecutionException, InterruptedException;

  /**
   * Delete a single file from a cloud store service.
   * <p>
   * This operation returns a future that when completed successfully will contain a
   * {@link StoreFile} with metadata about the deleted file.  If a file matching the
   * options is not found, an exception will be thrown.
   *
   * @param options DeleteOptions that specify what to delete
   * @return Future containing StoreFile with deleted file information
   */
  ListenableFuture<StoreFile> delete(DeleteOptions options);

  /**
   * Delete a set of files from a cloud store service, where all the files match
   * a particular prefix.
   *
   * @param options DeleteOptions that specify what to delete
   * @return Future containing list of StoreFiles with deleted file information
   * @throws ExecutionException -
   * @throws InterruptedException -
   */
  ListenableFuture<List<StoreFile>> deleteDirectory(DeleteOptions options)
    throws InterruptedException, ExecutionException;

  /** 
   * List all buckets visible to the current user, returning a future that when completed
   * will contain a list of {@link Bucket Buckets} with name and owner of each bucket.
   *
   * @return Future containing list of Buckets visible to the user
   */
  ListenableFuture<List<Bucket>> listBuckets();

  ListenableFuture<Bucket> getBucket(String bucketName)
    throws ExecutionException, InterruptedException, IOException;
  /**
   * Check existence of a specific file in a cloud store service.
   * <p>
   * Returns a future that when completed will contain null if the file does not exist or
   * will contain a {@link Metadata} object if it does exist.
   *
   * @param options ExistsOptions specifying what file to check for
   * @return Future containing Metadata for a file
   */
  ListenableFuture<Metadata> exists(ExistsOptions options);

  /**
   * Download a file from a cloud store service to the local file system.
   * <p>
   * If the file was encrypted when uploaded, one of the private keys corresponding with the
   * public keys associated with the file must be found in the local directory containing
   * public/private key pair files.  If no matching key is found, an error will be reported
   * and the download will fail.  Otherwise, the file will be downloaded and decrypted.
   * <p>
   * The current user must have read permission to the bucket containing the file to download.
   * If a local file with the destination name already exists, the file will not be downloaded
   * and an exception thrown unless the {@code overwrite} flag is set to true in the
   * {@code options} parameter.
   * <p>
   * If the file's size is bigger than the chunk size used for its upload, multiple
   * concurrent threads will be used to download parts of the file in parallel.  The level
   * of parallelism is controlled by the executor used to create the CloudStoreClient
   * interface.  See {@link #getApiExecutor()} and {@link Utils#createCloudStoreClient(String)}.
   * <p>
   * Since a file can be uploaded, updated, and/or copied by tools other than cloud-store,
   * there is no easy way to detect another tool's chosen chunk size (which affects the checksum)
   * so is not always safe and efficient to validate its checksum.  Currently, this client tries 
   * to do checksum validation when it knows that the file was uploaded by cloud-store or the 
   * file can been fully downloaded using chunk.
   * <p>
   * This returns a future that when completed will contain a {@link StoreFile} object with
   * information about the downloaded file.
   *
   * @param options Set of options controlling the download operation
   * @return Future containing StoreFile with information about downloaded file
   * @throws IOException -
   */
  ListenableFuture<StoreFile> download(DownloadOptions options)
    throws IOException;

  /**
   * Download a set of files from a cloud store service to the local file system.  The
   * {@code objectKey} in the {@link DownloadOptions} will be used as a prefix to find
   * a set of files in a simulated directory in the cloud store service. All files matching the
   * prefix will be downloaded.
   * <p>
   * If any file to be downloaded was encrypted when uploaded, one of the private keys 
   * corresponding with the public keys associated with the file must be found in the local 
   * directory containing public/private key pair files.  If no matching key is found, an error 
   * will be reported and the download will fail.  Otherwise, the file will be downloaded and 
   * decrypted.
   * <p>
   * The current user must have read permission to the bucket containing the files to download.
   * If any local file with the destination name already exists, the file will not be downloaded
   * and an exception thrown, unless the {@code overwrite} flag is set to true in the
   * {@code options} parameter.
   * <p>
   * If any file's size is bigger than the chunk size used for its upload, multiple
   * concurrent threads will be used to download parts of the file in parallel.  The level
   * of parallelism is controlled by the executor used to create the CloudStoreClient
   * interface.  See {@link #getApiExecutor()} and {@link Utils#createCloudStoreClient(String)}.
   * <p>
   * Since a file can be uploaded, updated, and/or copied by tools other than cloud-store,
   * there is no easy way to detect another tool's chosen chunk size (which affects the checksum)
   * so is not always safe and efficient to validate its checksum.  Currently, this client tries 
   * to do checksum validation when it knows that the file was uploaded by cloud-store or the 
   * file can been fully downloaded using chunk.
   * <p>
   * This returns a future that when completed will contain a list of {@link StoreFile} objects with
   * information about the downloaded files.
   *
   * @param options Set of options controlling the download operation
   * @return Future containing list of StoreFiles with information about downloaded files
   * @throws IOException -
   * @throws ExecutionException -
   * @throws InterruptedException -
   */
  ListenableFuture<List<StoreFile>> downloadRecursively(DownloadOptions options)
    throws IOException, ExecutionException, InterruptedException;

  /**
   * Copy one file in a cloud store service to have another name in the store.
   * <p>
   * The source bucket must already exist and the user must have read permission to it.
   * Likewise, the destination bucket must already exist and the user must have write
   * permission to it.
   * <p>
   * Return a future that when complete will contain a {@link StoreFile} with information
   * about the destination file.
   *
   * @param options Set of options specifying file to copy along with the new name and 
   *                other properties.
   * @return Future containing StoreFile with information about copied file
   */
  ListenableFuture<StoreFile> copy(CopyOptions options);

  /**
   * Copy all files in a cloud store service whose keys would be returned by the list 
   * operation on the source prefix URI to new files with a new prefix.  This behaves like a
   * local file system copy of a set of files into a new directory.  The destination URI in
   * the {@link CopyOptions} must look like a directory (end with a '/').
   * <p>
   * The source bucket must already exist and the user must have read permission to it.
   * Likewise, the destination bucket must already exist and the user must have write
   * permission to it.
   * <p>
   * Return a future that when complete will contain a list {@link StoreFile} objects with 
   * information about the copied files.
   *
   * @param options Set of options specifying files to copy along with their destination.
   * @return Future containing list of StoreFiles with information about copied files
   * @throws IOException -
   * @throws ExecutionException -
   * @throws InterruptedException -
   */
  ListenableFuture<List<StoreFile>> copyRecursively(CopyOptions options)
    throws InterruptedException, ExecutionException, IOException;

  /**
   * Rename a file in a cloud-store service.  That this is equivalent to a copy operation
   * followed by a delete operation on a file.
   * <p>
   * The source bucket must already exist and the user must have read permission to it.
   * Likewise, the destination bucket must already exist and the user must have write
   * permission to it.
   * <p>
   * Return a future that when complete will contain a {@link StoreFile} with information
   * on the destination file.
   *
   * @param options Set of parameters that specify the source file, new name, and other
   *                options for the operation.
   * @return Future containing StoreFile with information about renamed file
   */
  ListenableFuture<StoreFile> rename(RenameOptions options);

  /**
   * Rename all files in a cloud store service whose keys share a prefix to have another prefix,
   * behaving like a local directory rename.
   * <p>
   * The source bucket must already exist and the user must have read permission to it.
   * Likewise, the destination bucket must already exist and the user must have write
   * permission to it.
   * <p>
   * Both the source and destination keys should be folders (ending with '/').  There must be 
   * some keys that match the source folder prefix, but the destination folder does not need to exist.
   * <p>
   * Return a future that when complete will contain a list of @{link StoreFile} objects with
   * information about the new renamed files.
   *
   * @param options Set of parameters specifying what files to copy and their destination prefix.
   * @return Future containing list of StoreFiles with information about renamed files
   * @throws IOException -
   * @throws ExecutionException -
   * @throws InterruptedException -
   */
  ListenableFuture<List<StoreFile>> renameDirectory(RenameOptions options)
    throws InterruptedException, ExecutionException, IOException;

  /**
   * Return a future that when complete will contain a list of {@link StoreFile} objects
   * with summary information about all files whose keys start with a specified prefix
   * in a particular bucket.
   * <p>
   * If the {@code recursive} option in the ${link ListOptions} is enabled, then all files of all 
   * "subdirectories" will be included in the results as will as the "top-level" files that
   * match the prefix.  If {@code excludeDirs} is set in the options, then directories that
   * match the prefix are <i>not</i> included in the results.
   * <p>
   * List results are returned in lexicographic order.
   * <p>
   * If {@code includeVersions} is set in the options, then information about all versions of
   * the matched files will be included in the results.
   *
   * @param lsOptions Set of options controlling the behavior of the list operation
   * @return Future containing list of StoreFiles with information about files in a service
   */
  ListenableFuture<List<StoreFile>> listObjects(ListOptions lsOptions);

  /**
   * Return a list of pending in-progress uploads for files whose keys match a given key.
   * <p>
   * Cloud-store attempts to abort uploads if an error is detected.  But there are some
   * scenarios where AWS may have allocated storage for a pending upload and the upload
   * never completes or is not cleanly aborted.  This function allows users to find and
   * clean up pending uploads to avoid unnecessary service charges.  See 
   * {@link #abortPendingUploads}.
   * <p>
   * This returns a future that when complete will contain a list of {@link Upload} objects
   * for all pending uploads that match the requested key.  See {@link Upload#getId()} and 
   * {@link Upload#getInitiationDate()} for information useful in aborting the uploads.
   *
   * @param options Set of options that control the search for pending uploads
   * @return Future containing list of StoreFiles with information about pending file uploads
   */
  ListenableFuture<List<Upload>> listPendingUploads(PendingUploadsOptions options);

  /**
   * Abort pending uploads identified by object key or date of the upload.
   * <p>
   * Cloud-store attempts to abort uploads if an error is detected.  But there are some
   * scenarios where AWS may have allocated storage for a pending upload and the upload
   * never completes or is not cleanly aborted.  This function allows users to 
   * clean up pending uploads to avoid unnecessary service charges.  See 
   * {@link #listPendingUploads}.
   * <p>
   * If only the upload id is specified in the {@link PendingUploadsOptions}, then only one 
   * specific pending upload will be aborted.  If only the date is specified, then all pending 
   * uploads that were initiated before this date will be aborted.  If both upload id and date
   * are specified, then only one specific pending upload will be aborted, provided it had
   * been initiated before the specified date.
   *
   * @param options Set of options detailing what upload operations to be aborted
   * @return Future containing list of Void objects for each pending upload aborted
   */
  ListenableFuture<List<Void>> abortPendingUploads(PendingUploadsOptions options);

  /**
   * Adds a new encryption key to an existing file in a cloud store service.  The specified
   * public key must be found in a public/private key pair file in a local key directory.
   * <p>
   * Return a future that when complete will contain a {@link StoreFile} with information
   * on the modified file.
   *
   * @param options Set of options specifying the new key and file to apply the key to
   * @return Future containing StoreFile with information on the updated file
   * @throws IOException -
   */
  ListenableFuture<StoreFile> addEncryptionKey(EncryptionKeyOptions options)
    throws IOException;

  /**
   * Removes an existing encryption key from a file in a cloud store service.  The specified
   * private key must be found in a public/private key pair file in a local key directory. 
   *<p>
   * Return a future that when complete will contain a {@link StoreFile} with information
   * on the modified file.
   *
   * @param options Set of options specifying the key to remove and file to remove it from
   * @return Future containing StoreFile with information on the updated file
   * @throws IOException -
   */
  ListenableFuture<StoreFile> removeEncryptionKey(EncryptionKeyOptions options)
    throws IOException;

  /**
   * Ensure all pending tasks have been completed then shut down all internal machinery
   * properly.
   */
  void shutdown();


  /**
   * Query for the existence of a bucket.
   *
   * @param bucketName The name of the bucket
   * @return true if bucket exists, otherwise false
   */
  boolean hasBucket(String bucketName);


  /**
   * Create a new bucket with default permissions.  An exception will be thrown if a bucket with
   * that name already exists.
   *
   * @param bucketName The name of the bucket to be created
   */
  void createBucket(String bucketName);


  /**
   * Delete a bucket.  An exception will be thrown if the bucket does not exist.
   *
   * @param bucketName The name of the bucket to be deleted
   */
  void destroyBucket(String bucketName);

}
