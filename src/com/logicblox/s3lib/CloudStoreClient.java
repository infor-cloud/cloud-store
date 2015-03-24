package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Provides the a general client API for accessing cloud stores like Amazon S3
 * or Google Cloud Storage.
 * <p/>
 * Captures the full configuration independent of concrete operations like
 * uploads or downloads.
 */
public interface CloudStoreClient {
    /**
     * Sets the number of retries after a failure.
     *
     * @param retryCount Number of retries
     */
    void setRetryCount(int retryCount);

    void setRetryClientException(boolean retry);

    /**
     * Sets the API endpoint this client should issue the requests to.
     * <p/>
     * This method is mostly useful if we would like to test different services
     * with compatible APIs or for unit-testing purposes (e.g. mocks).
     *
     * @param endpoint API endpoint
     */
    void setEndpoint(String endpoint);

    /**
     * Creates a full URI out of {@code bucket} and {@code object} key. S3 URI
     * example: s3://bucket/object GCS URI example: gs://bucket/object
     *
     * @param bucket Bucket name
     * @param object Object key
     */
    URI getUri(String bucket, String object) throws URISyntaxException;

    /**
     * Uploads a file according to {@code options}. For more details
     * check {@link com.logicblox.s3lib.UploadOptions}.
     * <p/>
     * The specified bucket must already exist and the caller must have write
     * permission to the bucket to upload an object.
     * <p/>
     * By default, the upload is multi-part with each part being {@code
     * chunkSize} bytes.
     * <p/>
     * The level of parallelism depends on the {@code s3Executor}. Ideally,
     * different parts are uploaded by different threads.
     * <p/>
     * The client automatically does checksum validation both for each part and
     * for the final object, making sure there is no file corruption during the
     * transfer.
     * <p/>
     * Low-level upload operations (e.g. file type recognition) are taken care
     * of by the underlying low-level service-specific clients.
     * <p/>
     * If during this call an exception wasn't thrown, the entire object has
     * supposedly been stored successfully.
     *
     * @param options Upload options
     * @see UploadOptions
     */
    ListenableFuture<S3File> upload(UploadOptions options)
        throws IOException;

    /**
     * Uploads the specified {@code file} under the specified {@code bucket} and
     * {@code object} name. If the {@code key} is not null, the {@code
     * keyProvider} will be asked to provide a public key with that name. This
     * key will be used to encrypt the {@code file} at the client side. It
     * applies {@code acl} permissions on the uploaded object.
     *
     * @param file   File to upload
     * @param bucket Bucket to upload to
     * @param object Path in bucket to upload to
     * @param key    Name of encryption key to use
     * @param acl    Access control list to use
     * @see CloudStoreClient#upload(UploadOptions)
     */
    @Deprecated
    ListenableFuture<S3File> upload(File file, String bucket, String
        object, String key, CannedAccessControlList acl)
        throws IOException;

    /**
     * Uploads the specified {@code file} under the specified {@code uri}.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to the
     * uploaded object.
     *
     * @param file File to upload
     * @param uri  Object URI (e.g. s3://bucket/key)
     * @see CloudStoreClient#upload(UploadOptions)
     */
    @Deprecated
    ListenableFuture<S3File> upload(File file, URI uri)
        throws IOException;

    /**
     * Uploads the specified {@code file} under the specified {@code uri}. If
     * the {@code key} is not null, the {@code keyProvider} will be asked to
     * provide a public key with that name. This key will be used to encrypt the
     * {@code file} at the client side.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to the
     * uploaded object.
     *
     * @param file File to upload
     * @param uri  Object URI (e.g. s3://bucket/key)
     * @param key  Name of encryption key to use
     * @see CloudStoreClient#upload(UploadOptions)
     */
    @Deprecated
    ListenableFuture<S3File> upload(File file, URI uri, String key)
        throws IOException;

    /**
     * Uploads the specified {@code file} under the specified {@code bucket} and
     * {@code object} name.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to the
     * uploaded object.
     *
     * @param file   File to upload
     * @param bucket Bucket to upload to
     * @param object Path in bucket to upload to
     * @see CloudStoreClient#upload(UploadOptions)
     */
    @Deprecated
    ListenableFuture<S3File> upload(File file, String bucket, String object)
        throws IOException;

    /**
     * Uploads the specified {@code file} under the specified {@code bucket} and
     * {@code object} name. If the {@code key} is not null, the {@code
     * keyProvider} will be asked to provide a public key with that name. This
     * key will be used to encrypt the {@code file} at the client side.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to the
     * uploaded object.
     *
     * @param file   File to upload
     * @param bucket Bucket to upload to
     * @param object Path in bucket to upload to
     * @param key    Name of encryption key to use
     * @see CloudStoreClient#upload(UploadOptions)
     */
    @Deprecated
    ListenableFuture<S3File> upload(File file, String bucket, String
        object, String key)
        throws IOException;

    /**
     * Uploads a directory according to {@code options}. For more details
     * check {@link com.logicblox.s3lib.UploadOptions}.
     * <p/>
     * Each individual file is uploaded with {@link CloudStoreClient#upload
     * (UploadOptions)}.
     * <p/>
     * Symbolic links are not supported.
     *
     * @param options Upload options
     * @see UploadOptions
     */
    ListenableFuture<List<S3File>> uploadDirectory(UploadOptions options)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * Uploads every file under {@code directory} recursively.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to each
     * uploaded object.
     *
     * @param directory Directory to upload
     * @param uri       URL to upload to
     * @param encKey    Encryption key to use
     * @see CloudStoreClient#uploadDirectory(UploadOptions options)
     */
    @Deprecated
    ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        uri, String encKey)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * Uploads every file under {@code directory} recursively. The specified
     * {@code acl} is applied to each uploaded object.
     *
     * @param directory Directory to upload
     * @param uri       URL to upload to
     * @param encKey    Encryption key to use
     * @param acl       Access control list to use
     * @see CloudStoreClient#uploadDirectory(UploadOptions options)
     */
    @Deprecated
    ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        uri, String encKey, CannedAccessControlList acl)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * Deletes the specified {@code object} under {@code bucket}.
     * <p/>
     * No error is returned if the file doesn't exist. If you care, use the
     * {@link CloudStoreClient#exists} methods to check.
     *
     * @param bucket Bucket to delete from
     * @param object Object to be deleted from bucket
     */
    ListenableFuture<S3File> delete(String bucket, String object);

    /**
     * Deletes the specified {@code uri}.
     *
     * @param uri Object URI (e.g. s3://bucket/key)
     * @see CloudStoreClient#delete(String, String)
     */
    ListenableFuture<S3File> delete(URI uri);

    /** Lists all buckets visible for this account. */
    ListenableFuture<List<Bucket>> listBuckets();

    /**
     * Checks if the specified {@code object} exists in the {@code bucket}.
     * <p/>
     * Returns a null future if the file does not exist.
     * <p/>
     * Throws an exception if the metadata of the file could not be fetched for
     * different reasons.
     *
     * @param bucket Bucket to check
     * @param object Path in bucket to check
     */
    ListenableFuture<ObjectMetadata> exists(String bucket, String object);

    /**
     * Checks if the specified {@code uri} exists.
     *
     * @param uri Object URI (e.g. s3://bucket/key)
     * @see CloudStoreClient#exists(String, String)
     */
    ListenableFuture<ObjectMetadata> exists(URI uri);

    /**
     * Downloads a file according to {@code options}. For more details
     * check {@link com.logicblox.s3lib.DownloadOptions}.
     * <p/>
     * If the {@code object} was encrypted by s3lib at the client-side, this
     * method will try to decrypt it automatically. It knows which key to use by
     * checking {@code object}'s metadata.
     * <p/>
     * The specified bucket must already exist and the caller must have read
     * permission to the {@code bucket}.
     * <p/>
     * By default, if {@code object}'s size is bigger than the {@code chunkSize}
     * chosen during the upload, then multiple range GETs will be issued,
     * effectively downloading different parts of the file concurrently.
     * <p/>
     * The level of parallelism depends on the {@code s3Executor}. Ideally,
     * different parts are downloaded by different threads.
     * <p/>
     * Since a file can be uploaded, updated and/or copied by any tool is not
     * always safe and efficient to validate its checksum, since there is no
     * easy way to detect each tool's chosen chunk size (which affects object's
     * ETag).
     * <p/>
     * Currently, this client tries to do checksum validation when
     * it has good indication that a file was uploaded by s3lib and/or the file
     * can been fully downloaded using a single range GET.
     * <p/>
     * If during this call an exception wasn't thrown, the entire object has
     * supposedly been stored successfully.
     *
     * @param options Download options
     * @see DownloadOptions
     */
    ListenableFuture<S3File> download(DownloadOptions options)
        throws IOException;

    /**
     * Downloads the specified {@code object}, under {@code bucket}, to a local
     * {@code file}.
     *
     * @param file   File to download
     * @param bucket Bucket to download from
     * @param object Path in bucket to download
     * @see CloudStoreClient#download(DownloadOptions options)
     */
    @Deprecated
    ListenableFuture<S3File> download(File file, String bucket, String
        object)
        throws IOException;

    /**
     * Downloads the specified {@code uri} to a local {@code file}.
     *
     * @param file File to download
     * @param uri  Object URL to download from
     * @see CloudStoreClient#download(DownloadOptions options)
     */
    @Deprecated
    ListenableFuture<S3File> download(File file, URI uri)
        throws IOException;

    /**
     * Downloads a conceptual directory according to {@code options}. For more
     * details check {@link com.logicblox.s3lib.DownloadOptions}.
     * <p/>
     * If {@code recursive} is true, then all objects under {@code uri} will
     * be downloaded. Otherwise, only the first-level objects will be
     * downloaded.
     * <p/>
     * If {@code overwrite} is true, then newly downloaded files is possible to
     * overwrite existing local files.
     * <p/>
     * Each individual file is downloaded with {@link
     * CloudStoreClient#download(File, String, String)}.
     *
     * @param options Download options
     */
    ListenableFuture<List<S3File>> downloadDirectory(DownloadOptions options)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * Downloads the conceptual directory, under {@code uri}, to the local file
     * system, under {@code directory}.
     *
     * @param directory Directory to download to
     * @param uri       Object URI that represents a directory (e.g.
     *                  s3://bucket/key/)
     * @param recursive Download all files recursively under uri
     * @param overwrite Overwrite a file if it already exists
     * @see CloudStoreClient#downloadDirectory(DownloadOptions options)
     */
    @Deprecated
    ListenableFuture<List<S3File>> downloadDirectory(
        File directory, URI uri, boolean recursive, boolean overwrite)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * List object
     * <p/>
     * TODO: LB-1187
     */
    ListenableFuture<List<S3ObjectSummary>> listObjects(
        String bucket, String prefix, boolean recursive);

    /**
     * List objects and (first-level) directories
     * <p/>
     * TODO: LB-1187
     */
    ListenableFuture<List<S3File>> listObjectsAndDirs(
        String bucket, String prefix, boolean recursive);

    /**
     * Makes sure all pending tasks have been completed and shuts down all
     * internal machinery properly.
     */
    void shutdown();
}