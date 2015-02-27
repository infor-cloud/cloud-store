package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;


public interface CloudStoreClient {
    /**
     * Sets the number of retries after a failure.
     *
     * @param retryCount Number of retries
     */
    void setRetryCount(int retryCount);

    void setRetryClientException(boolean retry);

    /**
     * Sets the API endpoint this client should issue the requests to. If it's
     * not set, then the underlying {@code s3Client} is going to use its default
     * one for accessing Amazon S3.
     * <p/>
     * This method is mostly useful if we would like to use this client with
     * services other than S3 (e.g. Google Cloud Storage) that provide a
     * S3-compatible API or for testing reasons.
     *
     * @param endpoint API endpoint
     */
    void setEndpoint(String endpoint);

    /**
     * Uploads the specified {@code file} to Amazon S3 under the specified
     * {@code bucket} and {@code object} name. The specified {@code acl} is
     * applied to the uploaded file. If the {@code key} is not null, the {@code
     * keyProvider} will be asked to provide a public key with that name. This
     * key will be used to encrypt the {@code file} at the client side. If
     * {@code progress} is enabled, then progress notifications will be
     * printed.
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
     * of by the underlying low-level {@code s3Client}.
     * <p/>
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
    ListenableFuture<S3File> upload(File file, String bucket, String
        object, String key, String acl, boolean progress)
        throws IOException;

    /**
     * Uploads the specified {@code file} to Amazon S3 under the specified
     * {@code bucket} and {@code object} name. If the {@code key} is not null,
     * the {@code keyProvider} will be asked to provide a public key with that
     * name. This key will be used to encrypt the {@code file} at the client
     * side. It applies {@code acl} permissions on the uploaded object.
     *
     * @param file   File to upload
     * @param bucket Bucket to upload to
     * @param object Path in bucket to upload to
     * @param key    Name of encryption key to use
     * @param acl    Access control list to use
     * @see CloudStoreClient#upload(File, String, String, String, String,
     * boolean)
     */
    @Deprecated
    ListenableFuture<S3File> upload(File file, String bucket, String
        object, String key, CannedAccessControlList acl)
        throws IOException;

    /**
     * Uploads the specified {@code file} to Amazon S3 under the specified
     * {@code s3url}.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to the
     * uploaded object.
     *
     * @param file  File to upload
     * @param s3url Object URI (e.g. s3://bucket/key)
     * @see CloudStoreClient#upload(File, String, String, String, String,
     * boolean)
     */
    ListenableFuture<S3File> upload(File file, URI s3url)
        throws IOException;

    /**
     * Uploads the specified {@code file} to Amazon S3 under the specified
     * {@code s3url}. If the {@code key} is not null, the {@code keyProvider}
     * will be asked to provide a public key with that name. This key will be
     * used to encrypt the {@code file} at the client side.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to the
     * uploaded object.
     *
     * @param file  File to upload
     * @param s3url Object URI (e.g. s3://bucket/key)
     * @param key   Name of encryption key to use
     * @see CloudStoreClient#upload(File, String, String, String, String,
     * boolean)
     */
    ListenableFuture<S3File> upload(File file, URI s3url, String key)
        throws IOException;

    /**
     * Uploads the specified {@code file} to Amazon S3 under the specified
     * {@code bucket} and {@code object} name.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to the
     * uploaded object.
     *
     * @param file   File to upload
     * @param bucket Bucket to upload to
     * @param object Path in bucket to upload to
     * @see CloudStoreClient#upload(File, String, String, String, String,
     * boolean)
     */
    ListenableFuture<S3File> upload(File file, String bucket, String
        object)
        throws IOException;

    /**
     * Uploads the specified {@code file} to Amazon S3 under the specified
     * {@code bucket} and {@code object} name. If the {@code key} is not null,
     * the {@code keyProvider} will be asked to provide a public key with that
     * name. This key will be used to encrypt the {@code file} at the client
     * side.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to the
     * uploaded object.
     *
     * @param file   File to upload
     * @param bucket Bucket to upload to
     * @param object Path in bucket to upload to
     * @param key    Name of encryption key to use
     * @see CloudStoreClient#upload(File, String, String, String, String,
     * boolean)
     */
    ListenableFuture<S3File> upload(File file, String bucket, String
        object, String key)
        throws IOException;

    /**
     * Finds every file under {@code directory} recursively and uploads it to
     * S3. The specified {@code acl} is applied to each created object. If
     * {@code progress} is enabled, then progress notifications will be
     * printed.
     * <p/>
     * Each individual file is uploaded with {@link CloudStoreClient#upload
     * (File, String, String, String, CannedAccessControlList)}.
     * <p/>
     * Symbolic links are not supported.
     *
     * @param directory Directory to upload
     * @param s3url     S3 URL to upload to
     * @param encKey    Encryption key to use
     * @param acl       Access control list to use
     * @param progress  Enable progress indicator
     */
    ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        s3url, String encKey, String acl, boolean progress)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * Finds every file under {@code directory} recursively and uploads it to
     * S3.
     * <p/>
     * By default, the canned ACL "bucket-owner-full-control" is applied to each
     * uploaded object.
     *
     * @param directory Directory to upload
     * @param s3url     S3 URL to upload to
     * @param encKey    Encryption key to use
     * @see CloudStoreClient#uploadDirectory(File, java.net.URI, String, String,
     * boolean)
     */
    ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        s3url, String encKey)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * Finds every file under {@code directory} recursively and uploads it to
     * S3. The specified {@code acl} is applied to each uploaded object.
     *
     * @param directory Directory to upload
     * @param s3url     S3 URL to upload to
     * @param encKey    Encryption key to use
     * @param acl       Access control list to use
     * @see CloudStoreClient#uploadDirectory(File, java.net.URI, String, String,
     * boolean)
     */
    @Deprecated
    ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        s3url, String encKey, CannedAccessControlList acl)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * Deletes the specified {@code object} under S3 {@code bucket}.
     * <p/>
     * No error is returned if the file doesn't exist. If you care, use the
     * {@link CloudStoreClient#exists} methods to check.
     *
     * @param bucket Bucket to delete from
     * @param object Object to be deleted from bucket
     */
    ListenableFuture<S3File> delete(String bucket, String object);

    /**
     * Deletes the specified {@code s3url} from S3.
     *
     * @param s3url Object URI (e.g. s3://bucket/key)
     * @see CloudStoreClient#delete(String, String)
     */
    ListenableFuture<S3File> delete(URI s3url);

    /** Lists all S3 buckets visible for this account. */
    ListenableFuture<List<Bucket>> listBuckets();

    /**
     * Checks if the specified {@code object} exists in the S3 {@code bucket}.
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
     * Checks if the specified {@code s3url} exists in the S3.
     *
     * @param s3url Object URI (e.g. s3://bucket/key)
     * @see CloudStoreClient#exists(String, String)
     */
    ListenableFuture<ObjectMetadata> exists(URI s3url);

    /**
     * Downloads the specified {@code object}, under {@code bucket}, from S3 to
     * a local {@code file}. If {@code progress} is enabled, then progress
     * notifications will be printed.
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
     * effectively dowloading different parts of the file concurrently.
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
     * @param file     File to store the object
     * @param bucket   Bucket to download from
     * @param object   Path in bucket to download
     * @param progress Enable progress indicator
     */
    ListenableFuture<S3File> download(File file, String bucket, String
        object, boolean progress)
        throws IOException;

    /**
     * Downloads the specified {@code object}, under {@code bucket}, from S3 to
     * a local {@code file}.
     *
     * @param file   File to download
     * @param bucket Bucket to download from
     * @param object Path in bucket to download
     * @see CloudStoreClient#download(File, String, String, boolean)
     */
    ListenableFuture<S3File> download(File file, String bucket, String
        object)
        throws IOException;

    /**
     * Downloads the specified {@code s3url} from S3 to a local {@code file}.
     *
     * @param file  File to download
     * @param s3url S3 object URL to download from
     * @see CloudStoreClient#download(File, String, String, boolean)
     */
    ListenableFuture<S3File> download(File file, URI s3url)
        throws IOException;

    /**
     * Downloads the conceptual S3 directory, under {@code s3url}, to the local
     * file system, under {@code directory}. If {@code progress} is enabled,
     * then progress notifications will be printed.
     * <p/>
     * If {@code recursive} is true, then all objects under {@code s3url} will
     * be downloaded. Otherwise, only the first-level objects will be
     * downloaded.
     * <p/>
     * If {@code overwrite} is true, then newly downloaded files is possible to
     * overwrite existing local files.
     * <p/>
     * Each individual file is downloaded with {@link
     * CloudStoreClient#download(File, String, String)}.
     *
     * @param directory Directory to download to
     * @param s3url     Object URI that represents a directory (e.g.
     *                  s3://bucket/key/)
     * @param recursive Download all files recursively under s3url
     * @param overwrite Overwrite a file if it already exists
     * @param progress  Enable progress indicator
     */
    ListenableFuture<List<S3File>> downloadDirectory(
        File directory, URI s3url, boolean recursive, boolean overwrite, boolean
        progress)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * Downloads the conceptual S3 directory, under {@code s3url}, to the local
     * file system, under {@code directory}.
     *
     * @param directory Directory to download to
     * @param s3url     Object URI that represents a directory (e.g.
     *                  s3://bucket/key/)
     * @param recursive Download all files recursively under s3url
     * @param overwrite Overwrite a file if it already exists
     * @see CloudStoreClient#downloadDirectory(File, java.net.URI, boolean,
     * boolean, boolean)
     */
    ListenableFuture<List<S3File>> downloadDirectory(
        File directory, URI s3url, boolean recursive, boolean overwrite)
        throws IOException, ExecutionException, InterruptedException;

    /**
     * List object in S3
     * <p/>
     * TODO: LB-1187
     */
    ListenableFuture<List<S3ObjectSummary>> listObjects(
        String bucket, String prefix, boolean recursive);

    /**
     * List objects and (first-level) directories in S3
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
