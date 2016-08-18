package com.logicblox.s3lib;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.api.services.storage.Storage;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;


/**
 * Provides the client for accessing the Google Cloud Storage web service.
 * <p>
 * Captures the full configuration independent of concrete operations like
 * uploads or downloads.
 * <p>
 * For more information about Google Cloud Storage, please see <a
 * href="https://cloud.google.com/storage/">https://cloud.google
 * .com/storage/</a>
 */
public class GCSClient implements CloudStoreClient {
    private static final String GCS_JSON_API_ENDPOINT = "https://www.googleapis.com";
    private static final String GCS_XML_API_ENDPOINT = "https://storage.googleapis.com";


    private final Storage gcsClient;
    private final S3ClientDelegatee s3Client;

    /**
     * @param internalGCSClient Low-level GCS-client
     * @param internalS3Client  Low-level S3-client
     * @param apiExecutor       Executor for executing GCS API calls
     * @param internalExecutor  Executor for internally initiating uploads
     * @param chunkSize         Size of chunks. Currently, it is not used
     * @param keyProvider       Provider of encryption keys
     */
    GCSClient(
        Storage internalGCSClient,
        AmazonS3Client internalS3Client,
        ListeningExecutorService apiExecutor,
        ListeningScheduledExecutorService internalExecutor,
        long chunkSize,
        KeyProvider keyProvider) {
        s3Client = new S3ClientDelegatee(internalS3Client, apiExecutor,
            internalExecutor, chunkSize, keyProvider);
        gcsClient = internalGCSClient;
        setEndpoint(GCS_XML_API_ENDPOINT);
    }

    /**
     * Canned ACLs handling
     */

    public static final String defaultCannedACL = "projectPrivate";

    public static final List<String> allCannedACLs = Arrays.asList(
        "projectPrivate", "private", "publicRead", "publicReadWrite",
        "authenticatedRead", "bucketOwnerRead", "bucketOwnerFullControl");

    /**
     * {@code cannedACLsDescConst} has to be a compile-time String constant
     * expression. That's why e.g. we cannot re-use {@code allCannedACLs} to
     * construct it.
     */
    static final String cannedACLsDescConst = "For Google Cloud Storage, " +
        "choose one of: projectPrivate, private, publicRead, publicReadWrite," +
        " authenticatedRead,bucketOwnerRead, bucketOwnerFullControl (default:" +
        " projectPrivate).";

    public static boolean isValidCannedACL(String aclStr)
    {
        return allCannedACLs.contains(aclStr);
    }

    @Override
    public void setRetryCount(int retryCount) {
        s3Client.setRetryCount(retryCount);
    }

    @Override
    public void setRetryClientException(boolean retry) {
        s3Client.setRetryClientException(retry);
    }

    @Override
    public void setEndpoint(String endpoint) {
        s3Client.setEndpoint(endpoint);
    }

    @Override
    public URI getUri(String bucket, String object) throws URISyntaxException {
        return new URI("gs://" + bucket + "/" + object);
    }

    @Override
    public ListenableFuture<S3File> upload(UploadOptions options) throws IOException {
        return s3Client.upload(options);
    }

    @Override
    public ListenableFuture<S3File> upload(File file, String bucket, String
        object, String key, CannedAccessControlList acl)
        throws IOException {
        UploadOptions options = new UploadOptionsBuilder()
            .setFile(file)
            .setBucket(bucket)
            .setObjectKey(object)
            .setEncKey(key)
            .setAcl(acl.toString())
            .createUploadOptions();

        return upload(options);
    }

    @Override
    public ListenableFuture<S3File> upload(File file, URI s3url)
        throws IOException {
        return upload(file, s3url, null);
    }

    @Override
    public ListenableFuture<S3File> upload(File file, URI s3url, String key)
        throws IOException {
        String bucket = Utils.getBucket(s3url);
        String object = Utils.getObjectKey(s3url);
        return upload(file, bucket, object, key);
    }

    @Override
    public ListenableFuture<S3File> upload(File file, String bucket, String
        object)
        throws IOException {
        return upload(file, bucket, object, null);
    }

    @Override
    public ListenableFuture<S3File> upload(File file, String bucket, String
        object, String key)
        throws IOException {
        UploadOptions options = new UploadOptionsBuilder()
            .setFile(file)
            .setBucket(bucket)
            .setObjectKey(object)
            .setEncKey(key)
            .createUploadOptions();

        return upload(options);
    }

    @Override
    public ListenableFuture<List<S3File>> uploadDirectory(UploadOptions options)
        throws IOException,
        ExecutionException, InterruptedException {
        return s3Client.uploadDirectory(options);
    }

    @Override
    public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        s3url, String encKey) throws IOException, ExecutionException,
        InterruptedException {
        UploadOptions options = new UploadOptionsBuilder()
            .setFile(directory)
            .setUri(s3url)
            .setEncKey(encKey)
            .setAcl("projectPrivate")
            .createUploadOptions();

        return uploadDirectory(options);
    }

    @Override
    public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        s3url, String encKey, CannedAccessControlList acl) throws IOException,
        ExecutionException, InterruptedException {
        UploadOptions options = new UploadOptionsBuilder()
            .setFile(directory)
            .setUri(s3url)
            .setEncKey(encKey)
            .setAcl(acl.toString())
            .createUploadOptions();

        return uploadDirectory(options);
    }

    @Override
    public ListenableFuture<S3File> delete(String bucket, String object) {
        return s3Client.delete(bucket, object);
    }

    @Override
    public ListenableFuture<S3File> delete(URI s3url) {
        return s3Client.delete(s3url);
    }

    @Override
    public ListenableFuture<List<Bucket>> listBuckets() {
        return s3Client.listBuckets();
    }

    @Override
    public ListenableFuture<ObjectMetadata> exists(String bucket, String
        object) {
        return s3Client.exists(bucket, object);
    }

    @Override
    public ListenableFuture<ObjectMetadata> exists(URI s3url) {
        return s3Client.exists(s3url);
    }

    @Override
    public ListenableFuture<S3File> download(DownloadOptions options) throws
        IOException {
        return s3Client.download(options);
    }

    @Override
    public ListenableFuture<S3File> download(File file, String bucket, String
        object) throws IOException {
        return s3Client.download(file, bucket, object);
    }

    @Override
    public ListenableFuture<S3File> download(File file, URI s3url) throws
        IOException {
        return s3Client.download(file, s3url);
    }

    @Override
    public ListenableFuture<List<S3File>> downloadDirectory(DownloadOptions
                                                                    options)
        throws
        IOException, ExecutionException, InterruptedException {
        return s3Client.downloadDirectory(options);
    }

    @Override
    public ListenableFuture<List<S3File>> downloadDirectory(File directory, URI
        s3url, boolean recursive, boolean overwrite) throws IOException,
        ExecutionException, InterruptedException {
        return s3Client.downloadDirectory(directory, s3url, recursive,
            overwrite);
    }

    @Override
    public ListenableFuture<S3File> copy(CopyOptions options) throws IOException {
        throw new UnsupportedOperationException("Copy operation is not " +
            "supported for Google Cloud Storage yet.");
    }

    @Override
    public ListenableFuture<List<S3File>> copyToDir(CopyOptions options)
        throws InterruptedException, ExecutionException, IOException {
        throw new UnsupportedOperationException("Copy operation is not " +
            "supported for Google Cloud Storage yet.");
    }

    @Override
    public ListenableFuture<List<S3File>> listObjects(ListOptions lsOptions) {
        return s3Client.listObjects(lsOptions);
    }

    @Override
    public ListenableFuture<List<Upload>> listPendingUploads(String bucket,
                                                             String prefix) {
        throw new UnsupportedOperationException("listPendingUploads is not " +
            "supported.");
    }

    @Override
    public ListenableFuture<Void> abortPendingUpload(String bucket,
                                                     String key,
                                                     String uploadId) {
        throw new UnsupportedOperationException("abortPendingUpload is not " +
            "supported.");
    }

    @Override
    public ListenableFuture<List<Void>> abortOldPendingUploads(String bucket,
                                                               String prefix,
                                                               Date date) {
        throw new UnsupportedOperationException("abortOldPendingUploads is " +
            "not supported.");
    }

    @Override
    public ListenableFuture<S3File> addEncryptedKey(String bucket,
                                                    String object,
                                                    String key)
        throws IOException {
        throw new UnsupportedOperationException("addEncryptedKey is " +
                                                "not supported.");
    }

    @Override
    public ListenableFuture<S3File> removeEncryptedKey(String bucket,
                                                       String object,
                                                       String key)
        throws IOException {
        throw new UnsupportedOperationException("removeEncryptedKey is " +
                                                "not supported.");
    }

  @Override
    public void shutdown() {
        s3Client.shutdown();
    }

    private class S3ClientDelegatee extends S3Client {
        public S3ClientDelegatee(
            AmazonS3Client internalS3Client,
            ListeningExecutorService apiExecutor,
            ListeningScheduledExecutorService internalExecutor,
            long chunkSize,
            KeyProvider keyProvider) {
            super(internalS3Client, apiExecutor, internalExecutor, chunkSize,
                keyProvider);
        }

        void configure(Command cmd) {
            cmd.setRetryClientException(_retryClientException);
            cmd.setRetryCount(_retryCount);
            cmd.setAmazonS3Client(_client);
            cmd.setGCSClient(gcsClient);
            cmd.setScheme("gs://");
        }

        /**
         * Upload file to GCS.
         *
         * @param options Upload options
         */
        public ListenableFuture<S3File> upload(UploadOptions options)
            throws IOException {
            File file = options.getFile();
            String acl = options.getAcl().or("projectPrivate");
            String encKey = options.getEncKey().orNull();
            Optional<OverallProgressListenerFactory> progressListenerFactory =
                options.getOverallProgressListenerFactory();

            GCSUploadCommand cmd =
                new GCSUploadCommand(_s3Executor, _executor, file,
                    _chunkSize, encKey, _keyProvider, acl, progressListenerFactory);
            s3Client.configure(cmd);
            return cmd.run(options.getBucket(), options.getObjectKey());
        }

        /**
         * Upload directory to GCS.
         *
         * @param options Upload options
         */
        public ListenableFuture<List<S3File>> uploadDirectory(UploadOptions options)
            throws IOException, ExecutionException, InterruptedException {
            File directory = options.getFile();
            String bucket = options.getBucket();
            String object = options.getObjectKey();
            String encKey = options.getEncKey().orNull();
            String acl = options.getAcl().or("projectPrivate");
            OverallProgressListenerFactory progressListenerFactory = options
                .getOverallProgressListenerFactory().orNull();

            UploadDirectoryCommand cmd = new UploadDirectoryCommand(_s3Executor,
                _executor, this);
            s3Client.configure(cmd);
            return cmd.run(directory, bucket, object, encKey, acl,
                progressListenerFactory);
        }
    }
}
