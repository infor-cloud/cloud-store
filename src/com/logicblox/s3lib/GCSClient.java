package com.logicblox.s3lib;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;


/**
 * Captures the full configuration independent of concrete uploads and
 * downloads.
 */
public class GCSClient implements CloudStoreClient {
    private static Storage gcsClient;
    private final S3ClientDelegatee s3Client;

    /**
     * @param internalS3Client Low-level S3-client
     * @param apiExecutor      Executor for executing GCS API calls
     * @param internalExecutor Executor for internally initiating uploads
     * @param chunkSize        Size of chunks. Currently, it is not used
     * @param keyProvider      Provider of encryption keys
     */
    public GCSClient(
        AmazonS3Client internalS3Client,
        ListeningExecutorService apiExecutor,
        ListeningScheduledExecutorService internalExecutor,
        long chunkSize,
        KeyProvider keyProvider) {
        s3Client = new S3ClientDelegatee(internalS3Client, apiExecutor,
            internalExecutor, chunkSize, keyProvider);
        GCSClient.this.gcsClient = GCStorage.INSTANCE.storage();
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
    public ListenableFuture<S3File> upload(File file, String bucket, String
        object, String key, String acl, boolean progress) throws IOException {
        return s3Client.upload(file, bucket, object, key, acl, progress);
    }

    @Override
    public ListenableFuture<S3File> upload(File file, String bucket, String
        object, String key, CannedAccessControlList acl)
        throws IOException {
        return upload(file, bucket, object, key, acl.toString(), false);
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
        return upload(file, bucket, object, key, Utils.getDefaultACL(true),
            false);
    }

    @Override
    public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        s3url, String encKey, String acl, boolean progress) throws IOException,
        ExecutionException, InterruptedException {
        return s3Client.uploadDirectory(directory, s3url, encKey, acl,
            progress);
    }

    @Override
    public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        s3url, String encKey) throws IOException, ExecutionException,
        InterruptedException {
        return s3Client.uploadDirectory(directory, s3url, encKey);
    }

    @Override
    public ListenableFuture<List<S3File>> uploadDirectory(File directory, URI
        s3url, String encKey, CannedAccessControlList acl) throws IOException,
        ExecutionException, InterruptedException {
        return s3Client.uploadDirectory(directory, s3url, encKey, acl);
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
    public ListenableFuture<S3File> download(File file, String bucket, String
        object, boolean progress) throws IOException {
        return s3Client.download(file, bucket, object, progress);
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
    public ListenableFuture<List<S3File>> downloadDirectory(File directory, URI
        s3url, boolean recursive, boolean overwrite, boolean progress) throws
        IOException, ExecutionException, InterruptedException {
        return s3Client.downloadDirectory(directory, s3url, recursive,
            overwrite,
            progress);
    }

    @Override
    public ListenableFuture<List<S3File>> downloadDirectory(File directory, URI
        s3url, boolean recursive, boolean overwrite) throws IOException,
        ExecutionException, InterruptedException {
        return s3Client.downloadDirectory(directory, s3url, recursive,
            overwrite);
    }

    @Override
    public ListenableFuture<List<S3ObjectSummary>> listObjects(String bucket,
                                                               String prefix,
                                                               boolean
                                                                   recursive) {
        return s3Client.listObjects(bucket, prefix, recursive);
    }

    @Override
    public ListenableFuture<List<S3File>> listObjectsAndDirs(String bucket,
                                                             String prefix,
                                                             boolean
                                                                 recursive) {
        return s3Client.listObjectsAndDirs(bucket, prefix, recursive);
    }

    @Override
    public void shutdown() {
        s3Client.shutdown();
    }

    /**
     * Singleton GCS Storage object for sharing it across all clients.
     */
    public enum GCStorage {
        INSTANCE;

        private final String APPLICATION_NAME = "LogicBlox-cloud-store/1.0";
        private final HttpTransport httpTransport;
        private final JsonFactory jsonFactory = JacksonFactory
            .getDefaultInstance();
        private final GoogleCredential credential;
        private final Storage storage;


        private static HttpTransport initHttpTransport() {
            HttpTransport httpTransport0 = null;
            try {
                httpTransport0 = GoogleNetHttpTransport.newTrustedTransport();
            } catch (GeneralSecurityException e) {
                System.err.println("Security error during GCS HTTP Transport " +
                    "layer initialization: " + e.getMessage());
                System.exit(1);
            } catch (IOException e) {
                System.err.println("I/O error during GCS HTTP Transport layer" +
                    " initialization: " + e.getMessage());
                System.exit(1);
            }
            assert httpTransport0 != null;

            return httpTransport0;
        }

        private static GoogleCredential authorize() {
            GoogleCredential credential = null;
            try {
                credential = GoogleCredential.getApplicationDefault();
            } catch (IOException e) {
                System.err.println(
                    "Error during GCS client secrets JSON file loading. Make " +
                        "sure it exists, you are loading it with the right " +
                        "path, and a client ID and client secret are defined " +
                        "in it: " + e
                        .getMessage());
                System.exit(1);
            }

            assert credential != null;

            Collection scopes =
                Collections.singletonList(StorageScopes
                    .DEVSTORAGE_FULL_CONTROL);
            if (credential.createScopedRequired()) {
                credential = credential.createScoped(scopes);
            }

            return credential;
        }

        GCStorage() {
            httpTransport = initHttpTransport();
            credential = authorize();

            // By default, Google HTTP client doesn't resume uploads in case of
            // IOExceptions.
            // To make it cope with IOExceptions, we attach a back-off based
            // IOException handler during each HTTP request's initialization.
            HttpRequestInitializer requestInitializer = new
                HttpRequestInitializer() {
                @Override
                public void initialize(HttpRequest request) throws IOException {
                    credential.initialize(request);
                    request.setIOExceptionHandler(new
                        HttpBackOffIOExceptionHandler(new
                        ExponentialBackOff()));
                }
            };
            storage = new Storage.Builder(httpTransport, jsonFactory,
                requestInitializer)
                .setApplicationName(APPLICATION_NAME)
                .build();
        }

        public Storage storage() {
            return storage;
        }
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
         * Upload file to S3.
         *
         * @param file   File to upload
         * @param bucket Bucket to upload to
         * @param object Path in bucket to upload to
         * @param key    Name of encryption key to use
         * @param acl    Access control list to use
         */
        public ListenableFuture<S3File> upload(File file, String bucket, String
            object, String key, String acl, boolean progress)
            throws IOException {
            GCSUploadCommand cmd =
                new GCSUploadCommand(_s3Executor, _executor, file, _chunkSize, key,
                    _keyProvider, acl, progress);
            s3Client.configure(cmd);
            return cmd.run(bucket, object);
        }
    }
}
