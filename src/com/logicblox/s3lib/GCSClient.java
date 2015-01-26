package com.logicblox.s3lib;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
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
public class GCSClient extends S3Client {
    private static Storage _gcs_client;

    /**
     * @param credentials AWS Credentials
     * @param s3Executor  Executor for executing GCS API calls
     * @param executor    Executor for internally initiating uploads
     * @param chunkSize   Size of chunks. Currently, it is not used
     * @param keyProvider Provider of encryption keys
     */
    public GCSClient(
            AWSCredentialsProvider credentials,
            ListeningExecutorService s3Executor,
            ListeningScheduledExecutorService executor,
            long chunkSize,
            KeyProvider keyProvider) {
        super(credentials, s3Executor, executor, chunkSize, keyProvider);
        this._gcs_client = GCStorage.INSTANCE.storage();
    }

    void configure(Command cmd) {
        cmd.setRetryClientException(_retryClientException);
        cmd.setRetryCount(_retryCount);
        cmd.setAmazonS3Client(_client);
        cmd.setGCSClient(_gcs_client);
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
    public ListenableFuture<S3File> upload(File file, String bucket, String object, String key, CannedAccessControlList acl)
            throws IOException {
        GCSUploadCommand cmd =
                new GCSUploadCommand(_s3Executor, _executor, file, _chunkSize, key, _keyProvider, acl);
        configure(cmd);
        return cmd.run(bucket, object);
    }

    /**
     * Upload directory from S3
     *
     * @param file   Directory to upload
     * @param s3url  S3 URL to upload to
     * @param encKey Encryption key to use
     * @param acl    Access control list to use
     * @throws IllegalArgumentException If the s3url is not a valid S3 URL.
     */
    public ListenableFuture<List<S3File>> uploadDirectory(File file, URI s3url, String encKey, CannedAccessControlList acl)
            throws IOException, ExecutionException, InterruptedException {
        throw new UnsupportedOperationException("Directory uploading operation is not implemented yet.");
    }

    /**
     * Check if a file exists in the bucket
     *
     * @param bucket Bucket to check
     * @param object Path in bucket to check
     */
    public ListenableFuture<List<Bucket>> listBuckets() {
        throw new UnsupportedOperationException("Bucket listing operation is not implemented yet.");
    }

    /**
     * Download directory from S3
     *
     * @param file  Directory to download
     * @param s3url S3 object URL to download from
     * @throws IllegalArgumentException If the s3url is not a valid S3 URL.
     */
    public ListenableFuture<List<S3File>> downloadDirectory(
            File file, URI s3url, boolean recursive, boolean overwrite)
            throws IOException, ExecutionException, InterruptedException {
        throw new UnsupportedOperationException("Directory downloading operation is not implemented yet.");
    }

    /**
     * List object in S3
     *
     * @param bucket Bucket to check
     * @param object Path in bucket to check
     */
    public ListenableFuture<List<S3ObjectSummary>> listObjects(
            String bucket, String prefix, boolean recursive) {
        throw new UnsupportedOperationException("Object listing operation is not implemented yet.");
    }

    public void shutdown() {
        try {
            _client.shutdown();
        } catch (Exception exc) {
            exc.printStackTrace();
        }

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

    /**
     * Singleton GCS Storage object for sharing it across all clients.
     */
    public enum GCStorage {
        INSTANCE;

        private final String APPLICATION_NAME = "LogicBlox-s3tool/1.0";
        private final HttpTransport httpTransport;
        private final JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
        private final GoogleCredential credential;
        private final Storage storage;


        private static HttpTransport initHttpTransport() {
            HttpTransport httpTransport0 = null;
            try {
                httpTransport0 = GoogleNetHttpTransport.newTrustedTransport();
            } catch (GeneralSecurityException e) {
                System.out.println("Security error during GCS HTTP Transport layer initialization: " + e.getMessage());
                System.exit(1);
            } catch (IOException e) {
                System.out.println("I/O error during GCS HTTP Transport layer initialization: " + e.getMessage());
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
                System.out.println(
                        "Error during GCS client secrets JSON file loading. Make sure it exists, you are " +
                                "loading it with the right path, and a client ID and client secret are " +
                                "defined in it: " + e.getMessage());
                System.exit(1);
            }

            assert credential != null;

            Collection scopes =
                    Collections.singletonList(StorageScopes.DEVSTORAGE_READ_WRITE);
            if (credential.createScopedRequired()) {
                credential = credential.createScoped(scopes);
            }

            return credential;
        }

        GCStorage() {
            httpTransport = initHttpTransport();
            credential = authorize();

            // By default, Google HTTP client doesn't resume uploads in case of IOExceptions.
            // To make it cope with IOExceptions, we attach a back-off based IOException
            // handler during each HTTP request's initialization.
            HttpRequestInitializer requestInitializer = new HttpRequestInitializer() {
                @Override
                public void initialize(HttpRequest request) throws IOException {
                    credential.initialize(request);
                    request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(new ExponentialBackOff()));
                }
            };
            storage = new Storage.Builder(httpTransport, jsonFactory, requestInitializer)
                    .setApplicationName(APPLICATION_NAME)
                    .build();
        }

        public Storage storage() {
            return storage;
        }
    }

}
