package com.logicblox.s3lib;

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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;

public class GCSClientBuilder {
    private Storage gcsClient;
    private AmazonS3ClientForGCS s3Client;
    private ListeningExecutorService apiExecutor;
    private ListeningScheduledExecutorService internalExecutor;
    private long chunkSize = Utils.getDefaultChunkSize();
    private KeyProvider keyProvider;

    private final String APPLICATION_NAME = "LogicBlox-cloud-store/1.0";
    private final JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    private HttpTransport httpTransport;
    private GoogleCredential credential;
    private HttpRequestInitializer requestInitializer;
    private File credentialFile;

    public GCSClientBuilder setInternalGCSClient(Storage gcsClient) {
        this.gcsClient = gcsClient;
        return this;
    }

    public GCSClientBuilder setInternalS3Client(AmazonS3ClientForGCS s3Client) {
        this.s3Client = s3Client;
        return this;
    }

    public GCSClientBuilder setApiExecutor(ListeningExecutorService
                                               apiExecutor) {
        this.apiExecutor = apiExecutor;
        return this;
    }

    public GCSClientBuilder setInternalExecutor
        (ListeningScheduledExecutorService internalExecutor) {
        this.internalExecutor = internalExecutor;
        return this;
    }

    public GCSClientBuilder setChunkSize(long chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public GCSClientBuilder setKeyProvider(KeyProvider keyProvider) {
        this.keyProvider = keyProvider;
        return this;
    }

    public GCSClientBuilder setHttpTransport(HttpTransport httpTransport) {
        this.httpTransport = httpTransport;
        return this;
    }

    public GCSClientBuilder setCredential(GoogleCredential credential) {
        this.credential = credential;
        return this;
    }

    public GCSClientBuilder setHttpRequestInitializer(HttpRequestInitializer
                                                          requestInitializer) {
        this.requestInitializer = requestInitializer;
        return this;
    }

    public GCSClientBuilder setCredentialFile(File credentialFile) {
        this.credentialFile = credentialFile;
        return this;
    }

    private Storage getDefaultInternalGCSClient() {
        Storage gcsClient0 = new Storage.Builder(httpTransport, jsonFactory,
            requestInitializer)
            .setApplicationName(APPLICATION_NAME)
            .build();

        return gcsClient0;
    }

    private static HttpTransport getDefaultHttpTransport() throws
        GeneralSecurityException, IOException {
        HttpTransport httpTransport0 = null;
        try {
            httpTransport0 = GoogleNetHttpTransport.newTrustedTransport();
        } catch (GeneralSecurityException e) {
            System.err.println("Security error during GCS HTTP Transport " +
                "layer initialization: " + e.getMessage());
            throw e;
        } catch (IOException e) {
            System.err.println("I/O error during GCS HTTP Transport layer" +
                " initialization: " + e.getMessage());
            throw e;
        }
        assert httpTransport0 != null;

        return httpTransport0;
    }

    private static GoogleCredential getDefaultCredential() throws IOException {
        GoogleCredential credential0 = null;
        try {
            credential0 = GoogleCredential.getApplicationDefault();
        } catch (IOException e) {
            System.err.println(
                "Error during loading GCS client secrets from JSON file " +
                    System.getenv("GOOGLE_APPLICATION_CREDENTIALS") +
                    " (GOOGLE_APPLICATION_CREDENTIALS). Make sure it exists, " +
                    "you are loading it with the right path, and a client ID " +
                    "and client secret are defined in it: " + e.getMessage());
            throw e;
        }

        assert credential0 != null;
        credential0 = setScopes(credential0);

        return credential0;
    }

    private static GoogleCredential getCredentialFromFile(File credentialFile)
        throws
        IOException {
        GoogleCredential credential0 = null;
        try {
            InputStream credentialStream = new FileInputStream(credentialFile);
            credential0 = GoogleCredential.fromStream(credentialStream);
        } catch (IOException e) {
            System.err.println(
                "Error during loading GCS client secrets from JSON file " +
                    credentialFile + ". Make sure it exists, " +
                    "you are loading it with the right path, and a client ID " +
                    "and client secret are defined in it: " + e.getMessage());
            throw e;
        }

        assert credential0 != null;
        credential0 = setScopes(credential0);

        return credential0;
    }

    private static GoogleCredential setScopes(GoogleCredential credential0) {
        Collection scopes =
            Collections.singletonList(StorageScopes.DEVSTORAGE_FULL_CONTROL);
        if (credential0.createScopedRequired()) {
            credential0 = credential0.createScoped(scopes);
        }

        return credential0;
    }

    private HttpRequestInitializer getDefaultHttpRequestInitializer() {
        // By default, Google HTTP client doesn't resume uploads in case of
        // IOExceptions.
        // To make it cope with IOExceptions, we attach a back-off based
        // IOException handler during each HTTP request's initialization.
        HttpRequestInitializer requestInitializer0 = new
            HttpRequestInitializer() {
                @Override
                public void initialize(HttpRequest request) throws IOException {
                    credential.initialize(request);
                    request.setIOExceptionHandler(new
                        HttpBackOffIOExceptionHandler(new
                        ExponentialBackOff()));
                }
            };

        return requestInitializer0;
    }

    public GCSClient createGCSClient() throws IOException,
        GeneralSecurityException {
        if (httpTransport == null) {
            setHttpTransport(getDefaultHttpTransport());
        }
        if (credential == null) {
            if (credentialFile != null)
                setCredential(getCredentialFromFile(credentialFile));
            else
                setCredential(getDefaultCredential());
        }
        if (requestInitializer == null) {
            setHttpRequestInitializer(getDefaultHttpRequestInitializer());
        }
        if (gcsClient == null) {
            setInternalGCSClient(getDefaultInternalGCSClient());
        }
        if (s3Client == null) {
            setInternalS3Client(new AmazonS3ClientForGCS());
        }
        if (apiExecutor == null) {
            setApiExecutor(Utils.getHttpExecutor(10));
        }
        if (internalExecutor == null) {
            setInternalExecutor(Utils.getInternalExecutor(50));
        }
        if (keyProvider == null) {
            setKeyProvider(Utils.getKeyProvider(Utils.getDefaultKeyDirectory()));
        }
        return new GCSClient(gcsClient, s3Client, apiExecutor,
            internalExecutor, chunkSize, keyProvider);
    }
}