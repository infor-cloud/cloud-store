package com.logicblox.s3lib;

import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.services.storage.Storage;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.Map;
import java.util.concurrent.Callable;

class GCSUploadFactory implements UploadFactory {
    private Storage client;
    private ListeningExecutorService executor;
    private GCSProgressListenerFactory progressListenerFactory;

    public GCSUploadFactory(Storage client, ListeningExecutorService executor,
                            GCSProgressListenerFactory progressListenerFactory) {
        if (client == null)
            throw new IllegalArgumentException("non-null client is required");
        if (executor == null)
            throw new IllegalArgumentException("non-null executor is required");

        this.client = client;
        this.executor = executor;
        this.progressListenerFactory = progressListenerFactory;
    }

    public ListenableFuture<Upload> startUpload(String bucketName, String key, Map<String, String> meta, String cannedAcl) {
        return executor.submit(new StartCallable(bucketName, key, meta, cannedAcl));
    }

    private class StartCallable implements Callable<Upload> {
        private String key;
        private String bucketName;
        private Map<String, String> meta;
        private String cannedAcl;

        public StartCallable(String bucketName, String key, Map<String, String> meta, String cannedAcl) {
            this.bucketName = bucketName;
            this.key = key;
            this.meta = meta;
            this.cannedAcl = cannedAcl;
        }

        public Upload call() throws Exception {
            return new GCSUpload(client, bucketName, key, cannedAcl, this.meta, executor, progressListenerFactory);
        }
    }
}
