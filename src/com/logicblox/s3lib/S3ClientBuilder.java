package com.logicblox.s3lib;

import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

public class S3ClientBuilder {
    private AmazonS3Client s3Client;
    private ListeningExecutorService apiExecutor;
    private ListeningScheduledExecutorService internalExecutor;
    private long chunkSize = Utils.getDefaultChunkSize();
    private KeyProvider keyProvider;

    public S3ClientBuilder setInternalS3Client(AmazonS3Client s3Client) {
        this.s3Client = s3Client;
        return this;
    }

    public S3ClientBuilder setApiExecutor(ListeningExecutorService
                                              apiExecutor) {
        this.apiExecutor = apiExecutor;
        return this;
    }

    public S3ClientBuilder setInternalExecutor
        (ListeningScheduledExecutorService internalExecutor) {
        this.internalExecutor = internalExecutor;
        return this;
    }

    public S3ClientBuilder setChunkSize(long chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public S3ClientBuilder setKeyProvider(KeyProvider keyProvider) {
        this.keyProvider = keyProvider;
        return this;
    }

    public S3Client createS3Client() {
        if (s3Client == null) {
            setInternalS3Client(new AmazonS3Client());
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
        return new S3Client(s3Client, apiExecutor, internalExecutor, chunkSize,
            keyProvider);
    }
}