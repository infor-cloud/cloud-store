package com.logicblox;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.logicblox.s3lib.DirectoryKeyProvider;
import com.logicblox.s3lib.KeyProvider;
import com.logicblox.s3lib.S3Client;
import com.logicblox.s3lib.UsageException;
import com.logicblox.s3lib.Utils;

import java.util.Map;

public class S3downloader {

    private static ListeningExecutorService getHttpExecutor() {
        int maxConcurrentConnections = 10;
        return MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(maxConcurrentConnections));
    }

    private static ListeningScheduledExecutorService getInternalExecutor() {
        return MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(50));
    }

    private static KeyProvider getKeyProvider(String encKeyDirectory) {
        File dir = new File(encKeyDirectory);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new UsageException(String.format("specified key directory '%s' does not exist", encKeyDirectory));
        }
        if (!dir.isDirectory()) {
            throw new UsageException(String.format("specified key directory '%s' is not a directory", encKeyDirectory));
        }
        return new DirectoryKeyProvider(dir);
    }

    protected static URI getURI(List<String> urls) throws URISyntaxException {
        if (urls.size() != 1) {
            throw new UsageException("A single S3 object URL is required");
        }
        return Utils.getURI(urls.get(0));
    }

    protected static String getBucket(List<String> urls) throws URISyntaxException {
        return Utils.getBucket(getURI(urls));
    }

    protected static String getObjectKey(List<String> urls) throws URISyntaxException {
        return Utils.getObjectKey(getURI(urls));
    }

    private static void download(S3Client client, List<String> urls, String output_file) throws Exception {

        File output = new File(output_file);
        ListenableFuture<?> result;
        boolean recursive = false;
        boolean overwrite = true;

        if (getObjectKey(urls).endsWith("/")) {
            result = client.downloadDirectory(output, getURI(urls), recursive, overwrite);
        } else {
            // Test if S3 url exists.
            if (client.exists(getBucket(urls), getObjectKey(urls)).get() == null) {
                throw new UsageException("Object not found at " + getURI(urls));
            }
            result = client.download(output, getURI(urls));
        }

        try {
            result.get();
        } catch (ExecutionException exc) {
            rethrow(exc.getCause());
        }

        client.shutdown();
    }

    private static void rethrow(Throwable thrown) throws Exception {
        if (thrown instanceof Exception) {
            throw (Exception) thrown;
        }
        if (thrown instanceof Error) {
            throw (Error) thrown;
        } else {
            throw new RuntimeException(thrown);
        }
    }

    /*
     * This is a bare bones implementation of downloading a file from S3. No
     * error checking for things like
     *   AWS_ACCESS_KEY_ID defined?
     *   AWS_SECRET_KEY defined?
     *   s3lib-keys directory defined?
     *   file being requested actually exists?
     */

    public static void main(String[] args) throws Exception {
        ClientConfiguration clientCfg = new ClientConfiguration();
        clientCfg.setProtocol(Protocol.HTTPS);

        Map<String, String> env = System.getenv();

        // System.out.format("%s=%s%n","AWS_ACCESS_KEY_ID",env.get("AWS_ACCESS_KEY_ID"));
        // System.out.format("%s=%s%n","AWS_SECRET_KEY",env.get("AWS_SECRET_KEY"));

        AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

        long chunkSize = Utils.getDefaultChunkSize();
        String key_dir = String.format("%s/.s3lib-keys", env.get("HOME"));
        System.out.println(String.format("key_dir = '%s", key_dir));
        S3Client client = new S3Client(s3Client, getHttpExecutor(), getInternalExecutor(), chunkSize, getKeyProvider(key_dir));
        List<String> urls = new ArrayList<String>();
        String target = "s3://kiabi-fred-dev/test/test.gz";
        urls.add(target);
        String output_file = "test.gz";
        System.out.println(String.format("Downloading '%s' to '%s'.", target, output_file));
        download(client, urls, output_file);
        client.shutdown();
    }
}
