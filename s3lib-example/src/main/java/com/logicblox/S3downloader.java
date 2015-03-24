package com.logicblox;

import java.io.File;
import java.net.URI;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ListenableFuture;
import com.logicblox.s3lib.CloudStoreClient;
import com.logicblox.s3lib.DownloadOptions;
import com.logicblox.s3lib.DownloadOptionsBuilder;
import com.logicblox.s3lib.S3Client;
import com.logicblox.s3lib.UsageException;
import com.logicblox.s3lib.Utils;


public class S3downloader {

    private static void download(CloudStoreClient client,
                                 DownloadOptions options)
        throws Exception {
        URI url = client.getUri(options.getBucket(), options.getObjectKey());

        ListenableFuture<?> result;
        if (options.getObjectKey().endsWith("/")) {
            result = client.downloadDirectory(options);
        } else {
            // Test if url exists.
            if (client.exists(url).get() == null) {
                throw new UsageException("Object not found at " + url);
            }
            result = client.download(options);
        }

        result.get();
        client.shutdown();
    }

    /*
     * This is a bare bones implementation of downloading a file from S3. No error
     * checking for things like
     *   AWS_ACCESS_KEY_ID defined?
     *   AWS_SECRET_KEY defined?
     *   s3lib-keys directory defined?
     */

    public static void main(String[] args) throws Exception {
        ClientConfiguration clientCfg = new ClientConfiguration();
        clientCfg.setProtocol(Protocol.HTTPS);
        AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

        // Encryption key directory.
        // String key_dir = Utils.getDefaultKeyDirectory()
        String key_dir = System.getProperty("user.home") + File.separator +
            ".s3lib-keys";

        // CloudStore client
        CloudStoreClient client = new S3Client(s3Client,
            Utils.getKeyProvider(key_dir));

        // File url to download
        URI url = Utils.getURI("s3://kiabi-s3-poc/test/test.dat.gz");

        // Output file
        File output = new File("test-downloaded.gz");
        System.out.println(String.format("Downloading '%s' to '%s'.",
            url, output));

        // Set download options
        DownloadOptions options = new DownloadOptionsBuilder()
            .setFile(output)
            .setUri(url)
            .setRecursive(false)
            .setOverwrite(true)
            .createDownloadOptions();

        // Download the file
        download(client, options);
        client.shutdown();
    }
}
