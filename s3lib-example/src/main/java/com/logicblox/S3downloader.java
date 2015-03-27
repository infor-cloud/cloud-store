package com.logicblox;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ListenableFuture;
import com.logicblox.s3lib.CloudStoreClient;
import com.logicblox.s3lib.DownloadOptions;
import com.logicblox.s3lib.DownloadOptionsBuilder;
import com.logicblox.s3lib.S3Client;
import com.logicblox.s3lib.S3File;
import com.logicblox.s3lib.UsageException;
import com.logicblox.s3lib.Utils;


public class S3downloader {

    /*
     * This is a bare bones implementation of downloading a file from S3. No
     * error checking for things like
     *   AWS_ACCESS_KEY_ID defined?
     *   AWS_SECRET_KEY defined?
     *   s3lib-keys directory defined?
     */

    public static void main(String[] args) throws Exception {
        CloudStoreClient client = getCloudStoreClient();
        DownloadOptions options = getDownloadOptions();

        // Download the file/directory
        System.out.println(String.format("Downloading '%s' to '%s'.",
            options.getObjectKey(), options.getFile()));
        if (options.getObjectKey().endsWith("/")) {
            ListenableFuture<List<S3File>> result =
                client.downloadDirectory(options);
            List<S3File> fs = result.get();
        } else {
            URI uri = client.getUri(options.getBucket(), options.getObjectKey());
            if(client.exists(uri).get() == null)
                throw new UsageException("Object not found at " + uri);
            ListenableFuture<S3File> result =
                client.download(options);
            S3File f = result.get();
        }

        client.shutdown();
    }

    private static CloudStoreClient getCloudStoreClient() {
        // Client configuration
        ClientConfiguration clientCfg = new ClientConfiguration();
        // Examples of configuration options:
        //   clientCfg.setProtocol(Protocol.HTTPS);
        //   clientCfg.setProxyHost("localhost");
        //   clientCfg.setProxyPort(8118);
        AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

        // Encryption key directory. Examples:
        //   String key_dir = Utils.getDefaultKeyDirectory();
        //   String key_dir = System.getProperty("user.home") + File
        //     .separator + ".s3lib-keys";
        String key_dir = null;
        assert key_dir != null;

        // CloudStore client
        return new S3Client(s3Client, Utils.getKeyProvider(key_dir));
    }

    private static DownloadOptions getDownloadOptions() throws URISyntaxException {
        // Set download options

        // S3 object URI. Example:
        //   URI url = Utils.getURI("s3://bucket/dir/test.txt");
        URI url = null;
        assert url != null;

        // File to upload. Example:
        //   File file = new File("/path/to/test.txt");
        File file = null;
        assert file != null;

        return new DownloadOptionsBuilder()
            .setFile(file)
            .setUri(url)
            .setRecursive(false)
            .setOverwrite(true)
            .createDownloadOptions();
    }
}
