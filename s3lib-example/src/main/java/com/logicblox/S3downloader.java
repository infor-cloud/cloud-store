/*
  Copyright 2017, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ListenableFuture;
import com.logicblox.s3lib.S3Client;
import com.logicblox.s3lib.UsageException;
import com.logicblox.s3lib.Utils;

public class S3downloader {

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

    private static void download(S3Client client, List<String> urls) throws Exception {
        String file = "test.gz";
        File output = new File(file);
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
     * This is a bare bones implementation of downloading a file from S3. No error
     * checking for things like
     *   AWS_ACCESS_KEY_ID defined?
     *   AWS_SECRET_KEY defined?
     *   s3lib-keys directory defined?
     *   file being requested actually exists?
     */

    public static void main(String[] args) throws Exception {
        ClientConfiguration clientCfg = new ClientConfiguration();
        clientCfg.setProtocol(Protocol.HTTPS);

        AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

        S3Client client = new S3Client(s3Client);
        List<String> urls = new ArrayList<String>();
        urls.add("s3://kiabi-fred-dev/fmachine/test.gz");
        System.out.println("Downloading test.gz");
        download(client, urls);
        client.shutdown();
    }
}
