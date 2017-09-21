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

import com.logicblox.s3lib.*;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;





import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;



import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;

public class Proxy {
  
  private static ListeningExecutorService getHttpExecutor() {
    int maxConcurrentConnections = 10;
    return MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(maxConcurrentConnections));
  }

  private static ListeningScheduledExecutorService getInternalExecutor() {
    return MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(50));
  }

  private static KeyProvider getKeyProvider() {
    String encKeyDirectory = Utils.getDefaultKeyDirectory();
    File dir = new File(encKeyDirectory);
    if (!dir.exists() && !dir.mkdirs())
      throw new UsageException("specified key directory '" + encKeyDirectory + "' does not exist");

    if (!dir.isDirectory())
      throw new UsageException("specified key directory '" + encKeyDirectory + "' is not a directory");

    return new DirectoryKeyProvider(dir);
  }
  
  public static void main(String[] args) {
    ClientConfiguration clientCfg = new ClientConfiguration();
    clientCfg.setProtocol(Protocol.HTTPS);

    //setup proxy connection:
    clientCfg.setProxyHost("localhost");
    clientCfg.setProxyPort(8118);

    AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

    long chunkSize = Utils.getDefaultChunkSize();

    S3Client client = new S3Client(
        s3Client,
        getHttpExecutor(),
        getInternalExecutor(),
        chunkSize,
        getKeyProvider());
    
    client.shutdown();
  }
}
