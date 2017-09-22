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

package com.logicblox.cloudstore;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.S3Signer;


public class AmazonS3ClientForGCS extends AmazonS3Client {
    public AmazonS3ClientForGCS() {
    }

    public AmazonS3ClientForGCS(AWSCredentials awsCredentials) {
        super(awsCredentials);
    }

    public AmazonS3ClientForGCS(AWSCredentials awsCredentials,
                                ClientConfiguration clientConfiguration) {
        super(awsCredentials, clientConfiguration);
    }

    public AmazonS3ClientForGCS(AWSCredentialsProvider credentialsProvider) {
        super(credentialsProvider);
    }

    public AmazonS3ClientForGCS(AWSCredentialsProvider credentialsProvider,
                                ClientConfiguration clientConfiguration) {
        super(credentialsProvider, clientConfiguration);
    }

    public AmazonS3ClientForGCS(AWSCredentialsProvider credentialsProvider,
                                ClientConfiguration clientConfiguration,
                                RequestMetricCollector requestMetricCollector) {
        super(credentialsProvider, clientConfiguration, requestMetricCollector);
    }

    public AmazonS3ClientForGCS(ClientConfiguration clientConfiguration) {
        super(clientConfiguration);
    }

    protected Signer createSigner(
      final Request<?> request,
      final String bucketName,
      final String key,
      final boolean isAdditionalHeadRequestToFindRegion) 
    {
        Signer signer = getSigner();
        if (signer instanceof S3Signer)
        {
            // The old S3Signer needs a method and path passed to its
            // constructor; if that's what we should use, getSigner()
            // will return a dummy instance and we need to create a
            // new one with the appropriate values for this request.

            String resourcePath =
                "/" +
                    ((bucketName != null) ? bucketName + "/" : "") +
                    ((key != null) ? key : "");

            return new S3Signer(request.getHttpMethod().toString(),
               resourcePath);
        }

        return signer;
    }
}
