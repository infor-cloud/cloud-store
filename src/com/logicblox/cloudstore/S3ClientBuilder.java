/*
  Copyright 2018, Infor Inc.

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
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class S3ClientBuilder
{
  private AmazonS3 _client;
  private ClientConfiguration _clientCfg;
  private ListeningExecutorService _apiExecutor;
  private ListeningScheduledExecutorService _internalExecutor;
  private KeyProvider _keyProvider;
  private AWSCredentialsProvider _credentialsProvider;

  public S3ClientBuilder setInternalS3Client(AmazonS3 s3Client)
  {
    _client = s3Client;
    return this;
  }

  public S3ClientBuilder setInternalS3ClientConfiguration(ClientConfiguration clientCfg)
  {
    _clientCfg = clientCfg;
    return this;
  }

  public S3ClientBuilder setApiExecutor(ListeningExecutorService apiExecutor)
  {
    _apiExecutor = apiExecutor;
    return this;
  }

  public S3ClientBuilder setInternalExecutor(ListeningScheduledExecutorService internalExecutor)
  {
    _internalExecutor = internalExecutor;
    return this;
  }

  public S3ClientBuilder setKeyProvider(KeyProvider keyProvider)
  {
    _keyProvider = keyProvider;
    return this;
  }

  public S3ClientBuilder setAWSCredentialsProvider(AWSCredentialsProvider credentialsProvider)
  {
    _credentialsProvider = credentialsProvider;
    return this;
  }

  public S3ClientBuilder setAWSCredentialsProvider(List<String> credentialProviders)
  {
    setAWSCredentialsProvider(getCredentialsProvider(credentialProviders));
    return this;
  }

  private AmazonS3 getDefaultInternalS3Client()
  {
    return new AmazonS3Client(_credentialsProvider, _clientCfg);
  }

  private ClientConfiguration getDefaultInternalS3ClientConfiguration()
    throws MalformedURLException
  {
    ClientConfiguration clientCfg = new ClientConfiguration();
    if (Utils.viaProxy())
      setHttpProxy(clientCfg);

    return clientCfg;
  }

  private static AWSCredentialsProvider getDefaultAWSCredentialsProvider()
  {
    return getCredentialsProvider(new ArrayList<>());
  }

  private static AWSCredentialsProvider getCredentialsProvider(List<String> credentialsProviders)
  {
    if(credentialsProviders == null || credentialsProviders.size() == 0)
    {
      return new DefaultAWSCredentialsProviderChain();
    }

    List<AWSCredentialsProvider> choices = new ArrayList<>();
    for(String cp : credentialsProviders)
    {
      switch(cp)
      {
        case "env-vars":
          choices.add(new EnvironmentVariableCredentialsProvider());
          break;
        case "system-properties":
          choices.add(new SystemPropertiesCredentialsProvider());
          break;
        case "credentials-profile":
          choices.add(new ProfileCredentialsProvider());
          break;
        case "ec2-metadata-service":
          choices.add(new InstanceProfileCredentialsProvider());
          break;
        default:
          break;
      }
    }

    if(choices.size() == 0)
    {
      return new DefaultAWSCredentialsProviderChain();
    }

    AWSCredentialsProvider[] choicesArr = new AWSCredentialsProvider[choices.size()];
    choicesArr = choices.toArray(choicesArr);

    return new AWSCredentialsProviderChain(choicesArr);
  }

  public static class CredentialProvidersValidator
    implements IValueValidator<List<String>>
  {
    @Override
    public void validate(String name, List<String> credentialsProvidersS3)
      throws ParameterException
    {
      for(String cp : credentialsProvidersS3)
        if(!S3Client.defaultCredentialProviders.contains(cp))
        {
          throw new ParameterException("Credential providers should be a " + "subset of " +
            Arrays.toString(S3Client.defaultCredentialProviders.toArray()));
        }
    }
  }

  public static void setHttpProxy(ClientConfiguration clientCfg)
    throws MalformedURLException
  {
    String proxy = System.getenv("HTTPS_PROXY");
    if(proxy != null)
    {
      clientCfg.setProtocol(Protocol.HTTPS);
    }
    else
    {
      proxy = System.getenv("HTTP_PROXY");
      if (proxy != null)
        clientCfg.setProtocol(Protocol.HTTP);
      else
        return;
    }

    URL proxyURL = new URL(proxy);
    clientCfg.setProxyHost(proxyURL.getHost());
    clientCfg.setProxyPort(proxyURL.getPort());
    if(proxyURL.getUserInfo() != null)
    {
      String[] userInfo = proxyURL.getUserInfo().split(":");
      clientCfg.setProxyUsername(userInfo[0]);
      clientCfg.setProxyPassword(userInfo[1]);
    }
  }

  public S3Client createS3Client()
    throws MalformedURLException
  {
    if(_credentialsProvider == null)
    {
      setAWSCredentialsProvider(getDefaultAWSCredentialsProvider());
    }
    if(_clientCfg == null)
    {
      setInternalS3ClientConfiguration(getDefaultInternalS3ClientConfiguration());
    }
    if(_client == null)
    {
      setInternalS3Client(getDefaultInternalS3Client());
    }
    if(_apiExecutor == null)
    {
      setApiExecutor(Utils.createApiExecutor(10));
    }
    if(_internalExecutor == null)
    {
      setInternalExecutor(Utils.createInternalExecutor(50));
    }
    if(_keyProvider == null)
    {
      setKeyProvider(Utils.createKeyProvider(Utils.getDefaultKeyDirectory()));
    }
    return new S3Client(_client, _apiExecutor, _internalExecutor, _keyProvider);
  }
}
