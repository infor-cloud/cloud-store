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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
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
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;

/**
 * {@code GCSClientBuilder} provides methods for building {@code GCSClient} objects. None of these
 * methods is mandatory to be called. In this case, default values will be picked.
 * <p>
 * As is common for builders, this class is not thread-safe.
 */
public class GCSClientBuilder
{
  private Storage _gcsClient;
  private AmazonS3 _s3Client;
  private ClientConfiguration _s3ClientCfg;
  private ListeningExecutorService _apiExecutor;
  private ListeningScheduledExecutorService _internalExecutor;
  private KeyProvider _keyProvider;
  private AWSCredentialsProvider _awsCredentialsProvider;

  private final String _APPLICATION_NAME = "LogicBlox-cloud-store/1.0";
  private final JsonFactory _jsonFactory = JacksonFactory.getDefaultInstance();
  private HttpTransport _httpTransport;
  private HttpRequestInitializer _requestInitializer;
  private GoogleCredential _credential;

  static final String CREDENTIAL_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS";

  public GCSClientBuilder setInternalGCSClient(Storage gcsClient)
  {
    _gcsClient = gcsClient;
    return this;
  }

  /**
   * At least since AWS Java SDK 1.9.8, GET requests to non-standard endpoints (which, of course,
   * include GCS storage.googleapis.com) are forced to use Signature Version 4 signing process which
   * is not supported by the GCS XML API (only Version 2 is supported). This means all operations
   * that use GET requests, e.g. download, are going to fail with the default AmazonS3Client. As a
   * solution, we provide the AmazonS3ClientForGCS subclass which provides the correct signer
   * object.
   * <p>
   * If you don't set any internal S3 client at all, then it will be set to an AmazonS3ClientForGCS
   * object by default.
   *
   * @param s3Client Internal S3 client used for talking to GCS interoperable XML API
   * @return this builder
   */
  public GCSClientBuilder setInternalS3Client(AmazonS3 s3Client)
  {
    _s3Client = s3Client;
    return this;
  }

  public GCSClientBuilder setInternalS3ClientConfiguration(ClientConfiguration clientCfg)
  {
    _s3ClientCfg = clientCfg;
    return this;
  }

  public GCSClientBuilder setApiExecutor(ListeningExecutorService apiExecutor)
  {
    _apiExecutor = apiExecutor;
    return this;
  }

  public GCSClientBuilder setInternalExecutor(ListeningScheduledExecutorService internalExecutor)
  {
    _internalExecutor = internalExecutor;
    return this;
  }

  public GCSClientBuilder setKeyProvider(KeyProvider keyProvider)
  {
    _keyProvider = keyProvider;
    return this;
  }

  public GCSClientBuilder setAWSCredentialsProvider(AWSCredentialsProvider credentialsProvider)
  {
    _awsCredentialsProvider = credentialsProvider;
    return this;
  }

  public GCSClientBuilder setHttpTransport(HttpTransport httpTransport)
  {
    _httpTransport = httpTransport;
    return this;
  }

  public GCSClientBuilder setCredential(GoogleCredential credential)
  {
    _credential = credential;
    return this;
  }

  public GCSClientBuilder setCredentialFromFile(File credentialFile)
    throws IOException
  {
    InputStream credentialStream = null;
    try
    {
      credentialStream = new FileInputStream(credentialFile);
      return setCredentialFromStream(credentialStream);
    }
    catch(IOException e)
    {
      throw e;
    }
    finally
    {
      if(credentialFile != null)
      {
        credentialStream.close();
      }
    }
  }

  public GCSClientBuilder setCredentialFromStream(InputStream credentialStream)
    throws IOException
  {
    return setCredential(getCredentialFromStream(credentialStream));
  }

  public GCSClientBuilder setHttpRequestInitializer(HttpRequestInitializer requestInitializer)
  {
    _requestInitializer = requestInitializer;
    return this;
  }

  private Storage getDefaultInternalGCSClient()
  {
    Storage gcsClient0 = new Storage.Builder(_httpTransport, _jsonFactory, _requestInitializer).setApplicationName(_APPLICATION_NAME).build();

    return gcsClient0;
  }

  private AmazonS3 getDefaultInternalS3Client()
  {
    return new AmazonS3ClientForGCS(_awsCredentialsProvider, _s3ClientCfg);
  }

  private ClientConfiguration getDefaultInternalS3ClientConfiguration()
    throws MalformedURLException
  {
    ClientConfiguration clientCfg = new ClientConfiguration();
    if (Utils.viaProxy())
      S3ClientBuilder.setHttpProxy(clientCfg);
    // use V2 signatures for authentication to GCS's S3-compatible XML API
    clientCfg.setSignerOverride("S3SignerType");

    return clientCfg;
  }

  private static AWSCredentialsProvider getDefaultAWSCredentialsProvider()
  {
    return new XMLEnvCredentialsProvider();
  }

  private static HttpTransport getDefaultHttpTransport()
    throws GeneralSecurityException, IOException
  {
    HttpTransport httpTransport;
    try
    {
      if (Utils.viaProxy())
      {
        String proxy = System.getenv("HTTPS_PROXY");
        if(proxy == null)
        {
          proxy = System.getenv("HTTP_PROXY");
        }

        httpTransport = getHttpProxyTransport(new URL(proxy));
      }
      else
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    }
    catch(GeneralSecurityException e)
    {
      System.err.println(
        "Security error during GCS HTTP Transport layer initialization: " + e.getMessage());
      throw e;
    }
    catch(IOException e)
    {
      System.err.println(
        "I/O error during GCS HTTP Transport layer initialization: " + e.getMessage());
      throw e;
    }
    assert httpTransport != null;

    return httpTransport;
  }

  static HttpTransport getHttpProxyTransport(URL proxyURL)
    throws IOException, GeneralSecurityException
  {
    NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
    builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
    builder.setProxy(new Proxy(Proxy.Type.HTTP,
      new InetSocketAddress(proxyURL.getHost(), proxyURL.getPort())));

    return builder.build();
  }

  private static GoogleCredential getDefaultCredential()
    throws IOException
  {
    GoogleCredential credential0 = null;
    try
    {
      credential0 = GoogleCredential.getApplicationDefault();
    }
    catch(IOException e)
    {
      throw new IOException(String.format(
        "Error reading credential " + "file from environment variable %s, value '%s': %s",
        CREDENTIAL_ENV_VAR, System.getenv(CREDENTIAL_ENV_VAR), e.getMessage()));
    }

    assert credential0 != null;
    credential0 = setScopes(credential0);

    return credential0;
  }

  private static GoogleCredential getCredentialFromStream(InputStream credentialStream)
    throws IOException
  {
    GoogleCredential credential0 = null;
    try
    {
      credential0 = GoogleCredential.fromStream(credentialStream);
    }
    catch(IOException e)
    {
      throw new IOException(
        String.format("Error reading credential " + "stream: %s", e.getMessage()));
    }

    assert credential0 != null;
    credential0 = setScopes(credential0);

    return credential0;
  }

  private static GoogleCredential setScopes(GoogleCredential credential0)
  {
    Collection scopes = Collections.singletonList(StorageScopes.DEVSTORAGE_FULL_CONTROL);
    if(credential0.createScopedRequired())
    {
      credential0 = credential0.createScoped(scopes);
    }

    return credential0;
  }

  private HttpRequestInitializer getDefaultHttpRequestInitializer()
  {
    // By default, Google HTTP client doesn't resume uploads in case of
    // IOExceptions.
    // To make it cope with IOExceptions, we attach a back-off based
    // IOException handler during each HTTP request's initialization.
    HttpRequestInitializer requestInitializer0 = new HttpRequestInitializer()
    {
      @Override
      public void initialize(HttpRequest request)
        throws IOException
      {
        // Lazy credentials initialization to avoid throwing an
        // exception if CREDENTIAL_ENV_VAR doesn't point to a
        // valid credentials file at construction time. It's
        // useful for GCS clients that are constructed but never
        // used (e.g. in lb-web).
        if(_credential == null)
        {
          setCredential(getDefaultCredential());
        }
        _credential.initialize(request);
        request.setIOExceptionHandler(
          new HttpBackOffIOExceptionHandler(new ExponentialBackOff())
          {
            @Override
            public boolean handleIOException(HttpRequest request, boolean supportsRetry) throws IOException
            {
              // The main reason for handling IOExceptions is to let the underlying Google HTTP
              // client resume uploads interrupted by an IOException. In all other cases we still
              // want to be in charge of the underlying exception and decide how we want to
              // handle it (e.g. retry or not).
              if (request.getUrl().toString().contains("googleapis.com/upload/storage/"))
                return super.handleIOException(request, supportsRetry);
              return false;
            }
          }
        );
      }
    };

    return requestInitializer0;
  }

  public static final String GCS_XML_ACCESS_KEY_ENV_VAR = "GCS_XML_ACCESS_KEY";

  public static final String GCS_XML_SECRET_KEY_ENV_VAR = "GCS_XML_SECRET_KEY";

  static class XMLEnvCredentialsProvider implements AWSCredentialsProvider
  {

    public AWSCredentials getCredentials()
    {
      String accessKey = System.getenv(GCS_XML_ACCESS_KEY_ENV_VAR);
      String secretKey = System.getenv(GCS_XML_SECRET_KEY_ENV_VAR);

      if(accessKey == null || secretKey == null)
      {
        throw new UsageException("Unable to load GCS credentials from environment variables " +
          GCS_XML_ACCESS_KEY_ENV_VAR + " and " + GCS_XML_SECRET_KEY_ENV_VAR);
      }

      return new BasicAWSCredentials(accessKey, secretKey);
    }

    public void refresh()
    {
    }

    @Override
    public String toString()
    {
      return getClass().getSimpleName();
    }
  }


  public GCSClient createGCSClient()
    throws IOException, GeneralSecurityException
  {
    if(_httpTransport == null)
    {
      setHttpTransport(getDefaultHttpTransport());
    }
    if(_requestInitializer == null)
    {
      setHttpRequestInitializer(getDefaultHttpRequestInitializer());
    }
    if(_gcsClient == null)
    {
      setInternalGCSClient(getDefaultInternalGCSClient());
    }
    if(_awsCredentialsProvider == null)
    {
      setAWSCredentialsProvider(getDefaultAWSCredentialsProvider());
    }
    if(_s3ClientCfg == null)
    {
      setInternalS3ClientConfiguration(getDefaultInternalS3ClientConfiguration());
    }
    if(_s3Client == null)
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
    return new GCSClient(_gcsClient, _s3Client, _apiExecutor, _internalExecutor, _keyProvider);
  }
}
