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

{ pkgs
, stdenv ? pkgs.stdenv
, fetchurl ? pkgs.fetchurl
, unzip ? pkgs.unzip
}:

let
  buildjar = {name, url, sha256} : 
    stdenv.mkDerivation rec {
      inherit name;
      src = fetchurl { inherit url sha256; };
      buildCommand = ''
        mkdir -p $out/lib/java
        cp $src $out/lib/java/$name.jar
      '';
    };

in {

  jackson_annotations =
    buildjar {
      name = "jackson-annotations-2.10.4";
      url = https://search.maven.org/remotecontent?filepath=com/fasterxml/jackson/core/jackson-annotations/2.10.4/jackson-annotations-2.10.4.jar;
      sha256 = "9d07ea7ce579a678e7aea61249fa82c46469af9d02c5b5f13e84beb7b0827dbc";
    };

  jackson_core =
    buildjar {
      name = "jackson-core-2.10.4";
      url = https://search.maven.org/remotecontent?filepath=com/fasterxml/jackson/core/jackson-core/2.10.4/jackson-core-2.10.4.jar;
      sha256 = "564f6e5706096179537114299e6d7492d2c38da182df8d7834a4c9141b078ef3";
    };

  jackson_databind =
    buildjar {
      name = "jackson-databind-2.10.4";
      url = https://search.maven.org/remotecontent?filepath=com/fasterxml/jackson/core/jackson-databind/2.10.4/jackson-databind-2.10.4.jar;
      sha256 = "55312662a420c71508e6159c86aa41c1694c52e89a1b90dc94bcf4358134005e";
    };

  istack_runtime = 
    buildjar {
      name = "istack-commons-runtime-3.0.11";
      url = https://repo1.maven.org/maven2/com/sun/istack/istack-commons-runtime/3.0.11/istack-commons-runtime-3.0.11.jar;
      sha256 = "cc3f3704ca7bf23e97653edcfc6bbcf9bfe3866b8aed7f290ae7809085d3a959";
    };

  guava =
    buildjar {
      name = "guava-15.0";
      url = http://search.maven.org/remotecontent?filepath=com/google/guava/guava/15.0/guava-15.0.jar;
      sha256 = "7a34575770eebc60a5476616e3676a6cb6f2975c78c415e2a6014ac724ba5783";
    };

  jaxb_api = 
    buildjar {
      name = "jaxb-api-2.3.1";
      url = https://repo1.maven.org/maven2/javax/xml/bind/jaxb-api/2.3.1/jaxb-api-2.3.1.jar;
      sha256 = "88b955a0df57880a26a74708bc34f74dcaf8ebf4e78843a28b50eae945732b06";
    };

  jaxb_runtime = 
    buildjar {
    name = "jaxb-runtime-2.3.1";
    url = https://repo1.maven.org/maven2/org/glassfish/jaxb/jaxb-runtime/2.3.1/jaxb-runtime-2.3.1.jar;
      sha256 = "45fecfa5c8217ce1f3652ab95179790ec8cc0dec0384bca51cbeb94a293d9f2f";
    };

  jcommander =
    buildjar {
      name = "jcommander-1.29";
      url = http://search.maven.org/remotecontent?filepath=com/beust/jcommander/1.29/jcommander-1.29.jar;
      sha256 = "0b6gjgh028jb1fk39rb3yxs9f1ywdcdlvvhas81h6527p5sxhx8n";
    };

  junit =
    buildjar {
      name = "junit-4.8.2";
      url = https://maven.atlassian.com/content/groups/public/org/junit/com.springsource.org.junit/4.8.2/com.springsource.org.junit-4.8.2.jar;
      sha256 = "1l8v7bykvaqrswbg4jlwwb96v308020q2wiaq8w7lv1q748k7vdh";
    };

  log4j =
    buildjar {
      name = "log4j-1.2.13";
      url = http://search.maven.org/remotecontent?filepath=log4j/log4j/1.2.13/log4j-1.2.13.jar;
      sha256 = "053zkljmfsaj4p1vmnlfr04g7fchsb8v0i7aqibpjbd6i5c63vf8";
    };

  commonsio =
    buildjar {
      name = "commons-io-2.4";
      url = http://search.maven.org/remotecontent?filepath=commons-io/commons-io/2.4/commons-io-2.4.jar;
      sha256 = "108mw2v8ncig29kjvzh8wi76plr01f4x5l3b1929xk5a7vf42snc";
    };

  commonscodec =
    buildjar {
      name = "commons-codec-1.9";
      url = http://search.maven.org/remotecontent?filepath=commons-codec/commons-codec/1.9/commons-codec-1.9.jar;
      sha256 = "ad19d2601c3abf0b946b5c3a4113e226a8c1e3305e395b90013b78dd94a723ce";
    };

  aws_java_sdk =
    pkgs.stdenv.mkDerivation rec {
      name = "aws-java-sdk-1.11.102";
      src = fetchurl {
        url = http://sdk-for-java.amazonwebservices.com/aws-java-sdk-1.11.102.zip;
        sha256 = "c06a529b86c08d73b840adc6fe103d49d7ff3eea011977267f0f350a333c2fb3";
      };
      buildInputs = [unzip];
      buildCommand = ''
        # o option is necessary because the archive contains two
        # documentation files that have identical case-insensitive
        # names.
        unzip -o $src

        mkdir -p $out/lib/java

        for f in $(find ${name} -name '*.jar'); do
          cp $f $out/lib/java
        done
      '';
    };

  gcs_java_sdk =
    let
      google_api_services_storage =
        buildjar {
          name = "google-api-services-storage-v1-rev20190910-1.30.3";
          url = http://search.maven.org/remotecontent?filepath=com/google/apis/google-api-services-storage/v1-rev20190910-1.30.3/google-api-services-storage-v1-rev20190910-1.30.3.jar;
          sha256 = "8d0e3f337cde15a45c64cc4fdcbacf38c28bd46add19abb45b26f8f8c5c7d78a";
      };

      google_api_client =
        buildjar {
          name = "google-api-client-1.30.3";
          url = http://search.maven.org/remotecontent?filepath=com/google/api-client/google-api-client/1.30.3/google-api-client-1.30.3.jar;
          sha256 = "da89326bd0eb9b8a355e5b87090bf201cb1eed4e734fc60cdb8cbab31904dd8c";
      };

      google_oauth_client =
        buildjar {
          name = "google-oauth-client-1.30.2";
          url = http://search.maven.org/remotecontent?filepath=com/google/oauth-client/google-oauth-client/1.30.2/google-oauth-client-1.30.2.jar;
          sha256 = "f97bd2674949d0ce59e198129edf46dbd7c5509f382a1f41ff25040046ff5178";
      };

      google_http_client =
        buildjar {
          name = "google-http-client-1.32.0";
          url = http://search.maven.org/remotecontent?filepath=com/google/http-client/google-http-client/1.32.0/google-http-client-1.32.0.jar;
          sha256 = "6fd9e819d8d75bcedcb2ba9d8e08496b5160b3f855a50057f5d9f6850bbf0e4c";
      };

      google_http_client_jackson2 =
        buildjar {
          name = "google-http-client-jackson2-1.32.0";
          url = http://search.maven.org/remotecontent?filepath=com/google/http-client/google-http-client-jackson2/1.32.0/google-http-client-jackson2-1.32.0.jar;
          sha256 = "4cc7c7b0cf0cf03cb7264763efbacee8af4621eb09a51a078331f3f717c09694";
      };

      jsr305 =
        buildjar {
          name = "jsr305-3.0.2";
          url = http://search.maven.org/remotecontent?filepath=com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar;
          sha256 = "766ad2a0783f2687962c8ad74ceecc38a28b9f72a2d085ee438b7813e928d0c7";
      };

      j2objc_annotations =
        buildjar {
          name = "j2objc-annotations-1.3";
          url = http://search.maven.org/remotecontent?filepath=com/google/j2objc/j2objc-annotations/1.3/j2objc-annotations-1.3.jar;
          sha256 = "21af30c92267bd6122c0e0b4d20cccb6641a37eaf956c6540ec471d584e64a7b";
      };

      opencensus_api =
        buildjar {
          name = "opencensus-api-0.24.0";
          url = http://search.maven.org/remotecontent?filepath=io/opencensus/opencensus-api/0.24.0/opencensus-api-0.24.0.jar;
          sha256 = "f561b1cc2673844288e596ddf5bb6596868a8472fd2cb8993953fc5c034b2352";
      };

      opencensus_contrib_http_util =
        buildjar {
          name = "opencensus-contrib-http-util-0.24.0";
          url = http://search.maven.org/remotecontent?filepath=io/opencensus/opencensus-contrib-http-util/0.24.0/opencensus-contrib-http-util-0.24.0.jar;
          sha256 = "7155273bbb1ed3d477ea33cf19d7bbc0b285ff395f43b29ae576722cf247000f";
      };

      grpc_context =
        buildjar {
          name = "grpc-context-1.22.1";
          url = http://search.maven.org/remotecontent?filepath=io/grpc/grpc-context/1.22.1/grpc-context-1.22.1.jar;
          sha256 = "780a3937705b3c92e07292c97d065b2676fcbe031eae250f1622b026485f294e";
      };

    in

    pkgs.stdenv.mkDerivation rec {
      name = "gcs-java-sdk-v1-rev20190910-1.30.3";
      buildInputs = [google_api_services_storage google_api_client 
                     google_oauth_client google_http_client
                     google_http_client_jackson2 jsr305
                     j2objc_annotations opencensus_api 
                     opencensus_contrib_http_util grpc_context];
      buildCommand = ''
        mkdir -p $out/lib/java

        cp ${google_api_services_storage}/lib/java/*.jar $out/lib/java
        cp ${google_api_client}/lib/java/*.jar $out/lib/java
        cp ${google_oauth_client}/lib/java/*.jar $out/lib/java
        cp ${google_http_client}/lib/java/*.jar $out/lib/java
        cp ${jsr305}/lib/java/*.jar $out/lib/java
        cp ${google_http_client_jackson2}/lib/java/*.jar $out/lib/java
        cp ${j2objc_annotations}/lib/java/*.jar $out/lib/java
        cp ${opencensus_api}/lib/java/*.jar $out/lib/java
        cp ${opencensus_contrib_http_util}/lib/java/*.jar $out/lib/java
        cp ${grpc_context}/lib/java/*.jar $out/lib/java
      '';
    };
}
