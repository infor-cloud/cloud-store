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

  guava =
    buildjar {
      name = "guava-15.0";
      url = http://search.maven.org/remotecontent?filepath=com/google/guava/guava/15.0/guava-15.0.jar;
      sha256 = "7a34575770eebc60a5476616e3676a6cb6f2975c78c415e2a6014ac724ba5783";
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
      url = http://repo1.maven.org/maven2/log4j/log4j/1.2.13/log4j-1.2.13.jar;
      sha256 = "053zkljmfsaj4p1vmnlfr04g7fchsb8v0i7aqibpjbd6i5c63vf8";
    };

  commonsio =
    buildjar {
      name = "commons-io-2.4";
      url = http://repo1.maven.org/maven2/commons-io/commons-io/2.4/commons-io-2.4.jar;
      sha256 = "108mw2v8ncig29kjvzh8wi76plr01f4x5l3b1929xk5a7vf42snc";
    };

  commonscodec =
    buildjar {
      name = "commons-codec-1.11";
      url = https://repo1.maven.org/maven2/commons-codec/commons-codec/1.11/commons-codec-1.11.jar;
      sha256 = "e599d5318e97aa48f42136a2927e6dfa4e8881dff0e6c8e3109ddbbff51d7b7d";
    };

  commonslogging =
    buildjar {
      name = "commons-logging-1.2";
      url = https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar;
      sha256 = "daddea1ea0be0f56978ab3006b8ac92834afeefbd9b7e4e6316fca57df0fa636";
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
          name = "google-oauth-client-1.19.0";
          url = http://search.maven.org/remotecontent?filepath=com/google/oauth-client/google-oauth-client/1.19.0/google-oauth-client-1.19.0.jar;
          sha256 = "763ea99b3cd97c525884ee7c7304836366b696c4d545c38b0f502fc012879886";
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

      jackson_core =
        buildjar {
          name = "jackson-core-2.9.9";
          url = https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.9.9/jackson-core-2.9.9.jar;
          sha256 = "3083079be6088db2ed0a0c6ff92204e0aa48fa1de9db5b59c468f35acf882c2c";
      };

      jsr305 =
        buildjar {
          name = "jsr305-1.3.9";
          url = http://search.maven.org/remotecontent?filepath=com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar;
          sha256 = "905721a0eea90a81534abb7ee6ef4ea2e5e645fa1def0a5cd88402df1b46c9ed";
      };
    in
    pkgs.stdenv.mkDerivation rec {
      name = "gcs-java-sdk-v1-rev20190910-1.30.3";
      buildInputs = [google_api_services_storage google_api_client google_oauth_client google_http_client google_http_client_jackson2 jsr305];
      buildCommand = ''
        mkdir -p $out/lib/java

        cp ${google_api_services_storage}/lib/java/*.jar $out/lib/java
        cp ${google_api_client}/lib/java/*.jar $out/lib/java
        cp ${google_oauth_client}/lib/java/*.jar $out/lib/java
        cp ${google_http_client}/lib/java/*.jar $out/lib/java
        cp ${jsr305}/lib/java/*.jar $out/lib/java
        cp ${google_http_client_jackson2}/lib/java/*.jar $out/lib/java
      '';
    };
}
