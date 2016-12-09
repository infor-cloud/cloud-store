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

  log4j =
    buildjar {
      name = "log4j-1.2.13";
      url = http://mirrors.ibiblio.org/maven2/log4j/log4j/1.2.13/log4j-1.2.13.jar;
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
      name = "commons-codec-1.9";
      url = http://repo1.maven.org/maven2/commons-codec/commons-codec/1.9/commons-codec-1.9.jar;
      sha256 = "ad19d2601c3abf0b946b5c3a4113e226a8c1e3305e395b90013b78dd94a723ce";
    };

  aws_java_sdk =
    pkgs.stdenv.mkDerivation rec {
      name = "aws-java-sdk-1.10.32";
      src = fetchurl {
        url = http://sdk-for-java.amazonwebservices.com/aws-java-sdk-1.10.32.zip;
        sha256 = "076bxsk6sf99g30a305dic69nna42sn2ijkkjpfq78yq26wlpxch";
      };
      buildInputs = [unzip];
      buildCommand = ''
        unzip $src

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
          name = "google-api-services-storage-v1-rev26-1.19.1";
          url = http://search.maven.org/remotecontent?filepath=com/google/apis/google-api-services-storage/v1-rev26-1.19.1/google-api-services-storage-v1-rev26-1.19.1.jar;
          sha256 = "15abdb8fbf5d26b23337822ec957320fb595524558c143a3cd7e12223d747231";
      };

      google_api_client =
        buildjar {
          name = "google-api-client-1.19.1";
          url = http://search.maven.org/remotecontent?filepath=com/google/api-client/google-api-client/1.19.1/google-api-client-1.19.1.jar;
          sha256 = "e3b1fda37e5e485e2df77e604c2d0c4966067464724f8d098a751408c3f7ee60";
      };

      google_oauth_client =
        buildjar {
          name = "google-oauth-client-1.19.0";
          url = http://search.maven.org/remotecontent?filepath=com/google/oauth-client/google-oauth-client/1.19.0/google-oauth-client-1.19.0.jar;
          sha256 = "763ea99b3cd97c525884ee7c7304836366b696c4d545c38b0f502fc012879886";
      };

      google_http_client =
        buildjar {
          name = "google-http-client-1.19.0";
          url = http://search.maven.org/remotecontent?filepath=com/google/http-client/google-http-client/1.19.0/google-http-client-1.19.0.jar;
          sha256 = "aa70d0384697f8d674ddc483b07ea3ae5821373e6393a861a3a3db786b9718b9";
      };

      google_http_client_jackson2 =
        buildjar {
          name = "google-http-client-jackson2-1.19.0";
          url = http://search.maven.org/remotecontent?filepath=com/google/http-client/google-http-client-jackson2/1.19.0/google-http-client-jackson2-1.19.0.jar;
          sha256 = "f416949077a926f14b3804c2c14cc3b3e691840cb2c2c2d2d31222d6ee05e4b5";
      };

      jsr305 =
        buildjar {
          name = "jsr305-1.3.9";
          url = http://search.maven.org/remotecontent?filepath=com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar;
          sha256 = "905721a0eea90a81534abb7ee6ef4ea2e5e645fa1def0a5cd88402df1b46c9ed";
      };
    in
    pkgs.stdenv.mkDerivation rec {
      name = "gcs-java-sdk-v1-rev26-1.19.1";
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
