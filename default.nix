{ nixpkgs ? <nixpkgs>
, config ? <config>
, s3lib ? { outPath = ./.; rev = "1234"; }
}:

with import (config + "/lib") { inherit nixpkgs; };
let

  buildjar = {name, url, sha256} : 
    with pkgs; stdenv.mkDerivation rec {
      inherit name;
      src = fetchurl { inherit url sha256; };
      buildCommand = ''
        ensureDir $out/lib/java
        cp $src $out/lib/java/$name.jar
      '';
    };

  guava =
    buildjar {
      name = "guava";
      url = http://search.maven.org/remotecontent?filepath=com/google/guava/guava/14.0/guava-14.0.jar;
      sha256 = "c0127b076e3056f58294e4ae6c01a96599b8f58200345eb6f859192a2d9b2962";
    };

  httpcore =
    buildjar {
      name = "httpcore-4.1";
      url = http://search.maven.org/remotecontent?filepath=org/apache/httpcomponents/httpcore/4.1/httpcore-4.1.jar;
      sha256 = "1kr95c4q7w32yk81klyj2plgjhc5s2l5fh0qdn66c92f3zjqvqrw";
    };

  jcommander =
    buildjar {
      name = "jcommander-1.29";
      url = http://search.maven.org/remotecontent?filepath=com/beust/jcommander/1.29/jcommander-1.29.jar;
      sha256 = "0b6gjgh028jb1fk39rb3yxs9f1ywdcdlvvhas81h6527p5sxhx8n";
    };

   commons-logging =
    buildjar {
      name = "commons-logging-1.1.1";
      url = http://search.maven.org/remotecontent?filepath=org/ow2/util/bundles/commons-logging-1.1.1/1.0.0/commons-logging-1.1.1-1.0.0.jar;
      sha256 = "18hi4jd2ic4vywi5nj2avq9zhy7c5fjkbay2ijh70dangg1iv8ss";
    };

   httpclient =
    buildjar {
      name = "httpclient-4.1.1";
      url = http://search.maven.org/remotecontent?filepath=org/apache/httpcomponents/httpclient/4.1.1/httpclient-4.1.1.jar;
      sha256 = "0la7iqy72w09h6z8h9s09bnac2xj0l05mm1ql5nbyyb6ib82drga";
    };

   commons-codec =
    buildjar {
      name = "commons-codec-1.6";
      url = http://search.maven.org/remotecontent?filepath=commons-codec/commons-codec/1.6/commons-codec-1.6.jar;
      sha256 = "11ix43vckkj5mbv9ccnv4vf745s8sgpkdms07syi854f3fa4xcsl";
    };

   log4j =
    buildjar {
      name = "log4j-1.2.13";
      url = http://mirrors.ibiblio.org/maven2/log4j/log4j/1.2.13/log4j-1.2.13.jar;
      sha256 = "053zkljmfsaj4p1vmnlfr04g7fchsb8v0i7aqibpjbd6i5c63vf8";
    };

   aws-java-sdk =
    buildjar {
      name = "aws-java-sdk-1.3.18";
      url = http://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk/1.3.18/aws-java-sdk-1.3.18.jar;
      sha256 = "1wj5r7smml0lwyrlqv8gjc7cpnv4f66vak5lqqh9l2a30pvk335d";
    };

  revision = version s3lib;
  name = "s3lib-${revision}";

  jobs = rec {
    source_tarball = 
      pkgs.releaseTools.sourceTarball { 
        inherit name;
        src = "${s3lib}";
        buildInputs = [pkgs.python26 jdk_jce];
        propagatedBuildInputs = [pkgs.python26Packages.argparse];
        preConfigure = "patchShebangs .";
      };

     build =
      pkgs.releaseTools.nixBuild { 
        inherit name;
        src = source_tarball;

        configureFlags = [
          "--with-guava=${guava}"
          "--with-jcommander=${jcommander}"
          "--with-commons-logging=${commons-logging}"
          "--with-httpcore=${httpcore}"
          "--with-httpclient=${httpclient}"
          "--with-commons-codec=${commons-codec}"
          "--with-log4j=${log4j}"
          "--with-aws-java-sdk=${aws-java-sdk}"
        ];
        buildInputs = [ pkgs.python26 jdk_jce];
        propagatedBuildInputs = [pkgs.python26Packages.argparse];
      };

     binary_tarball =
      release_helper {
        inherit name build;
        unixify = "bin/s3tool bin/s3lib-keygen";
        };
    };

in jobs
