{ system ? builtins.currentSystem
, nixpkgs ? <nixpkgs>
, config ? <config>
, pkgs ? import nixpkgs { inherit system; }
, stdenv ? pkgs.stdenv
, fetchurl ? pkgs.fetchurl
, python ? pkgs.pythonFull
, jdk ? pkgs.jdk
, unzip ? pkgs.unzip
, s3lib ? { outPath = ./.; rev = "1234"; }
}:

let
  buildjar = {name, url, sha256} : 
    stdenv.mkDerivation rec {
      inherit name;
      src = fetchurl { inherit url sha256; };
      buildCommand = ''
        ensureDir $out/lib/java
        cp $src $out/lib/java/$name.jar
      '';
    };

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

  aws-java-sdk =
    pkgs.stdenv.mkDerivation rec {
      name = "aws-java-sdk-1.7.1";
      src = fetchurl {
        url = http://sdk-for-java.amazonwebservices.com/aws-java-sdk-1.7.1.zip;
        sha256 = "afc1a93635b5e77fb2f1fac4025a3941300843dce7fc5af4f2a99ff9bf4af05b";
      };
      buildInputs = [unzip];
      buildCommand = ''
        unzip $src

        ensureDir $out/lib/java

        for f in $(find ${name} -name '*.jar'); do
          cp $f $out/lib/java
        done
      '';
    };

  version = src: stdenv.lib.optionalString (src ? revCount) (toString src.revCount + "_" ) + toString (src.rev or src.tag or "unknown");

  revision = version s3lib;
  name = "s3lib-${revision}";
  builder_config = import (config + "/lib") { inherit nixpkgs; };

  jobs = rec {
    source_tarball = 
      pkgs.releaseTools.sourceTarball { 
        inherit name;
        src = "${s3lib}";
        buildInputs = [python jdk];
        preConfigure = "patchShebangs .";
      };

     build =
      pkgs.releaseTools.nixBuild { 
        inherit name;
        src = source_tarball;

        configureFlags = [
          "--with-guava=${guava}"
          "--with-jcommander=${jcommander}"
          "--with-log4j=${log4j}"
          "--with-aws-java-sdk=${aws-java-sdk}"
        ];
        buildInputs = [ python jdk];
      };

     binary_tarball =
      builder_config.release_helper {
        inherit name build;
        unixify = "bin/s3tool bin/s3lib-keygen";
        };
    };

in jobs
