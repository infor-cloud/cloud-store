{ system ? builtins.currentSystem
, nixpkgs ? <nixpkgs>
, config ? <config>
, pkgs ? import nixpkgs { inherit system; }
, builder_config ? import (config + "/lib") { inherit pkgs; }
, stdenv ? pkgs.stdenv
, fetchurl ? pkgs.fetchurl
, python ? pkgs.pythonFull
, jdk ? pkgs.openjdk8 or pkgs.openjdk
, unzip ? pkgs.unzip
, s3lib ? { outPath = ./.; rev = "1234"; }
}:

let

  deps  = import ./deps.nix { inherit pkgs; };

  version = src: stdenv.lib.optionalString (src ? revCount) (toString src.revCount + "_" ) + toString (src.rev or src.tag or "unknown");

  revision = version s3lib;
  name = "s3lib-${revision}";

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
          "--with-guava=${deps.guava}"
          "--with-jcommander=${deps.jcommander}"
          "--with-log4j=${deps.log4j}"
          "--with-commons-io=${deps.commonsio}"
          "--with-commons-codec=${deps.commonscodec}"
          "--with-aws-java-sdk=${deps.aws_java_sdk}"
          "--with-gcs-java-sdk=${deps.gcs_java_sdk}"
        ];
        buildInputs = [ python jdk];
      };

     binary_tarball =
      builder_config.release_helper {
        inherit name build;
        unixify = "bin/cloud-store bin/s3tool";
        };
    };

in jobs
