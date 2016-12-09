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

        buildInputs = [ python jdk pkgs.makeWrapper ];

        postInstall = ''
          wrapProgram $out/bin/cloud-store --prefix PATH : ${jdk.jre}/bin
        '';
      };

    test2 =
      (import <nixpkgs/nixos/lib/testing.nix> { inherit system; }).runInMachine {
        drv =
          pkgs.runCommand "${name}-test"
            {
              buildInputs =
                [ pkgs.minio
                  pkgs.minio-client
                  pkgs.awscli
                  build
                  #(builtins.storePath /nix/store/p9zjmjg8mxnqan8psnamwykj46kd1gd0-s3lib-1234-0pre)
                ];
              src = ./manual-test.sh;
            }
            ''
              export HOME=$(pwd)

              export AWS_ACCESS_KEY_ID=9NLZKB4SPH2OP5L845XE
              export AWS_SECRET_ACCESS_KEY=rvzui7pQS0PI1aAOhtTHWVmJvhMY+b9xSw7arAbC

              MINIO_ACCESS_KEY=$AWS_ACCESS_KEY_ID MINIO_SECRET_KEY=$AWS_SECRET_ACCESS_KEY \
                minio server $HOME/buckets --address 127.0.0.1:9000 &

              dd if=/dev/urandom of=test bs=1M count=1

              cloud-store keygen -n testkeypair

              aws configure set default.region us-east-1
              aws configure set default.s3.signature_version s3v4

              aws --endpoint-url http://localhost:9000/ s3 mb s3://bucket
              aws --endpoint-url http://localhost:9000/ s3 cp $src s3://bucket/
              aws --endpoint-url http://localhost:9000/ s3 ls s3://bucket

              #bash $src ./test s3://bucket/dir testkeypair

              cloud-store ls s3://bucket/ --endpoint http://127.0.0.1:9000/
            '';

        machine =
          { lib, ... }:
          { networking.extraHosts =
              ''
                127.0.0.1 
              '';
          };
      };

    binary_tarball =
      builder_config.release_helper {
        inherit name build;
        unixify = "bin/cloud-store bin/s3tool";
      };

  };

in jobs
