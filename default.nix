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
, s3lib ?  { outPath = ./.; rev = "1234"; }
}:

let

  deps  = import ./deps.nix { inherit pkgs; };

  version = src: stdenv.lib.optionalString (src ? revCount) (toString src.revCount + "_" ) + toString (src.rev or src.tag or "unknown");

  revision = version s3lib;
  name = "cloudstore-${revision}";

  build_minio = pkgs.buildGoPackage rec {
    name = "minio";
    goPackagePath = "github.com/minio/minio";
    rev = "e2aba9196f849c458303aff42d2d6ea3e3ea8904";

    src = pkgs.fetchgit {
      inherit rev;
      url = "https://github.com/minio/minio.git";
      sha256 = "1iixpxcyhfa1lln3qd4xpnmjpbkf0zicj1irk21wqjqkac3rar0s";
    };
  };

  test_linux =
    pkgs.stdenv.mkDerivation {
      name = "${name}-test";
      src = jobs.build.out;
      jre = "${jdk.jre}";
      buildInputs =
        [
          pkgs.awscli
          jobs.build
          build_minio
        ];
      buildPhase = ''
        set -e
        minio_pid=""

       cleanup_minio()
        {
          if [[ -n "$minio_pid" ]];
          then
            kill $minio_pid
          fi
        }

        trap cleanup_minio EXIT

        export HOME=$(pwd)

        # these are dummy keys created just for the tests
        export AWS_ACCESS_KEY_ID=9NLZKB4SPH2OP5L845XE
        export AWS_SECRET_ACCESS_KEY=rvzui7pQS0PI1aAOhtTHWVmJvhMY+b9xSw7arAbC

        s3_addr="127.0.0.1:9000"
        s3_endpoint="http://$s3_addr/"
        keydir="$(pwd)/cloud-store-ut-keys"
        mkdir -p $keydir

        minio_bin="${build_minio}/bin/minio"
        $minio_bin -h
        $minio_bin version

        MINIO_ACCESS_KEY=$AWS_ACCESS_KEY_ID MINIO_SECRET_KEY=$AWS_SECRET_ACCESS_KEY \
          $minio_bin server $HOME/cloud-store-ut-buckets --address $s3_addr &

        minio_pid="$!"
        sleep 5

        $jre/bin/java -cp ./lib/java/cloudstore-test.jar com.logicblox.cloudstore.TestRunner --keydir $keydir --endpoint $s3_endpoint
      '';

      installPhase = ''
         # nothing to do here, but this job will fail if we don't produce
         # an output directory for some reason....
         mkdir -p $out
      '';

    };


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
          "--with-junit=${deps.junit}"
        ];
        buildInputs = [ python pkgs.makeWrapper ];
        propagatedBuildInputs = [ jdk ];
      };

    binary_tarball =
      builder_config.release_helper {
        inherit name build;
        unixify = "bin/cloud-store";
        };

    # minio fails to build on osx
    test = (if stdenv.system == "x86_64-linux" then test_linux else null);

    };

in jobs
