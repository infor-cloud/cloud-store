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
	preCheck = ''
	  echo "-------------- MAKEFILE -------------"
          cat Makefile
	  echo "-----"
	  echo "-------------- JAVA -------------"
          ls -lR /opt/logicblox/share/java
	  echo "-----"
	  
#	  echo "LB_HOME=$LOGICBLOX_HOME"
#	  ls -lR $LOGICBLOX_HOME/share/java
#	  echo "-----"
	'';
      };

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

    test_cloud_store =
      pkgs.stdenv.mkDerivation {
        name = "${name}-test";
        src = build.out;
	jre = "${jdk.jre}";
        buildInputs =
          [
	    # pkgs.minio
            # pkgs.minio-client
            pkgs.awscli
            build
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

          #minio_bin="minio"
          #minio_bin="./bin/minio.latest"
	  minio_bin="${build_minio}/bin/minio"
          $minio_bin -h
	  $minio_bin version

          MINIO_ACCESS_KEY=$AWS_ACCESS_KEY_ID MINIO_SECRET_KEY=$AWS_SECRET_ACCESS_KEY \
            $minio_bin server $HOME/cloud-store-ut-buckets --address $s3_addr &

          minio_pid="$!"
          sleep 5

	  $jre/bin/java -cp ./lib/java/s3lib-test.jar com.logicblox.s3lib.TestRunner --keydir $keydir --endpoint $s3_endpoint

        '';

      };

    binary_tarball =
      builder_config.release_helper {
        inherit name build;
        unixify = "bin/cloud-store bin/s3tool";
        };
    };

in jobs
