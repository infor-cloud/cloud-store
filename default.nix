{ nixpkgs ? <nixpkgs>
, config ? <config>
, logicblox ? (import <LogicBlox> { inherit nixpkgs config; }).package
, s3lib ? { outPath = ./.; rev = "1234"; }
}:

with import (config + "/lib") { inherit nixpkgs; };
let

  lb = if builtins.isString logicblox
    then previousReleases.logicblox logicblox
    else logicblox;

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

  revision = version s3lib;
  name = "s3lib-${revision}";

  jobs = rec {
    source_tarball = 
      pkgs.releaseTools.sourceTarball { 
        inherit name;
        src = "${s3lib}";
        buildInputs = [lb pkgs.python26 jdk_jce];
        propagatedBuildInputs = [pkgs.python26Packages.argparse];
        preConfigure = "patchShebangs .";
      };

     build =
      pkgs.releaseTools.nixBuild { 
        inherit name;
        src = source_tarball;
        # TODO refine the dependencies to refer to specifc packages and add configure options
        configureFlags = [ "--with-logicblox=${lb} --with-guava=${guava} --with-httpcore=${httpcore}"];
        buildInputs = [lb pkgs.python26 jdk_jce];
        propagatedBuildInputs = [pkgs.python26Packages.argparse];
      };

     binary_tarball =
      release_helper {
        inherit name build;
        unixify = "bin/s3tool bin/s3lib-keygen";
        };
    };

in jobs
