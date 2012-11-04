{ nixpkgs ? <nixpkgs>
, logicblox ? "3.9.2.64075_4a330b01c9f9"
, s3lib ? { outPath = ./.; rev = "1234"; }
}:

with import <config/lib> { inherit nixpkgs; };
let

  lb = if builtins.isString logicblox
    then previousReleases.logicblox logicblox
    else logicblox;

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
        configureFlags = [ "--with-logicblox=${lb}"];
        buildInputs = [lb pkgs.python26 jdk_jce];
        propagatedBuildInputs = [pkgs.python26Packages.argparse];
      };

     binary_tarball =
      release_helper {
        inherit name build;
        unixify = "bin/s3tool";
        };
    };

in jobs
