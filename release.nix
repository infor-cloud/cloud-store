{ nixpkgs ? <nixpkgs>
, s3lib ? { outPath = ./.; rev = "1234"; }
, config ? <BuilderConfig>
, logicblox ? "3.9.2.64075_4a330b01c9f9"
}:

with import "${config}/lib" { inherit nixpkgs; };

let
  lb = if builtins.isString logicblox
    then previousReleases.logicblox logicblox
    else logicblox;

  guavaJar = "${lb}/lib/java/guava-13.0.1.jar";

  awsJavaSdkJar = "${lb}/lib/java/aws-java-sdk-1.3.18.jar";

  joptSimpleJar = "${lb}/lib/java/jopt-simple-3.3.jar";

  log4jJar = "${lb}/lib/java/log4j-1.2.13.jar";
in

{
  build = pkgs.runCommand "s3lib-${version s3lib}" { ant = "${pkgs.ant}/bin/ant"; } ''
    $ant -Ddist=$out -DguavaJar=${guavaJar} -DawsJavaSdkJar=${awsJavaSdkJar} -DjoptSimpleJar=${joptSimpleJar} -Dlog4jJar=${log4jJar} -Dbuild=$TMPDIR/build -f ${s3lib}/build.xml
  '';
}
