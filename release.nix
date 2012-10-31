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
in

{
  build = pkgs.runCommand "s3lib-${version s3lib}" { ant = "${pkgs.ant}/bin/ant"; } ''
    $ant -Ddist=$out -Dclasspath=$(readlink -f ${lb}/lib/java/guava*.jar):$(readlink -f ${lb}/lib/java/aws-java-sdk-*.jar) -Dbuild=$TMPDIR/build -f ${s3lib}/build.xml
  '';
}
