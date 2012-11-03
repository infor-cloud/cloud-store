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

  deps = {
    guava = "${lb}/lib/java/guava-13.0.1.jar";

    awsJavaSdk = "${lb}/lib/java/aws-java-sdk-1.3.18.jar";

    joptSimple = "${lb}/lib/java/jopt-simple-3.3.jar";

    log4j = "${lb}/lib/java/log4j-1.2.13.jar";

    commonsLogging = "${lb}/lib/java/commons-logging-1.1.1.jar";

    httpcore = "${lb}/lib/java/httpcore-4.1.jar";

    httpclient = "${lb}/lib/java/httpclient-4.1.1.jar";

    commonsCodec = "${lb}/lib/java/commons-codec-1.6.jar";
  };

  depsString = pkgs.lib.concatStringsSep " " 
    (pkgs.lib.mapAttrsToList (name: value: "-D${name}Jar=${value}") deps);
in

{
  build = pkgs.runCommand "s3lib-${version s3lib}" { ant = "${pkgs.ant}/bin/ant"; } ''
    $ant -Ddist=$out ${depsString} -Dbuild=$TMPDIR/build -f ${s3lib}/build.xml
  mkdir -p $out/bin
  cat > $out/bin/s3tool << EOF
#!/bin/sh
exec ${pkgs.jdk}/bin/java -jar $(readlink -f $out/lib/s3lib-*.jar) "\$@"
EOF
  chmod +x $out/bin/s3tool
  '';
}
