{ nixpkgs ? <nixpkgs>
, s3lib ? { outPath = ./.; rev = "1234"; }
, config ? <BuilderConfig>
}:

with import "${config}/lib" { inherit nixpkgs; };

{
  build = pkgs.runCommand "s3lib-${version s3lib}" { ant = "${pkgs.ant}/bin/ant"; } ''
    $ant -Ddist=$out -Dbuild=$TMPDIR/build -f ${s3lib}/build.xml
  '';
}
