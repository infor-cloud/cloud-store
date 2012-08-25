{
  s3multipart ? { outPath = ./.; rev = "1234"; name = "s3multipart"; },
  nixpkgs ? <nixpkgs>,
}:

with import <nixpkgs> {};

let

   version = src: lib.optionalString (src ? revCount) (toString src.revCount + "_" ) + toString (if src ? rev then src.rev else src.tag);

  name = "s3multipart-0.0.0pre${version s3multipart}";

  jobs = {
    build = nodePackages.buildNodePackage {
      inherit name;

      src = s3multipart;

      postInstall = ''
        find $out/node_modules/s3multipart/bin/ -name \*.coffee -print0 |
          xargs -0 sed -i 's|/usr/bin/coffee|${nodePackages."coffee-script"}/bin/coffee|g'
      '';

      deps = [];
    };
  }; in jobs
