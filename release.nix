{
  s3multipart ? { outPath = ./.; rev = "1234"; },
  nixpkgs ? <nixpkgs>
}:

with import <nixpkgs> {
  config.packageOverrides = (p: {
    nodejs = p.lib.overrideDerivation p.nodejs (attrs: {
      name = "nodejs-0.9.3pre";
      src = p.fetchurl {
        url = https://github.com/joyent/node/tarball/63ff449d87e23b5e3d475da960135c1a2fd0ed58;
        sha256 = "1faqcl7jqh7g088mjgas34jjrlmw1lmbxm9vhw7g7vhanhh39yy9";
        name = "node.tar.gz";
      };
    prePatch = ''
      sed -e 's|^#!/usr/bin/env python$|#!${p.python}/bin/python|g' -i tools/{*.py,gyp_node} configure
    '';
    });
  });
};

let

  version = src: lib.optionalString (src ? revCount) (toString src.revCount + "_" ) + toString (if src ? rev then src.rev else src.tag);

  name = "s3multipart-0.0.0pre${version s3multipart}";

  src = s3multipart // { name = if s3multipart ? name then s3multipart.name else name; };

  jobs = {
    build = nodePackages.buildNodePackage {
      inherit name src;

      postInstall = ''
        find $out/node_modules/s3multipart/bin/ -type f -print0 |
          xargs -0 sed -i 's|/usr/bin/coffee|${nodePackages."coffee-script"}/bin/coffee|g'
      '';

      deps = [ nodePackages."cipher-block-size" nodePackages.optimist nodePackages.knox nodePackages."node-expat" nodePackages."buffertools-~1" ];
    };
  }; in jobs
