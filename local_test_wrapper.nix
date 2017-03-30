{ bld_cfg # path to source files from the nix builder_config repository
}:


let
  src_builder_config = bld_cfg;

  system = builtins.currentSystem;
  pkgs = import <nixpkgs> { inherit system; };

  builder_config =
    import src_builder_config {
      inherit pkgs;
    };

in

  rec {
    run_test = import ./default.nix {
      inherit builder_config;
    };
  }
