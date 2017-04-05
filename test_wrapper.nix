{ src_builder_config
}:


let
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
