{
  inputs = {
    nixpkgs = {
      url = "github:nixos/nixpkgs/nixos-unstable";
    };
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
  };
  outputs = { nixpkgs, flake-utils, ... }: flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs {
        inherit system;
      };
    in {
      devShell = pkgs.mkShell {
        buildInputs = with pkgs; [
          valgrind
          zlib
          doxygen
          python39
          clang_8
          clang-tools
          lldb
        ];
        shellHook = ''
          export CC=clang
          export CXX=clang++
        '';
      };
    }
  );
}