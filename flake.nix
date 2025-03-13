{
  description = "A flake that provides tools needed to hack on river-python";

  inputs.nixpkgs.url = "github:nixos/nixpkgs";

  outputs = { self, nixpkgs }: let
    mkPkgs = system: import nixpkgs {
      inherit system;
    };
    mkDevShell = system:
    let
      pkgs = mkPkgs system;
      replitNixDeps = (import ./replit.nix { pkgs = pkgs; }).deps;
    in
    pkgs.mkShell {
      env = {
        # Needed for Python/gRPC to be able to interact with libstdc++.
        LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";
      };
      packages = replitNixDeps ++ [
        pkgs.python312
        pkgs.uv
      ];
    };
  in
  {
    devShells.aarch64-darwin.default = mkDevShell "aarch64-darwin";
    devShells.x86_64-linux.default = mkDevShell "x86_64-linux";
  };
}
