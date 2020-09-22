{ sources ? import ./nix/sources.nix
, pkgs ? import sources.nixpkgs {}
}:

pkgs.mkShell {
  buildInputs = [
    pkgs.go
    pkgs.which
    pkgs.htop
    pkgs.zlib
  ];
}
