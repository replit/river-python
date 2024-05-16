{pkgs}: {
  deps = [
    pkgs.borgbackup
    pkgs.rustc
    pkgs.libiconv
    pkgs.cargo
    pkgs.libxcrypt
    pkgs.zlib
    pkgs.pkg-config
    pkgs.openssl
    pkgs.grpc
    pkgs.c-ares
  ];
}
