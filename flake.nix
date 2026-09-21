{
  description = "Spinta development environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    {
      self,
      nixpkgs,
    }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      # Nixpkgs poetry test suite fails on Python 3.14, so skip it.
      poetry = pkgs.poetry.overridePythonAttrs (old: {
        doCheck = false;
      });
      libraries = with pkgs; [
        stdenv.cc.cc.lib
        zlib
        libxml2
        libxslt
        openssl
        geos
      ];
      markout = pkgs.writeShellScriptBin "markout" ''
        exec python notes/scripts/markout.py "$@"
      '';
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pkgs.python311
          poetry
          pkgs.git
          pkgs.openssl
          pkgs.nushell
          pkgs.nushellPlugins.formats
          pkgs.jq
          pkgs.curl
          pkgs.httpie
          pkgs.sqlite
          markout
        ];

        nativeBuildInputs = [
          pkgs.pkg-config
        ];

        # System libraries required by Python packages installed via poetry.
        shellHook = ''
          export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath libraries}:$LD_LIBRARY_PATH"

          poetry env use ${pkgs.python311}/bin/python
          export PATH="$(poetry env info -p)/bin:$PATH"

          # Register nushell formats plugin (provides `from ini` for credentials.cfg)
          nu -c "plugin add ${pkgs.nushellPlugins.formats}/bin/nu_plugin_formats" || true

          echo "=== Dev environment ready ==="
          echo "Python: $(python3 --version)"
          echo "Poetry: $(poetry --version)"
          echo "Docker: $(docker --version 2>/dev/null || echo 'not running')"
        '';
      };
    };
}
