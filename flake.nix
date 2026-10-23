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
      # Ruff version pinned in poetry.lock. Packaged from the same PyPI wheel
      # (patched for NixOS) so the dev shell lints exactly like CI.
      ruff = pkgs.stdenv.mkDerivation {
        pname = "ruff";
        version = "0.12.12";
        src = pkgs.python3Packages.fetchPypi {
          pname = "ruff";
          version = "0.12.12";
          format = "wheel";
          dist = "py3";
          python = "py3";
          abi = "none";
          platform = "manylinux_2_17_x86_64.manylinux2014_x86_64";
          hash = "sha256:2afc2fa864197634e549d87fb1e7b6feb01df0a80fd510d6489e1ce8c0b1cc45";
        };
        nativeBuildInputs = [ pkgs.autoPatchelfHook ];
        buildInputs = [ pkgs.stdenv.cc.cc.lib ];
        dontBuild = true;
        unpackPhase = "true";
        installPhase = ''
          mkdir -p $out/bin wheel
          ${pkgs.unzip}/bin/unzip -q $src -d wheel
          install -m755 wheel/ruff-0.12.12.data/scripts/ruff $out/bin/ruff
        '';
      };
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
          pkgs.postgresql  # psql
          pkgs.gettext     # envsubst
          ruff             # pinned to poetry.lock version
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

          # Poetry installs ruff as a generic PyPI binary, which NixOS cannot
          # execute (no nix-ld). Put the patched Nix ruff ahead of it.
          export PATH="${ruff}/bin:$PATH"

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
