{ pkgs, lib, config, inputs, ... }:

{
  packages = [ pkgs.git ];

  dotenv.enable = true;

  languages = {
    rust.enable = true;
    python.enable = true;
    nix.enable = true;
  };

  enterShell = ''
  '';

  tasks = {
    "models:tide:deploy".exec = ''
      uv run prefect --no-prompt deploy --name tide-training
      '';
    "models:tide:train".exec = ''
      uv run prefect deployment run tide-training-pipeline/tide-training
      '';
  };

  enterTest = ''
  '';

}
