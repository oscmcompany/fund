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

  processes.prefect-worker.exec = "uv run prefect worker start --pool fund-work-pool-local";

  tasks = {
    "models:tide:deploy".exec = ''
      uv run prefect --no-prompt deploy --all
      '';
    "models:tide:train".exec = ''
      uv run prefect deployment run tide-training-pipeline/tide-training
      '';
    "models:tide:train:local".exec = ''
      uv run prefect deployment run tide-training-pipeline/tide-training-local
      '';
  };

  enterTest = ''
  '';

}
