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
      uvx prefect-cloud deploy models/tide/src/tide/workflow.py:training_pipeline \
        --from oscmcompany/fund \
        --name tide-training
      '';
  };

  enterTest = ''
  '';

  git-hooks.hooks.shellcheck.enable = true;

}
