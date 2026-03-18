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
      branch=$(git rev-parse --abbrev-ref HEAD)
      uvx prefect-cloud deploy models/tide/src/tide/workflow.py:training_pipeline \
        --from "oscmcompany/fund/tree/$branch" \
        --name tide-training \
        --with boto3 \
        --with polars \
        --with structlog \
        --with prefect \
        --with tinygrad \
        --with numpy \
        --with "pandera[polars]" \
        --with requests
      '';
    "models:tide:train".exec = ''
      uvx prefect-cloud run training_pipeline/tide-training
      '';
  };

  enterTest = ''
  '';

}
