{
  buildPgrxExtension,
  cargo-pgrx_0_16_0,
  fetchFromGitHub,
  lib,
  postgresql,
}:
buildPgrxExtension (finalAttrs: {
  pname = "pg_parquet";
  version = "0.5.1";

  src = fetchFromGitHub {
    owner = "CrunchyData";
    repo = "pg_parquet";
    tag = "v${finalAttrs.version}";
    hash = "sha256-UhocMFETxcnKvi1IDI3ASIrslL1C4bfEdplUeRdxnOg=";
  };

  cargoHash = "sha256-Ena4/68JHpwzQBh2/PLpYzYqfY8JcCgwT3agBUliGvU=";

  # Tests require a running PostgreSQL instance with pg_parquet loaded.
  doCheck = false;

  inherit postgresql;
  cargo-pgrx = cargo-pgrx_0_16_0;

  meta = {
    homepage = "https://github.com/CrunchyData/pg_parquet";
    description = "Copy data to and from Parquet files in S3 or local storage directly from PostgreSQL";
    license = lib.licenses.postgresql;
    platforms = postgresql.meta.platforms;
    changelog = "https://github.com/CrunchyData/pg_parquet/releases/tag/v${finalAttrs.version}";
  };
})
