# src_old

The frozen legacy system, kept only as the source to port from and never built by checks or CI; the running archiver
builds from the `legacy` branch instead, and this directory is deleted at pivot task 27. Its test reading
`data/archive_provenance.json` fails here, since that file is deleted on master.
