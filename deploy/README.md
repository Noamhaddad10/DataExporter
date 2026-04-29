# `deploy/` — production install scripts

This folder contains the scripts and walkthrough document used to install the
Data Exporter pipeline on a fresh Windows production PC (typically air-gapped,
no internet access). They are the source of truth — copies are bundled into
the offline transfer kit (USB drive) for actual delivery.

## Files

| File | Purpose |
|---|---|
| `INSTALL-START-HERE.md` | Step-by-step walkthrough for the operator running the install on prod. Plain English, ~5 manual minutes total. |
| `1-install-prereqs.ps1` | PowerShell, **admin required**, run **once** per PC. Idempotent install of Python 3.10, Java 17, Apache Kafka 3.9.0, ODBC Driver 17 from the kit's `installers/` folder. |
| `2-deploy-project.ps1` | PowerShell, **no admin**, re-runnable. Wipes `C:\WiresharkLogger\`, copies the project from the kit, creates a venv, runs `pip install --no-index --find-links ../python-offline-packages/ -r requirements.txt`, smoke-tests imports, formats Kafka storage if first-time. Preserves `DataExporterConfig.json` if the operator has already edited it. |

## Relationship with the transfer kit

These three files are mirrored verbatim at the root of the offline transfer
kit (`transfer-kit/`). The kit is what gets copied to a USB drive and brought
to the production PC. Whenever you change anything in this folder, copy the
modified file to the kit too — there is no automated sync today.

```
transfer-kit/
├── INSTALL-START-HERE.md       <- copy of deploy/INSTALL-START-HERE.md
├── 1-install-prereqs.ps1       <- copy of deploy/1-install-prereqs.ps1
├── 2-deploy-project.ps1        <- copy of deploy/2-deploy-project.ps1
├── installers/                 <- Python, Java, Kafka, ODBC installers (.exe / .msi / .tgz)
├── python-offline-packages/    <- 15 pre-downloaded wheels for offline pip install
├── data-exporter/              <- the project source tree
├── DataExporterConfig.json.template
└── MANIFEST.md
```

## Manual fallback

If anything in `1-install-prereqs.ps1` or `2-deploy-project.ps1` fails on
prod, the operator can fall back to the manual 12-phase walkthrough in
`docs/INSTALL-PROD.md`. The scripts in this folder do exactly the same thing,
just automated and idempotent.

## Why PowerShell, not bash

The production PC is Windows-only. Bash is not available out of the box on a
fresh Win10/11 install, but PowerShell is. PowerShell also has first-class
support for MSI installers, registry checks, environment variable management,
and built-in `tar` (since Windows 10 1803).

## Testing changes

Before bundling a modified script into a new kit:

1. Update the file under `deploy/`.
2. Copy it into `transfer-kit/` (manual today; consider scripting later).
3. Re-run `2-deploy-project.ps1` against `C:\DataExporter-prod-test\install-test-clean\`
   on the dev machine — this is a clean offline simulation of a prod install.
4. Verify the smoke test prints `SMOKE OK` at the end.
