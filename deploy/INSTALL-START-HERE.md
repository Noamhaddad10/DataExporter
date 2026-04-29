# START HERE — Data Exporter installation on the production PC

This kit installs the Data Exporter pipeline on a Windows PC that has no
internet access. Follow the steps below in order. Total time: ~30 minutes,
of which only ~5 minutes need you actively in front of the screen.

> If anything fails, see the **Troubleshooting** section at the bottom, or
> read `data-exporter\docs\INSTALL-PROD.md` for the manual step-by-step
> equivalent of these scripts.

---

## What the kit does for you

| Script | Run by | What it does | Time |
|---|---|---|---|
| `1-install-prereqs.ps1` | **YOU**, once | Installs Python 3.10, Java 17, Kafka 3.9.0, ODBC Driver 17 | 15-25 min |
| `2-deploy-project.ps1`  | **YOU**, every update | Copies the project, builds a Python venv, installs all packages offline, formats Kafka storage | 2-3 min |

You can re-run `2-deploy-project.ps1` any time the project code changes.
You should normally only run `1-install-prereqs.ps1` once per PC.

---

## Step 0 — Before you start

Have these things ready:

- [ ] **Administrator password** for the production PC (needed for Step 2 only)
- [ ] **Name of the SQL database** where the data will be written (ask your DBA, e.g. `prod_db`)
- [ ] **SQL Server address** (e.g. `localhost\SQLEXPRESS`, or an IP, or a server name — ask your DBA)
- [ ] **Confirmation from your DBA** that the SQL database already has:
  - schema `dis` with 6 tables: `FirePdu`, `Entities`, `EntityLocations`, `DetonationPdu`, `Aggregates`, `AggregateLocations`
  - schema `dbo` with table `Loggers`

If you don't have any of these, **stop and get them first**. The install will
not work without them.

---

## Step 1 — Copy the kit to the production PC

1. Plug the USB drive into the production PC.
2. Copy the entire `transfer-kit` folder to a local disk. Recommended location:
   ```
   C:\install-data-exporter\
   ```
   It does not really matter where, as long as the path has no spaces.
3. Eject the USB.

---

## Step 2 — Install prerequisites (admin, run ONCE)

> **Skip this step if Python 3.10, Java 17, Kafka 3.9.0 and ODBC 17 are already
> installed on the PC.** The script auto-detects what's there and skips it,
> so re-running it is safe — but if everything's already there, there's no
> point.

1. Open the kit folder you copied (e.g. `C:\install-data-exporter\transfer-kit\`).
2. **Right-click** on `1-install-prereqs.ps1`.
3. Choose **"Run with PowerShell"**.
4. Windows asks: *"Do you want to allow this app to make changes?"* → click **Yes**.
5. A black window opens. The script tells you what it's doing in colored text:
   - `[OK]` (green) = step succeeded
   - `[SKIP]` (yellow) = already installed, nothing to do
   - `[FAIL]` (red) = something failed, read the error message
6. Wait until you see **"All prerequisites installed"**. This takes 15-25 min.
7. Close the window.

> **If Windows blocks the script** ("running scripts is disabled"):
> Open PowerShell as Administrator and run:
> ```powershell
> Set-ExecutionPolicy -Scope LocalMachine RemoteSigned -Force
> ```
> Then retry the right-click → Run with PowerShell.

---

## Step 3 — Deploy the project (no admin)

1. Same kit folder. **Right-click** on `2-deploy-project.ps1`.
2. Choose **"Run with PowerShell"** (no UAC prompt this time).
3. The script will:
   - Verify all prerequisites are present
   - Wipe `C:\WiresharkLogger\` if it exists, and recreate it from the kit
   - Create a Python virtual environment in `C:\WiresharkLogger\.venv\`
   - Install all Python packages from local wheels (offline)
   - Run a smoke test (imports the project + checks `AggregateStatePdu`)
   - Format Kafka storage if it's the first time
4. Wait until you see **"Deployment OK"** (green). This takes 2-3 min.
5. The script ends by printing **"WHAT YOU MUST DO NEXT (manually)"**. Read it.

> If you re-run this later (e.g. to update the project code), it will preserve
> the `DataExporterConfig.json` you edited — so you only need to do Step 4 once.

---

## Step 4 — Configure SQL connection (manually)

The script created `C:\WiresharkLogger\DataExporterConfig.json` from the
template, but **you must edit two values** before launching the pipeline.

1. Open the file in Notepad:
   ```
   C:\WiresharkLogger\DataExporterConfig.json
   ```
2. Find these two lines:
   ```json
   "database_name": "TODO_NOM_BASE_PROD",
   "sql_server":    "TODO_localhost\\SQLEXPRESS_OU_SERVEUR_PROD",
   ```
3. Replace with the real values you collected in Step 0:
   ```json
   "database_name": "prod_db",
   "sql_server":    "localhost\\SQLEXPRESS",
   ```
   > **Important**: in JSON, the backslash must be doubled (`\\`).
4. Save and close.

---

## Step 5 — Launch and verify

1. Go to `C:\WiresharkLogger\` and double-click **`Launch DataExporter.bat`**.
2. The launcher window opens. Switch to the **PROD** preset.
3. Click **▶ START EVERYTHING**.
4. Wait until you see **"Pipeline ready"** in the live log (~30-60 seconds).
5. The 3 status dots at the top should all be green:
   - **Kafka** = up
   - **SQL** = up
   - **Pipeline** = running

If anything is red, click **STOP** and read the live log. The most common
problem is Step 4 (wrong DB name or SQL server address).

---

## Re-running later (project updates)

When you receive an updated kit (e.g. with a newer `launcher.py`):

1. Copy the new kit on top of the old one (overwrite).
2. Re-run only **`2-deploy-project.ps1`**.
3. Your `DataExporterConfig.json` is preserved.
4. Re-launch.

---

## Troubleshooting

### "Running scripts is disabled on this system"
Windows blocks PowerShell scripts by default. Open PowerShell as Admin and run:
```powershell
Set-ExecutionPolicy -Scope LocalMachine RemoteSigned -Force
```

### `1-install-prereqs.ps1` fails on Python install
Reboot the PC and retry. If still fails, see `data-exporter\docs\INSTALL-PROD.md`
section "Phase 3.1 Python" to install manually with the GUI installer.

### `2-deploy-project.ps1` fails with "pip install failed"
Usually means a wheel is missing from `python-offline-packages\`. Verify
the folder has 15 `.whl` files (see `MANIFEST.md`).

### Pipeline starts but SQL stays empty
- Check Step 4: are `database_name` and `sql_server` correct?
- Check with your DBA: are the 6 tables in the `dis` schema actually created?
- See `data-exporter\docs\INSTALL-PROD.md` "Phase 4.5 Manual smoke test".

### Kafka crashes with "log dirs have failed"
The launcher's `Reset Kafka state at start` checkbox should prevent this.
Make sure it's checked in the PROD preset (it's the default).

### Need a clean restart of just the project
Re-run `2-deploy-project.ps1`. It wipes and recreates everything except
your `DataExporterConfig.json`.

---

## Reference

- **Full manual install steps**: `data-exporter\docs\INSTALL-PROD.md`
- **Kit content inventory**: `MANIFEST.md`
- **Project README**: `data-exporter\docs\README.md`
