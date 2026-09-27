# dev-app-ctp

Streamlit data-profiling app for Databricks Apps. Source copied from the
`data-profiling-app` workspace folder.

## Files

| File | Purpose |
| --- | --- |
| `app.py` | Streamlit UI and profiling logic (Databricks Connect serverless session) |
| `app.yaml` | App entrypoint and env vars |
| `requirements.txt` | Python dependencies |

## Configuration

`TABLE_FQN` in `app.py` sets the profiled table (currently
`uc_vdm_salesmargin.default.vendor_master`). The app's service principal needs
`SELECT` on that table.

The listening port is not hardcoded: `app.yaml` passes
`$DATABRICKS_APP_PORT`, which the Apps runtime injects.

## Deploy

From the repo copy:

```bash
databricks apps get dev-app-ctp --output JSON       # must be RUNNING first
databricks apps start dev-app-ctp --timeout 20m     # only if STOPPED
databricks apps deploy dev-app-ctp \
  --source-code-path /Workspace/Users/<user>/dbx-app/dev-app-ctp
```

Alternatively, configure `git_repository` on the app and deploy from a branch;
the app's service principal then needs a Git credential for the provider.
