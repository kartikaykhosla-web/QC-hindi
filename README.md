# QC Hindi

Streamlit app for Hindi editorial QC using Gemini on Vertex AI.

## Main app

- `qc_code_hindi.py`

## Supporting files

- `hindi_qc_rules.txt`
- `requirements.txt`
- `packages.txt`

## Local run

```bash
streamlit run qc_code_hindi.py
```

## Required secret

Add `GCP_SERVICE_ACCOUNT_JSON_B64` in Streamlit secrets.

## Keep-awake workflow

This repo includes a GitHub Actions workflow at `.github/workflows/keep_streamlit_awake.yml`
that pings the deployed app every 6 hours.

Set a repository variable named `STREAMLIT_APP_URL` to your deployed app URL, for example:

```text
https://your-app-name.streamlit.app
```
