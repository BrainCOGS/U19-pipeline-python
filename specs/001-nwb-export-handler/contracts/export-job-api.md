# Contract: Export Job API (Internal)

## submit_nwb_export_job

### Input

```yaml
session_key: object
user_id: string
modalities: list[str]  # valid values: behavior, ephys-raw, ephys-processed, imaging-raw, imaging-processed
output_filepath: string
upload_to_dandi: boolean
```

### Output

```yaml
nwb_job_id: integer
status: QUEUED
```

## get_job_status

### Input

```yaml
nwb_job_id: integer
```

### Output

```yaml
nwb_job_id: integer
status: string
status_timestamp: string
```

## can_upload_to_dandi

### Input

```yaml
user_id: string
```

### Output

```yaml
upload_allowed: boolean
reason: string
```

Rules:
- `upload_allowed=true` only when both encrypted API key and dandiset ID are present.
- Missing either credential is not an error for overall job completion.
