"""Cancel train pipeline if running, then trigger score_and_refresh."""
import requests, subprocess, time, json, sys

WS = "WORKSPACE_ID_PLACEHOLDER"

def get_token(resource="https://api.fabric.microsoft.com"):
    return subprocess.check_output(
        ["powershell", "-Command",
         f'az account get-access-token --resource "{resource}" --query accessToken -o tsv'],
        text=True,
    ).strip()

headers = lambda: {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}

TRAIN_ID = "5042b06a-e826-4afe-8b4b-7c548db5353c"

print("=== Cancelling train pipeline ===")
h = headers()
jobs_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items/{TRAIN_ID}/jobs/instances?limit=1",
    headers=h,
)
print(f"Jobs status: {jobs_resp.status_code}")
if jobs_resp.status_code == 200:
    jobs = jobs_resp.json()
    if isinstance(jobs, dict) and "value" in jobs:
        jobs = jobs["value"]
    if isinstance(jobs, list) and jobs:
        job = jobs[0]
        job_id = job.get("id", "")
        status = job.get("status", "")
        print(f"  Latest job: {job_id} status={status}")
        if status in ("InProgress", "NotStarted"):
            cancel = requests.post(
                f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items/{TRAIN_ID}/jobs/instances/{job_id}/cancel",
                headers=h,
            )
            print(f"  Cancel: {cancel.status_code}")
    elif isinstance(jobs, dict):
        job_id = jobs.get("id", "")
        status = jobs.get("status", "")
        print(f"  Job: {job_id} status={status}")
        if status in ("InProgress", "NotStarted"):
            cancel = requests.post(
                f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items/{TRAIN_ID}/jobs/instances/{job_id}/cancel",
                headers=h,
            )
            print(f"  Cancel: {cancel.status_code}")
else:
    print(f"  Response: {jobs_resp.text[:300]}")

print("\nWaiting 10s for cancellation to settle...")
time.sleep(10)

print("\n=== Triggering pl_ibp_score_and_refresh ===")
h = headers()
items = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items?type=DataPipeline",
    headers=h,
).json()
score_id = None
for item in items.get("value", []):
    if item["displayName"] == "pl_ibp_score_and_refresh":
        score_id = item["id"]
        break

if not score_id:
    print("ERROR: pl_ibp_score_and_refresh not found!")
    sys.exit(1)

print(f"Pipeline: pl_ibp_score_and_refresh ({score_id})")
run = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items/{score_id}/jobs/instances?jobType=Pipeline",
    headers=h,
)
print(f"Trigger: {run.status_code}")
if run.status_code not in (200, 202):
    print(f"  {run.text[:300]}")
    sys.exit(1)

location = run.headers.get("Location", "")
retry = int(run.headers.get("Retry-After", "30"))
print(f"Polling every {retry}s...")

start = time.time()
last_status = ""
while True:
    time.sleep(retry)
    elapsed = int(time.time() - start)
    h = headers()
    try:
        poll = requests.get(location, headers=h)
        if poll.status_code == 200:
            data = poll.json()
            status = data.get("status", "Unknown")
            if status != last_status:
                print(f"  [{elapsed:>4d}s] {status}")
                last_status = status
            if status in ("Completed", "Succeeded"):
                print(f"\nScore pipeline completed in {elapsed}s!")
                break
            if status in ("Failed", "Cancelled"):
                print(f"\nScore pipeline FAILED after {elapsed}s")
                print(f"  {json.dumps(data, indent=2)}")
                sys.exit(1)
        elif poll.status_code == 202:
            if "Running" != last_status:
                print(f"  [{elapsed:>4d}s] Running...")
                last_status = "Running"
    except Exception as e:
        print(f"  [{elapsed:>4d}s] Error: {e}")

    if elapsed > 3600:
        print("TIMEOUT")
        sys.exit(1)

print("\nDone! Score pipeline completed. All gold tables populated.")
