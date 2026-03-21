"""Trigger a Fabric pipeline and poll until completion."""
import sys, time, requests, subprocess, json

WS = "WORKSPACE_ID_PLACEHOLDER"

def get_token():
    return subprocess.check_output(
        ["powershell", "-Command",
         'az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv'],
        text=True,
    ).strip()

def find_pipeline(name, headers):
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items?type=DataPipeline",
        headers=headers,
    )
    resp.raise_for_status()
    for item in resp.json().get("value", []):
        if item["displayName"] == name:
            return item["id"]
    raise ValueError(f"Pipeline '{name}' not found")

def run_pipeline(name):
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    pipeline_id = find_pipeline(name, headers)
    print(f"\n{'='*60}")
    print(f"Pipeline: {name} ({pipeline_id})")
    print(f"{'='*60}")

    run_resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
        headers=headers,
    )
    print(f"Trigger status: {run_resp.status_code}")

    if run_resp.status_code not in (200, 202):
        print(f"ERROR: {run_resp.text}")
        return False

    location = run_resp.headers.get("Location", "")
    if not location:
        print("No polling URL returned, checking job status via API...")
        retry_after = int(run_resp.headers.get("Retry-After", "30"))
        location = f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items/{pipeline_id}/jobs/instances"

    retry_after = int(run_resp.headers.get("Retry-After", "30"))
    print(f"Polling every {retry_after}s...")

    start = time.time()
    last_status = ""
    while True:
        time.sleep(retry_after)
        elapsed = int(time.time() - start)

        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        try:
            poll = requests.get(location, headers=headers)
            if poll.status_code == 200:
                data = poll.json()
                if isinstance(data, dict):
                    status = data.get("status", data.get("Status", "Unknown"))
                    if status != last_status:
                        print(f"  [{elapsed:>4d}s] {status}")
                        last_status = status
                    if status in ("Completed", "Succeeded"):
                        print(f"\nPipeline {name} completed successfully in {elapsed}s")
                        return True
                    if status in ("Failed", "Cancelled"):
                        failure = data.get("failureReason", data.get("error", ""))
                        print(f"\nPipeline {name} FAILED after {elapsed}s")
                        print(f"  Reason: {failure}")
                        print(f"  Full response: {json.dumps(data, indent=2)}")
                        return False
                elif isinstance(data, list):
                    if data:
                        latest = data[0] if isinstance(data[0], dict) else data[-1]
                        status = latest.get("status", "Unknown")
                        if status != last_status:
                            print(f"  [{elapsed:>4d}s] {status}")
                            last_status = status
                        if status in ("Completed", "Succeeded"):
                            print(f"\nPipeline {name} completed successfully in {elapsed}s")
                            return True
                        if status in ("Failed", "Cancelled"):
                            print(f"\nPipeline {name} FAILED after {elapsed}s")
                            print(f"  {json.dumps(latest, indent=2)}")
                            return False
            elif poll.status_code == 202:
                print(f"  [{elapsed:>4d}s] Running...")
            else:
                print(f"  [{elapsed:>4d}s] Poll returned {poll.status_code}")
        except Exception as e:
            print(f"  [{elapsed:>4d}s] Poll error: {e}")

        if elapsed > 3600:
            print(f"\nTIMEOUT after {elapsed}s")
            return False

if __name__ == "__main__":
    pipeline_name = sys.argv[1] if len(sys.argv) > 1 else "pl_ibp_train"
    success = run_pipeline(pipeline_name)
    sys.exit(0 if success else 1)
