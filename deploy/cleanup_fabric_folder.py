"""
Delete all items and sub-folders inside a Fabric workspace folder.

Usage:
    python cleanup_fabric_folder.py                          # uses deploy.config.toml defaults
    python cleanup_fabric_folder.py --folder "IBP Forecast"  # explicit folder name
    python cleanup_fabric_folder.py --dry-run                # preview without deleting

Reads workspace_id from deploy.config.toml (same dir) or pass --workspace-id.

Lessons learned from Fabric REST API cleanup:
  - Deleting too many items in rapid succession triggers 429 / UnknownError.
    Fix: pause 3-5s between each DELETE, longer between item-type batches.
  - SQL Endpoints are auto-managed by their parent Lakehouse; never delete them
    directly (you'll get 400 errors or they just come back).
  - Lakehouses must be deleted AFTER notebooks/pipelines that reference them,
    otherwise the delete can hang or fail with dependency errors.
  - Folders must be deleted bottom-up (leaf folders first). A folder with items
    still inside returns FolderNotEmpty even if you just deleted the items
    (eventual consistency). Fix: multi-pass with delays.
  - Semantic models sometimes need a few seconds after deletion before the name
    is freed for re-creation (ItemDisplayNameAlreadyInUse).
  - Token refresh: Fabric tokens expire in ~5 min. Refresh before each batch.
  - LRO: some DELETEs return 202 with a Location header. Poll until complete.
"""

import argparse
import json
import pathlib
import subprocess
import sys
import time

CONFIG_PATH = pathlib.Path(__file__).parent / "deploy.config.toml"

DELETE_ORDER = [
    "DataPipeline",
    "SemanticModel",
    "Notebook",
    "MLExperiment",
    "MLModel",
    "Lakehouse",
    "Warehouse",
    "Dataflow",
]

SKIP_TYPES = {"SQLEndpoint"}

BATCH_PAUSE_S = 5
ITEM_PAUSE_S = 3
MAX_RETRIES = 3
RETRY_BACKOFF_S = 8
FOLDER_DELETE_PAUSE_S = 8
FOLDER_DELETE_PASSES = 3


def get_token() -> str:
    result = subprocess.run(
        "az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv",
        capture_output=True, text=True, check=True, shell=True,
    )
    return result.stdout.strip()


def api_get(url: str, token: str) -> dict:
    import urllib.request
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def api_delete(url: str, token: str) -> int:
    import urllib.request
    req = urllib.request.Request(url, method="DELETE",
                                headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return 404
        raise


def get_all_items(workspace_id: str, token: str) -> list[dict]:
    items = []
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    while url:
        data = api_get(url, token)
        items.extend(data.get("value", []))
        url = data.get("continuationUri")
    return items


def get_all_folders(workspace_id: str, token: str) -> list[dict]:
    data = api_get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders",
        token,
    )
    return data.get("value", [])


def find_folder_tree(folders: list[dict], root_folder_id: str) -> set[str]:
    """Return set of all folder IDs under (and including) root_folder_id."""
    tree = {root_folder_id}
    changed = True
    while changed:
        changed = False
        for f in folders:
            fid = f["id"]
            parent = f.get("parentFolderId")
            if parent in tree and fid not in tree:
                tree.add(fid)
                changed = True
    return tree


def get_item_folder(workspace_id: str, item_id: str, token: str) -> str | None:
    try:
        detail = api_get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}",
            token,
        )
        return detail.get("folderId")
    except Exception:
        return None


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    return tomllib.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser(description="Delete all Fabric items in a folder")
    parser.add_argument("--workspace-id", default="")
    parser.add_argument("--folder", default="", help="Folder display name (default: from config)")
    parser.add_argument("--dry-run", action="store_true", help="List items without deleting")
    args = parser.parse_args()

    config = load_config()
    workspace_id = args.workspace_id or config.get("fabric", {}).get("workspace_id", "")
    folder_name = args.folder or config.get("naming", {}).get("project_folder", "IBP Forecast")

    if not workspace_id:
        print("ERROR: No workspace_id. Set in config or pass --workspace-id.")
        sys.exit(1)

    print(f"Workspace:     {workspace_id}")
    print(f"Target folder: {folder_name}")
    print(f"Dry run:       {args.dry_run}\n")

    token = get_token()

    # Find the root folder
    folders = get_all_folders(workspace_id, token)
    root = None
    for f in folders:
        if f["displayName"] == folder_name and not f.get("parentFolderId"):
            root = f
            break
    if not root:
        for f in folders:
            if f["displayName"] == folder_name:
                root = f
                break
    if not root:
        print(f"ERROR: Folder '{folder_name}' not found in workspace.")
        sys.exit(1)

    root_id = root["id"]
    folder_ids = find_folder_tree(folders, root_id)
    folder_names = {f["id"]: f["displayName"] for f in folders if f["id"] in folder_ids}
    print(f"Folder tree ({len(folder_ids)} folders):")
    for fid, fname in folder_names.items():
        print(f"  {fname} ({fid})")

    # Identify items in folder tree
    print("\nScanning items...")
    all_items = get_all_items(workspace_id, token)
    target_items = []
    for item in all_items:
        if item["type"] in SKIP_TYPES:
            continue
        fid = get_item_folder(workspace_id, item["id"], token)
        if fid and fid in folder_ids:
            target_items.append({
                "id": item["id"],
                "name": item["displayName"],
                "type": item["type"],
                "folderId": fid,
            })

    print(f"\nFound {len(target_items)} items to delete:")
    by_type: dict[str, list] = {}
    for item in target_items:
        by_type.setdefault(item["type"], []).append(item)
    for t, items in sorted(by_type.items()):
        print(f"  {t}: {len(items)}")
        for it in items:
            print(f"    {it['name']}")

    if args.dry_run:
        print("\n[DRY RUN] No items deleted.")
        return

    # Delete items in type-priority order
    deleted = 0
    failed = []

    for item_type in DELETE_ORDER:
        items = by_type.pop(item_type, [])
        if not items:
            continue
        print(f"\n--- Deleting {len(items)} {item_type}(s) ---")
        token = get_token()
        for item in items:
            success = False
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    status = api_delete(
                        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item['id']}",
                        token,
                    )
                    if status == 404:
                        print(f"  {item['name']}: already gone")
                    else:
                        print(f"  {item['name']}: deleted (HTTP {status})")
                    deleted += 1
                    success = True
                    break
                except Exception as e:
                    err_msg = str(e)
                    if "429" in err_msg or "UnknownError" in err_msg or "503" in err_msg:
                        wait = RETRY_BACKOFF_S * attempt
                        print(f"  {item['name']}: throttled (attempt {attempt}/{MAX_RETRIES}), waiting {wait}s...")
                        time.sleep(wait)
                        token = get_token()
                    else:
                        print(f"  {item['name']}: FAILED ({e})")
                        break
            if not success:
                failed.append(item)
            time.sleep(ITEM_PAUSE_S)
        time.sleep(BATCH_PAUSE_S)

    # Handle any remaining types not in DELETE_ORDER
    for item_type, items in by_type.items():
        print(f"\n--- Deleting {len(items)} {item_type}(s) (unlisted type) ---")
        token = get_token()
        for item in items:
            try:
                api_delete(
                    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item['id']}",
                    token,
                )
                print(f"  {item['name']}: deleted")
                deleted += 1
            except Exception as e:
                print(f"  {item['name']}: FAILED ({e})")
                failed.append(item)
            time.sleep(ITEM_PAUSE_S)

    # Delete folders bottom-up (multi-pass for eventual consistency)
    print(f"\n--- Deleting {len(folder_ids)} folders (bottom-up, {FOLDER_DELETE_PASSES} passes) ---")
    remaining_folders = set(folder_ids)

    for pass_num in range(1, FOLDER_DELETE_PASSES + 1):
        if not remaining_folders:
            break
        print(f"\n  Pass {pass_num}/{FOLDER_DELETE_PASSES} ({len(remaining_folders)} folders remaining)")
        token = get_token()
        time.sleep(FOLDER_DELETE_PAUSE_S)

        # Sort: deepest folders first (those whose ID is NOT a parent of any other)
        parent_ids = {f.get("parentFolderId") for f in folders if f["id"] in remaining_folders}
        leaf_first = [fid for fid in remaining_folders if fid not in parent_ids]
        non_leaf = [fid for fid in remaining_folders if fid in parent_ids]
        ordered = leaf_first + non_leaf

        for fid in ordered:
            fname = folder_names.get(fid, fid)
            try:
                api_delete(
                    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders/{fid}",
                    token,
                )
                print(f"    {fname}: deleted")
                remaining_folders.discard(fid)
            except Exception as e:
                err = str(e)
                if "404" in err:
                    print(f"    {fname}: already gone")
                    remaining_folders.discard(fid)
                elif "FolderNotEmpty" in err or "409" in err:
                    print(f"    {fname}: not empty yet (will retry)")
                else:
                    print(f"    {fname}: FAILED ({e})")
            time.sleep(ITEM_PAUSE_S)

    # Summary
    print(f"\n{'='*60}")
    print(f"Deleted:  {deleted} items")
    print(f"Failed:   {len(failed)} items")
    print(f"Folders:  {len(folder_ids) - len(remaining_folders)} deleted, {len(remaining_folders)} remaining")
    if failed:
        print("\nFailed items:")
        for item in failed:
            print(f"  {item['name']} ({item['type']}) -- {item['id']}")
    if remaining_folders:
        print("\nRemaining folders (may need manual cleanup):")
        for fid in remaining_folders:
            print(f"  {folder_names.get(fid, fid)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
