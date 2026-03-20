"""
Convert plain .py notebooks to Fabric Git source format and inject default lakehouse.

Reads from  deploy/assets/notebooks/{main,modules}/*.py   (NEVER modified)
Writes to   deploy/build/notebooks/{main,modules}/*.py     (overwritten each run)

Usage:
    python convert_notebooks.py
    python convert_notebooks.py --workspace-id WS --source-id S ...
"""

import argparse, pathlib, re, shutil

SRC_DIR = pathlib.Path(__file__).parent / "assets" / "notebooks"
BUILD_DIR = pathlib.Path(__file__).parent / "build" / "notebooks"

PROLOGUE = "# Fabric notebook source"

LAKEHOUSE_MAP = {
    "00_generate_test_data": "source",
    "01_ingest_sources":     "landing",
    "02_transform_bronze":   "bronze",
    "03_feature_engineering": "silver",
    "04_train":              "silver",
    "05_score_forecast":     "gold",
    "06_version_snapshot":   "gold",
    "07_demand_to_capacity": "gold",
    "08_sales_overrides":    "gold",
    "09_market_adjustments": "gold",
    "10_consensus_build":    "gold",
    "11_accuracy_tracking":  "gold",
    "12_aggregate_gold":     "gold",
    "13_budget_comparison":        "gold",
    "14_build_reporting_view":     "gold",
    "15_refresh_semantic_model":   "gold",
    "P2_":                         "gold",
}


def get_default_lakehouse(filename: str) -> str:
    stem = pathlib.Path(filename).stem
    for prefix, tier in LAKEHOUSE_MAP.items():
        if stem.startswith(prefix):
            return tier
    return "gold"


def build_header_meta(workspace_id: str, lakehouse_ids: dict, lakehouse_names: dict,
                      filename: str) -> str:
    if not workspace_id or not any(lakehouse_ids.values()):
        return (
            "\n# METADATA ********************\n"
            "\n"
            "# META {\n"
            '# META   "kernel_info": {\n'
            '# META     "name": "synapse_pyspark"\n'
            "# META   },\n"
            '# META   "dependencies": {}\n'
            "# META }\n"
        )

    tier = get_default_lakehouse(filename)
    lh_id = lakehouse_ids.get(tier, "")
    lh_name = lakehouse_names.get(tier, "")

    known = []
    for t in ["source", "landing", "bronze", "silver", "gold"]:
        lid = lakehouse_ids.get(t)
        if lid:
            known.append(f'{{"id": "{lid}"}}')
    known_str = ",\n# META         ".join(known)

    return (
        "\n# METADATA ********************\n"
        "\n"
        "# META {\n"
        '# META   "kernel_info": {\n'
        '# META     "name": "synapse_pyspark"\n'
        "# META   },\n"
        '# META   "dependencies": {\n'
        '# META     "lakehouse": {\n'
        f'# META       "default_lakehouse": "{lh_id}",\n'
        f'# META       "default_lakehouse_name": "{lh_name}",\n'
        f'# META       "default_lakehouse_workspace_id": "{workspace_id}",\n'
        '# META       "known_lakehouses": [\n'
        f"# META         {known_str}\n"
        "# META       ]\n"
        "# META     }\n"
        "# META   }\n"
        "# META }\n"
    )


CELL_META = (
    "\n# METADATA ********************\n"
    "\n"
    "# META {\n"
    '# META   "language": "python",\n'
    '# META   "language_group": "synapse_pyspark"\n'
    "# META }\n"
)

PARAM_CELL_META = (
    "\n# METADATA ********************\n"
    "\n"
    "# META {\n"
    '# META   "language": "python",\n'
    '# META   "language_group": "synapse_pyspark",\n'
    '# META   "tags": [\n'
    '# META     "parameters"\n'
    "# META   ]\n"
    "# META }\n"
)


def make_markdown_cell(lines: list[str]) -> str:
    block = "\n# MARKDOWN ********************\n\n"
    for line in lines:
        text = line.lstrip("# ").rstrip()
        block += f"# {text}\n" if text else "# \n"
    return block


def make_code_cell(code: str) -> str:
    code = code.strip("\n")
    if not code:
        return ""
    return f"\n# CELL ********************\n\n{code}\n{CELL_META}"


def make_param_cell(code: str) -> str:
    code = code.strip("\n")
    if not code:
        return ""
    return f"\n# CELL ********************\n\n{code}\n{PARAM_CELL_META}"


def make_run_cell(module_name: str) -> str:
    return f"\n# CELL ********************\n\n%run {module_name}\n{CELL_META}"


PARAM_TO_TIER = {
    "source_lakehouse_id":  "source",
    "landing_lakehouse_id": "landing",
    "bronze_lakehouse_id":  "bronze",
    "silver_lakehouse_id":  "silver",
    "gold_lakehouse_id":    "gold",
}


def inject_lakehouse_ids(line: str, lakehouse_ids: dict) -> str:
    """Replace empty-string defaults in parameter cells with real lakehouse IDs."""
    m = re.match(r'^(\w+_lakehouse_id)\s*=\s*""(.*)$', line)
    if m:
        param_name = m.group(1)
        rest = m.group(2)
        tier = PARAM_TO_TIER.get(param_name)
        if tier:
            real_id = lakehouse_ids.get(tier, "")
            if real_id:
                return f'{param_name} = "{real_id}"{rest}'
    return line


def convert_file(path: pathlib.Path, workspace_id: str, lakehouse_ids: dict,
                 lakehouse_names: dict) -> str:
    raw = path.read_text(encoding="utf-8")
    lines = raw.splitlines()

    header_meta = build_header_meta(workspace_id, lakehouse_ids, lakehouse_names, path.name)
    out = PROLOGUE + header_meta

    i = 0
    header_comments = []
    while i < len(lines) and lines[i].startswith("#"):
        text = lines[i].lstrip("# ").strip()
        if text and not re.search(r"Fabric\s+[Nn]otebook", text):
            header_comments.append(lines[i])
        i += 1

    if header_comments:
        out += make_markdown_cell(header_comments)

    while i < len(lines) and not lines[i].strip():
        i += 1

    code_buffer: list[str] = []
    in_param_block = False

    def flush_buffer(as_param: bool = False) -> str:
        nonlocal code_buffer
        if code_buffer:
            code = "\n".join(code_buffer)
            code_buffer = []
            if code.strip():
                return make_param_cell(code) if as_param else make_code_cell(code)
        return ""

    while i < len(lines):
        line = lines[i]

        if line.strip() == "# @parameters":
            out += flush_buffer()
            in_param_block = True
            i += 1
            continue
        if line.strip() == "# @end_parameters":
            out += flush_buffer(as_param=True)
            in_param_block = False
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1
            continue

        run_match = re.match(r"^#?\s*%run\s+(.+)", line)
        if run_match:
            out += flush_buffer(as_param=in_param_block)
            in_param_block = False
            raw_path = run_match.group(1).strip()
            module_name = raw_path.split("/")[-1]
            out += make_run_cell(module_name)
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1
            continue

        if in_param_block:
            line = inject_lakehouse_ids(line, lakehouse_ids)

        code_buffer.append(line)
        i += 1

    out += flush_buffer(as_param=in_param_block)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace-id", default="")
    parser.add_argument("--source-id", default="")
    parser.add_argument("--source-name", default="lh_ibp_source")
    parser.add_argument("--landing-id", default="")
    parser.add_argument("--landing-name", default="lh_ibp_landing")
    parser.add_argument("--bronze-id", default="")
    parser.add_argument("--bronze-name", default="lh_ibp_bronze")
    parser.add_argument("--silver-id", default="")
    parser.add_argument("--silver-name", default="lh_ibp_silver")
    parser.add_argument("--gold-id", default="")
    parser.add_argument("--gold-name", default="lh_ibp_gold")
    parser.add_argument("--output-dir", default=str(BUILD_DIR))
    args = parser.parse_args()

    lakehouse_ids = {
        "source": args.source_id, "landing": args.landing_id,
        "bronze": args.bronze_id, "silver": args.silver_id, "gold": args.gold_id,
    }
    lakehouse_names = {
        "source": args.source_name, "landing": args.landing_name,
        "bronze": args.bronze_name, "silver": args.silver_name, "gold": args.gold_name,
    }

    out_dir = pathlib.Path(args.output_dir)

    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.workspace_id:
        print(f"Injecting default lakehouses (workspace: {args.workspace_id[:8]}...)")
    else:
        print("No lakehouse IDs provided -- notebooks will have empty dependencies")

    files = sorted(SRC_DIR.rglob("*.py"))
    for f in files:
        converted = convert_file(f, args.workspace_id, lakehouse_ids, lakehouse_names)

        rel = f.relative_to(SRC_DIR)
        dest = out_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(converted, encoding="utf-8", newline="\r\n")

        cell_count = converted.count("# CELL **")
        tier = get_default_lakehouse(f.name)
        lh = lakehouse_names.get(tier, "?")
        print(f"  {str(rel):<50s}  {cell_count} cells  default_lh={lh}")

    print(f"\nConverted {len(files)} notebooks to {out_dir}")


if __name__ == "__main__":
    main()
