"""
Convert plain .py notebooks to Fabric Git source format.

Fabric requires:
  - Prologue: "# Fabric notebook source"
  - METADATA block with kernel_info
  - Each code chunk wrapped in # CELL *** + # METADATA ***
  - Markdown cells with # MARKDOWN ***
  - %run directives in their own cells (un-commented, flat name)
"""

import pathlib, re, sys, textwrap

NOTEBOOK_DIR = pathlib.Path(__file__).parent / "assets" / "notebooks"

PROLOGUE = "# Fabric notebook source"

HEADER_META = """\

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }
"""

CELL_META = """\

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
"""

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

def make_run_cell(module_name: str) -> str:
    return f"\n# CELL ********************\n\n%run {module_name}\n{CELL_META}"

def convert_file(path: pathlib.Path) -> str:
    raw = path.read_text(encoding="utf-8")
    lines = raw.splitlines()

    out = PROLOGUE + HEADER_META

    # Skip old prologue lines (first 1-3 lines that are comments)
    i = 0
    header_comments = []
    while i < len(lines) and lines[i].startswith("#"):
        text = lines[i].lstrip("# ").strip()
        if text and text != "Fabric notebook source" and text != "Fabric Notebook" and not text.startswith("Fabric Notebook"):
            header_comments.append(lines[i])
        i += 1

    if header_comments:
        out += make_markdown_cell(header_comments)

    # Skip blank lines after header
    while i < len(lines) and not lines[i].strip():
        i += 1

    # Process remaining lines: split at %run directives and large gaps
    code_buffer = []

    def flush_buffer():
        nonlocal code_buffer
        if code_buffer:
            code = "\n".join(code_buffer)
            if code.strip():
                return make_code_cell(code)
            code_buffer = []
        return ""

    while i < len(lines):
        line = lines[i]

        # %run directive (commented or not)
        run_match = re.match(r"^#?\s*%run\s+(.+)", line)
        if run_match:
            out += flush_buffer()
            code_buffer = []
            raw_path = run_match.group(1).strip()
            module_name = raw_path.split("/")[-1]
            out += make_run_cell(module_name)
            i += 1
            # skip blank line after %run
            while i < len(lines) and not lines[i].strip():
                i += 1
            continue

        code_buffer.append(line)
        i += 1

    out += flush_buffer()
    code_buffer = []

    return out


def main():
    files = sorted(NOTEBOOK_DIR.rglob("*.py"))
    if not files:
        print("No .py files found"); return

    for f in files:
        converted = convert_file(f)
        f.write_text(converted, encoding="utf-8", newline="\r\n")
        cell_count = converted.count("# CELL **")
        run_count = converted.count("%run ")
        print(f"  {f.name:<40s}  {cell_count} cells  ({run_count} %run)")

    print(f"\nConverted {len(files)} notebooks to Fabric Git source format.")


if __name__ == "__main__":
    main()
