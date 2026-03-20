"""
Generate the semantic model BIM definition from the template.
Replaces placeholders with actual lakehouse connection info.
"""

import json
import pathlib
import sys

TEMPLATE = pathlib.Path(__file__).parent / "assets" / "semantic_models" / "ibp_forecast_model.bim.template"
OUT_DIR = pathlib.Path(__file__).parent / "assets" / "semantic_models"


def generate(gold_sql_endpoint: str, gold_lakehouse_name: str):
    raw = TEMPLATE.read_text(encoding="utf-8")
    raw = raw.replace("{{GOLD_SQL_ENDPOINT}}", gold_sql_endpoint)
    raw = raw.replace("{{GOLD_LAKEHOUSE_NAME}}", gold_lakehouse_name)

    model = json.loads(raw)
    out_path = OUT_DIR / "ibp_forecast_model.bim"
    out_path.write_text(json.dumps(model, indent=2), encoding="utf-8")
    print(f"Generated semantic model BIM: {out_path}")
    return out_path


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python generate_semantic_model.py <gold_sql_endpoint> <gold_lakehouse_name>")
        sys.exit(1)
    generate(sys.argv[1], sys.argv[2])
