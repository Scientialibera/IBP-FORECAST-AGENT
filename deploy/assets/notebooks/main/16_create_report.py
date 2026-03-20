# Fabric Notebook
# 16_create_report.py
#
# Creates or updates a PBIR-Legacy report in Fabric using report.json + definition.pbir.
# No manual report/template required.
#
# This version improves the generated layout:
# - Two pages: Summary + Detail
# - Cleaner top slicer bar
# - KPI cards
# - Trend chart for Actual vs Predicted
# - Comparison charts by Model and Plant
# - Detail page with error trend + detail table
#
# Assumptions:
# - Notebook 15 already created the semantic model
# - Semantic model contains a table/entity named "Backtest Predictions"
# - Entity fields/measures:
#     Columns: period, plant_id, sku_id, model_type, actual, predicted, error, abs_error, pct_error
#     Measures: Backtest MAPE %, Total Actual, Total Predicted
#
# If your semantic model uses different names, update the constants below.

# @parameters
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module


gold_lakehouse_id = resolve_lakehouse_id(gold_lakehouse_id, "gold")

import base64
import json
import time
import requests

workspace_id = spark.conf.get("trident.workspace.id")
token = notebookutils.credentials.getToken("pbi")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

REPORT_NAME = cfg("report_name")
REPORT_DESCRIPTION = cfg("report_description")
SEMANTIC_MODEL_NAME = cfg("semantic_model_name")

# ---------------------------------------------------------------------------
# Extracted decoded source files (human-readable)
# ---------------------------------------------------------------------------
REPORT_JSON_TEXT = r"""{
  "config": "{\"version\":\"5.68\",\"themeCollection\":{\"baseTheme\":{\"name\":\"CY25SU11\",\"type\":2,\"version\":{\"visual\":\"2.4.0\",\"report\":\"3.0.0\",\"page\":\"2.3.0\"}}},\"activeSectionIndex\":0,\"defaultDrillFilterOtherVisuals\":true,\"linguisticSchemaSyncVersion\":0,\"settings\":{\"useNewFilterPaneExperience\":true,\"allowChangeFilterTypes\":true,\"useStylableVisualContainerHeader\":true,\"useDefaultAggregateDisplayName\":true,\"useEnhancedTooltips\":true}}",
  "layoutOptimization": 0,
  "resourcePackages": [
    {
      "resourcePackage": {
        "disabled": false,
        "items": [
          {
            "name": "CY25SU11",
            "path": "BaseThemes/CY25SU11.json",
            "type": 202
          }
        ],
        "name": "SharedResources",
        "type": 2
      }
    }
  ],
  "sections": [
    {
      "config": "{\"objects\":{\"background\":[{\"properties\":{\"color\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":0,\"Percent\":-0.1}}}}},\"transparency\":{\"expr\":{\"Literal\":{\"Value\":\"5D\"}}}}}]}}",
      "displayName": "Error Diagnostics",
      "displayOption": 1,
      "filters": "[]",
      "height": 768.00,
      "name": "a42e082c0a2e11c77bf1",
      "ordinal": 1,
      "visualContainers": [
        {
          "config": "{\"name\":\"0eecc5468fd3c39b086c\",\"layouts\":[{\"id\":0,\"position\":{\"x\":20.178112349975144,\"y\":93.12974930757758,\"z\":0,\"width\":765.2161068105959,\"height\":312.7607414246147}}],\"singleVisual\":{\"visualType\":\"lineChart\",\"projections\":{\"Category\":[{\"queryRef\":\"Backtest Predictions.period\",\"active\":true}],\"Y\":[{\"queryRef\":\"Sum(Backtest Predictions.abs_error)\"}],\"Series\":[{\"queryRef\":\"Backtest Predictions.model_type\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"period\"},\"Name\":\"Backtest Predictions.period\",\"NativeReferenceName\":\"Count of period\"},{\"Aggregation\":{\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"abs_error\"}},\"Function\":1},\"Name\":\"Sum(Backtest Predictions.abs_error)\",\"NativeReferenceName\":\"Average of abs_error\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"model_type\"},\"Name\":\"Backtest Predictions.model_type\",\"NativeReferenceName\":\"model_type\"}],\"OrderBy\":[{\"Direction\":1,\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"period\"}}}]},\"drillFilterOtherVisuals\":true,\"objects\":{}}}",
          "filters": "[]",
          "height": 312.76,
          "width": 765.22,
          "x": 20.18,
          "y": 93.13,
          "z": 0.00
        },
        {
          "config": "{\"name\":\"67966d0a77d339b11749\",\"layouts\":[{\"id\":0,\"position\":{\"x\":523.078758610894,\"y\":0,\"z\":6002,\"width\":280.9414104111924,\"height\":67.51906824799374}}],\"singleVisual\":{\"visualType\":\"slicer\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.plant_id\",\"active\":true}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"plant_id\"},\"Name\":\"Backtest Predictions.plant_id\"}]},\"syncGroup\":{\"groupName\":\"plant_id\",\"fieldChanges\":true,\"filterChanges\":true},\"drillFilterOtherVisuals\":true,\"objects\":{\"data\":[{\"properties\":{\"mode\":{\"expr\":{\"Literal\":{\"Value\":\"'Dropdown'\"}}}}}],\"items\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}},\"background\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}}}],\"header\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}}}}],\"general\":[{\"properties\":{\"orientation\":{\"expr\":{\"Literal\":{\"Value\":\"1D\"}}}}}]}}}",
          "filters": "[]",
          "height": 67.52,
          "width": 280.94,
          "x": 523.08,
          "y": 0.00,
          "z": 6002.00
        },
        {
          "config": "{\"name\":\"67c530a37abfcb1c674e\",\"layouts\":[{\"id\":0,\"position\":{\"x\":0,\"y\":0,\"z\":6001,\"width\":523.078758610894,\"height\":67.51906824799374}}],\"singleVisual\":{\"visualType\":\"slicer\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.model_type\",\"active\":true}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"model_type\"},\"Name\":\"Backtest Predictions.model_type\"}]},\"syncGroup\":{\"groupName\":\"model_type\",\"fieldChanges\":true,\"filterChanges\":true},\"drillFilterOtherVisuals\":true,\"objects\":{\"data\":[{\"properties\":{\"mode\":{\"expr\":{\"Literal\":{\"Value\":\"'Basic'\"}}}}}],\"items\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}},\"background\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}}}],\"header\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}}}}],\"general\":[{\"properties\":{\"orientation\":{\"expr\":{\"Literal\":{\"Value\":\"1D\"}}}}}]}}}",
          "filters": "[]",
          "height": 67.52,
          "width": 523.08,
          "x": 0.00,
          "y": 0.00,
          "z": 6001.00
        },
        {
          "config": "{\"name\":\"c4020e256502a1c0a707\",\"layouts\":[{\"id\":0,\"position\":{\"x\":812.5570627086145,\"y\":93.12974930757758,\"z\":1000,\"width\":533.1678147858817,\"height\":312.7607414246147}}],\"singleVisual\":{\"visualType\":\"clusteredColumnChart\",\"projections\":{\"Category\":[{\"queryRef\":\"Backtest Predictions.model_type\",\"active\":true}],\"Y\":[{\"queryRef\":\"Backtest Predictions.Backtest MAPE %\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"model_type\"},\"Name\":\"Backtest Predictions.model_type\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Backtest MAPE %\"},\"Name\":\"Backtest Predictions.Backtest MAPE %\",\"NativeReferenceName\":\"Backtest MAPE %\"}],\"OrderBy\":[{\"Direction\":2,\"Expression\":{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Backtest MAPE %\"}}}]},\"drillFilterOtherVisuals\":true,\"hasDefaultSort\":true,\"objects\":{\"dataPoint\":[{\"properties\":{\"fill\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}}}]}}}",
          "filters": "[]",
          "height": 312.76,
          "width": 533.17,
          "x": 812.56,
          "y": 93.13,
          "z": 1000.00
        },
        {
          "config": "{\"name\":\"c7fc2a469e39efc84ff4\",\"layouts\":[{\"id\":0,\"position\":{\"x\":20.178112349975144,\"y\":434.6054967686954,\"z\":6000,\"width\":1325.546765144521,\"height\":314.31290391307436}}],\"singleVisual\":{\"visualType\":\"tableEx\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.model_type\"},{\"queryRef\":\"Backtest Predictions.plant_id\"},{\"queryRef\":\"Backtest Predictions.sku_id\"},{\"queryRef\":\"Backtest Predictions.actual\"},{\"queryRef\":\"Backtest Predictions.predicted\"},{\"queryRef\":\"Backtest Predictions.abs_error\"},{\"queryRef\":\"Backtest Predictions.pct_error\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"model_type\"},\"Name\":\"Backtest Predictions.model_type\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"plant_id\"},\"Name\":\"Backtest Predictions.plant_id\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"sku_id\"},\"Name\":\"Backtest Predictions.sku_id\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"actual\"},\"Name\":\"Backtest Predictions.actual\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"predicted\"},\"Name\":\"Backtest Predictions.predicted\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"abs_error\"},\"Name\":\"Backtest Predictions.abs_error\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"pct_error\"},\"Name\":\"Backtest Predictions.pct_error\"}],\"OrderBy\":[{\"Direction\":1,\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"plant_id\"}}},{\"Direction\":1,\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"model_type\"}}}]},\"drillFilterOtherVisuals\":true,\"objects\":{},\"vcObjects\":{\"stylePreset\":[{\"properties\":{\"name\":{\"expr\":{\"Literal\":{\"Value\":\"'Sparse'\"}}}}}]}}}",
          "filters": "[]",
          "height": 314.31,
          "width": 1325.55,
          "x": 20.18,
          "y": 434.61,
          "z": 6000.00
        },
        {
          "config": "{\"name\":\"d9812c4ac7a7372b654f\",\"layouts\":[{\"id\":0,\"position\":{\"x\":1084.9615794332788,\"y\":0,\"z\":6004,\"width\":280.9414104111924,\"height\":67.51906824799374}}],\"singleVisual\":{\"visualType\":\"slicer\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.period\",\"active\":true}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"period\"},\"Name\":\"Backtest Predictions.period\"}]},\"syncGroup\":{\"groupName\":\"period\",\"fieldChanges\":true,\"filterChanges\":true},\"drillFilterOtherVisuals\":true,\"objects\":{\"data\":[{\"properties\":{\"mode\":{\"expr\":{\"Literal\":{\"Value\":\"'Dropdown'\"}}}}}],\"items\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}},\"background\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}}}],\"header\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}}}}],\"general\":[{\"properties\":{\"orientation\":{\"expr\":{\"Literal\":{\"Value\":\"1D\"}}}}}]}}}",
          "filters": "[]",
          "height": 67.52,
          "width": 280.94,
          "x": 1084.96,
          "y": 0.00,
          "z": 6004.00
        },
        {
          "config": "{\"name\":\"dab429fbf5e8093efb2b\",\"layouts\":[{\"id\":0,\"position\":{\"x\":804.0201690220865,\"y\":0,\"z\":6003,\"width\":280.9414104111924,\"height\":67.51906824799374}}],\"singleVisual\":{\"visualType\":\"slicer\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.sku_id\",\"active\":true}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"sku_id\"},\"Name\":\"Backtest Predictions.sku_id\"}]},\"syncGroup\":{\"groupName\":\"sku_id\",\"fieldChanges\":true,\"filterChanges\":true},\"drillFilterOtherVisuals\":true,\"objects\":{\"data\":[{\"properties\":{\"mode\":{\"expr\":{\"Literal\":{\"Value\":\"'Dropdown'\"}}}}}],\"items\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}},\"background\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}}}],\"header\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}}}}],\"general\":[{\"properties\":{\"orientation\":{\"expr\":{\"Literal\":{\"Value\":\"1D\"}}}}}]}}}",
          "filters": "[]",
          "height": 67.52,
          "width": 280.94,
          "x": 804.02,
          "y": 0.00,
          "z": 6003.00
        }
      ],
      "width": 1366.00
    },
    {
      "config": "{\"objects\":{\"background\":[{\"properties\":{\"color\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":0,\"Percent\":-0.1}}}}},\"transparency\":{\"expr\":{\"Literal\":{\"Value\":\"5D\"}}}}}]}}",
      "displayName": "Summary — Backtest Overview",
      "displayOption": 1,
      "filters": "[]",
      "height": 768.00,
      "name": "summary_page",
      "visualContainers": [
        {
          "config": "{\"name\":\"summary_bar_by_model\",\"layouts\":[{\"id\":0,\"position\":{\"x\":852.9132874085647,\"y\":177.72260492862722,\"z\":510,\"width\":502.90064626091896,\"height\":249.12207939777005}}],\"singleVisual\":{\"visualType\":\"clusteredColumnChart\",\"projections\":{\"Category\":[{\"queryRef\":\"Backtest Predictions.model_type\",\"active\":true}],\"Y\":[{\"queryRef\":\"Backtest Predictions.Total Actual\"},{\"queryRef\":\"Backtest Predictions.Total Predicted\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"model_type\"},\"Name\":\"Backtest Predictions.model_type\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Total Actual\"},\"Name\":\"Backtest Predictions.Total Actual\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Total Predicted\"},\"Name\":\"Backtest Predictions.Total Predicted\"}]},\"drillFilterOtherVisuals\":true,\"objects\":{\"dataPoint\":[{\"properties\":{\"fill\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}},\"selector\":{\"metadata\":\"Backtest Predictions.Total Actual\"}},{\"properties\":{\"fill\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":-0.5}}}}}},\"selector\":{\"metadata\":\"Backtest Predictions.Total Predicted\"}}]}}}",
          "filters": "[]",
          "height": 249.12,
          "width": 502.90,
          "x": 852.91,
          "y": 177.72,
          "z": 510.00
        },
        {
          "config": "{\"name\":\"summary_bar_by_plant\",\"layouts\":[{\"id\":0,\"position\":{\"x\":12.41729990767701,\"y\":437.70982174561465,\"z\":1000,\"width\":659.6690575953412,\"height\":314.31290391307436}}],\"singleVisual\":{\"visualType\":\"clusteredColumnChart\",\"projections\":{\"Category\":[{\"queryRef\":\"Backtest Predictions.plant_id\",\"active\":true}],\"Y\":[{\"queryRef\":\"Backtest Predictions.Total Actual\"},{\"queryRef\":\"Backtest Predictions.Total Predicted\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"plant_id\"},\"Name\":\"Backtest Predictions.plant_id\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Total Actual\"},\"Name\":\"Backtest Predictions.Total Actual\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Total Predicted\"},\"Name\":\"Backtest Predictions.Total Predicted\"}]},\"drillFilterOtherVisuals\":true,\"objects\":{\"dataPoint\":[{\"properties\":{\"fill\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":-0.5}}}}}},\"selector\":{\"metadata\":\"Backtest Predictions.Total Predicted\"}},{\"properties\":{\"fill\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}},\"selector\":{\"metadata\":\"Backtest Predictions.Total Actual\"}}]}}}",
          "filters": "[]",
          "height": 314.31,
          "width": 659.67,
          "x": 12.42,
          "y": 437.71,
          "z": 1000.00
        },
        {
          "config": "{\"name\":\"summary_card_actual\",\"layouts\":[{\"id\":0,\"position\":{\"x\":289.4783040977203,\"y\":75.27988069029188,\"z\":110,\"width\":259.9872168169874,\"height\":90.02542433065832}}],\"singleVisual\":{\"visualType\":\"card\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.Total Actual\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Total Actual\"},\"Name\":\"Backtest Predictions.Total Actual\"}]},\"drillFilterOtherVisuals\":true,\"objects\":{\"categoryLabels\":[{\"properties\":{\"color\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}}}}]}}}",
          "filters": "[]",
          "height": 90.03,
          "width": 259.99,
          "x": 289.48,
          "y": 75.28,
          "z": 110.00
        },
        {
          "config": "{\"name\":\"summary_card_avg_abs\",\"layouts\":[{\"id\":0,\"position\":{\"x\":838.9438250124281,\"y\":75.27988069029188,\"z\":130,\"width\":252.99938129394218,\"height\":90.02232000568141}}],\"singleVisual\":{\"visualType\":\"card\",\"projections\":{\"Values\":[{\"queryRef\":\"Avg.abs_error\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Aggregation\":{\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"abs_error\"}},\"Function\":1},\"Name\":\"Avg.abs_error\"}]},\"drillFilterOtherVisuals\":true,\"objects\":{\"categoryLabels\":[{\"properties\":{\"color\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}}}}]}}}",
          "filters": "[]",
          "height": 90.02,
          "width": 253.00,
          "x": 838.94,
          "y": 75.28,
          "z": 130.00
        },
        {
          "config": "{\"name\":\"summary_card_avg_pct\",\"layouts\":[{\"id\":0,\"position\":{\"x\":1102.8114480505646,\"y\":75.27988069029188,\"z\":140,\"width\":253.0024856189191,\"height\":90.02542433065832}}],\"singleVisual\":{\"visualType\":\"card\",\"projections\":{\"Values\":[{\"queryRef\":\"Avg.pct_error\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Aggregation\":{\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"pct_error\"}},\"Function\":1},\"Name\":\"Avg.pct_error\"}]},\"drillFilterOtherVisuals\":true,\"objects\":{\"categoryLabels\":[{\"properties\":{\"color\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}}}}]}}}",
          "filters": "[]",
          "height": 90.03,
          "width": 253.00,
          "x": 1102.81,
          "y": 75.28,
          "z": 140.00
        },
        {
          "config": "{\"name\":\"summary_card_mape\",\"layouts\":[{\"id\":0,\"position\":{\"x\":12.41729990767701,\"y\":75.27988069029188,\"z\":100,\"width\":265.4197855265961,\"height\":90.02542433065832}}],\"singleVisual\":{\"visualType\":\"card\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.Backtest MAPE %\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Backtest MAPE %\"},\"Name\":\"Backtest Predictions.Backtest MAPE %\"}]},\"drillFilterOtherVisuals\":true,\"objects\":{\"categoryLabels\":[{\"properties\":{\"color\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}}}}]}}}",
          "filters": "[]",
          "height": 90.03,
          "width": 265.42,
          "x": 12.42,
          "y": 75.28,
          "z": 100.00
        },
        {
          "config": "{\"name\":\"summary_card_pred\",\"layouts\":[{\"id\":0,\"position\":{\"x\":558.0024146012357,\"y\":75.27988069029188,\"z\":120,\"width\":270.0747208294865,\"height\":90.02232000568141}}],\"singleVisual\":{\"visualType\":\"card\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.Total Predicted\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Total Predicted\"},\"Name\":\"Backtest Predictions.Total Predicted\"}]},\"drillFilterOtherVisuals\":true,\"objects\":{\"categoryLabels\":[{\"properties\":{\"color\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}}}}]}}}",
          "filters": "[]",
          "height": 90.02,
          "width": 270.07,
          "x": 558.00,
          "y": 75.28,
          "z": 120.00
        },
        {
          "config": "{\"name\":\"summary_line_actual_vs_pred\",\"layouts\":[{\"id\":0,\"position\":{\"x\":12.41729990767701,\"y\":177.72260492862722,\"z\":500,\"width\":826.526525104751,\"height\":249.12207939777005}}],\"singleVisual\":{\"visualType\":\"lineChart\",\"projections\":{\"Category\":[{\"queryRef\":\"Backtest Predictions.period\",\"active\":true}],\"Y\":[{\"queryRef\":\"Backtest Predictions.Total Actual\"},{\"queryRef\":\"Backtest Predictions.Total Predicted\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"period\"},\"Name\":\"Backtest Predictions.period\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Total Actual\"},\"Name\":\"Backtest Predictions.Total Actual\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"Total Predicted\"},\"Name\":\"Backtest Predictions.Total Predicted\"}],\"OrderBy\":[{\"Direction\":1,\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"period\"}}}]},\"drillFilterOtherVisuals\":true,\"objects\":{\"dataPoint\":[{\"properties\":{\"fill\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}},\"selector\":{\"metadata\":\"Backtest Predictions.Total Actual\"}},{\"properties\":{\"fill\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":-0.5}}}}}},\"selector\":{\"metadata\":\"Backtest Predictions.Total Predicted\"}}]}}}",
          "filters": "[]",
          "height": 249.12,
          "width": 826.53,
          "x": 12.42,
          "y": 177.72,
          "z": 500.00
        },
        {
          "config": "{\"name\":\"summary_slicer_model\",\"layouts\":[{\"id\":0,\"position\":{\"x\":0,\"y\":0,\"z\":0,\"width\":523.078758610894,\"height\":67.51906824799374}}],\"singleVisual\":{\"visualType\":\"slicer\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.model_type\",\"active\":true}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"model_type\"},\"Name\":\"Backtest Predictions.model_type\"}]},\"syncGroup\":{\"groupName\":\"model_type\",\"fieldChanges\":true,\"filterChanges\":true},\"drillFilterOtherVisuals\":true,\"objects\":{\"data\":[{\"properties\":{\"mode\":{\"expr\":{\"Literal\":{\"Value\":\"'Basic'\"}}}}}],\"items\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}},\"background\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}}}],\"header\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}}}}],\"general\":[{\"properties\":{\"orientation\":{\"expr\":{\"Literal\":{\"Value\":\"1D\"}}}}}]}}}",
          "filters": "[]",
          "height": 67.52,
          "width": 523.08,
          "x": 0.00,
          "y": 0.00,
          "z": 0.00
        },
        {
          "config": "{\"name\":\"summary_slicer_period\",\"layouts\":[{\"id\":0,\"position\":{\"x\":1084.9615794332788,\"y\":0,\"z\":30,\"width\":280.9414104111924,\"height\":67.51906824799374}}],\"singleVisual\":{\"visualType\":\"slicer\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.period\",\"active\":true}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"period\"},\"Name\":\"Backtest Predictions.period\"}]},\"syncGroup\":{\"groupName\":\"period\",\"fieldChanges\":true,\"filterChanges\":true},\"drillFilterOtherVisuals\":true,\"objects\":{\"data\":[{\"properties\":{\"mode\":{\"expr\":{\"Literal\":{\"Value\":\"'Dropdown'\"}}}}}],\"items\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}},\"background\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}}}],\"header\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}}}}],\"general\":[{\"properties\":{\"orientation\":{\"expr\":{\"Literal\":{\"Value\":\"1D\"}}}}}]}}}",
          "filters": "[]",
          "height": 67.52,
          "width": 280.94,
          "x": 1084.96,
          "y": 0.00,
          "z": 30.00
        },
        {
          "config": "{\"name\":\"summary_slicer_plant\",\"layouts\":[{\"id\":0,\"position\":{\"x\":523.078758610894,\"y\":0,\"z\":10,\"width\":280.9414104111924,\"height\":67.51906824799374}}],\"singleVisual\":{\"visualType\":\"slicer\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.plant_id\",\"active\":true}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"plant_id\"},\"Name\":\"Backtest Predictions.plant_id\"}]},\"syncGroup\":{\"groupName\":\"plant_id\",\"fieldChanges\":true,\"filterChanges\":true},\"drillFilterOtherVisuals\":true,\"objects\":{\"data\":[{\"properties\":{\"mode\":{\"expr\":{\"Literal\":{\"Value\":\"'Dropdown'\"}}}}}],\"items\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}},\"background\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}}}],\"header\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}}}}],\"general\":[{\"properties\":{\"orientation\":{\"expr\":{\"Literal\":{\"Value\":\"1D\"}}}}}]}}}",
          "filters": "[]",
          "height": 67.52,
          "width": 280.94,
          "x": 523.08,
          "y": 0.00,
          "z": 10.00
        },
        {
          "config": "{\"name\":\"summary_slicer_sku\",\"layouts\":[{\"id\":0,\"position\":{\"x\":804.0201690220865,\"y\":0,\"z\":20,\"width\":280.9414104111924,\"height\":67.51906824799374}}],\"singleVisual\":{\"visualType\":\"slicer\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.sku_id\",\"active\":true}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"sku_id\"},\"Name\":\"Backtest Predictions.sku_id\"}]},\"syncGroup\":{\"groupName\":\"sku_id\",\"fieldChanges\":true,\"filterChanges\":true},\"drillFilterOtherVisuals\":true,\"objects\":{\"data\":[{\"properties\":{\"mode\":{\"expr\":{\"Literal\":{\"Value\":\"'Dropdown'\"}}}}}],\"items\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}},\"bold\":{\"expr\":{\"Literal\":{\"Value\":\"true\"}}},\"background\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":7,\"Percent\":0.6}}}}}}}],\"header\":[{\"properties\":{\"fontColor\":{\"solid\":{\"color\":{\"expr\":{\"ThemeDataColor\":{\"ColorId\":1,\"Percent\":0}}}}}}}],\"general\":[{\"properties\":{\"orientation\":{\"expr\":{\"Literal\":{\"Value\":\"1D\"}}}}}]}}}",
          "filters": "[]",
          "height": 67.52,
          "width": 280.94,
          "x": 804.02,
          "y": 0.00,
          "z": 20.00
        },
        {
          "config": "{\"name\":\"summary_table_top\",\"layouts\":[{\"id\":0,\"position\":{\"x\":689.9362261203039,\"y\":437.70982174561465,\"z\":1010,\"width\":665.8777075491797,\"height\":314.31290391307436}}],\"singleVisual\":{\"visualType\":\"tableEx\",\"projections\":{\"Values\":[{\"queryRef\":\"Backtest Predictions.model_type\"},{\"queryRef\":\"Backtest Predictions.plant_id\"},{\"queryRef\":\"Backtest Predictions.sku_id\"},{\"queryRef\":\"Backtest Predictions.actual\"},{\"queryRef\":\"Backtest Predictions.predicted\"},{\"queryRef\":\"Backtest Predictions.abs_error\"},{\"queryRef\":\"Backtest Predictions.pct_error\"}]},\"prototypeQuery\":{\"Version\":2,\"From\":[{\"Name\":\"b\",\"Entity\":\"Backtest Predictions\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"model_type\"},\"Name\":\"Backtest Predictions.model_type\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"plant_id\"},\"Name\":\"Backtest Predictions.plant_id\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"sku_id\"},\"Name\":\"Backtest Predictions.sku_id\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"actual\"},\"Name\":\"Backtest Predictions.actual\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"predicted\"},\"Name\":\"Backtest Predictions.predicted\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"abs_error\"},\"Name\":\"Backtest Predictions.abs_error\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"pct_error\"},\"Name\":\"Backtest Predictions.pct_error\"}],\"OrderBy\":[{\"Direction\":2,\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"b\"}},\"Property\":\"abs_error\"}}}]},\"drillFilterOtherVisuals\":true,\"objects\":{},\"vcObjects\":{\"stylePreset\":[{\"properties\":{\"name\":{\"expr\":{\"Literal\":{\"Value\":\"'Sparse'\"}}}}}]}}}",
          "filters": "[]",
          "height": 314.31,
          "width": 665.88,
          "x": 689.94,
          "y": 437.71,
          "z": 1010.00
        }
      ],
      "width": 1366.00
    }
  ]
}
"""

THEME_JSON_TEXT = r"""{
  "name": "CY25SU11",
  "dataColors": [
    "#118DFF",
    "#12239E",
    "#E66C37",
    "#6B007B",
    "#E044A7",
    "#744EC2",
    "#D9B300",
    "#D64550",
    "#197278",
    "#1AAB40",
    "#15C6F4",
    "#4092FF",
    "#FFA058",
    "#BE5DC9",
    "#F472D0",
    "#B5A1FF",
    "#C4A200",
    "#FF8080",
    "#00DBBC",
    "#5BD667",
    "#0091D5",
    "#4668C5",
    "#FF6300",
    "#99008A",
    "#EC008C",
    "#533285",
    "#99700A",
    "#FF4141",
    "#1F9A85",
    "#25891C",
    "#0057A2",
    "#002050",
    "#C94F0F",
    "#450F54",
    "#B60064",
    "#34124F",
    "#6A5A29",
    "#1AAB40",
    "#BA141A",
    "#0C3D37",
    "#0B511F"
  ],
  "foreground": "#252423",
  "foregroundNeutralSecondary": "#605E5C",
  "foregroundNeutralTertiary": "#B3B0AD",
  "background": "#FFFFFF",
  "backgroundLight": "#F3F2F1",
  "backgroundNeutral": "#C8C6C4",
  "tableAccent": "#118DFF",
  "good": "#1AAB40",
  "neutral": "#D9B300",
  "bad": "#D64554",
  "maximum": "#118DFF",
  "center": "#D9B300",
  "minimum": "#DEEFFF",
  "null": "#FF7F48",
  "hyperlink": "#0078d4",
  "visitedHyperlink": "#0078d4",
  "textClasses": {
    "callout": {
      "fontSize": 24,
      "fontFace": "DIN",
      "color": "#252423"
    },
    "title": {
      "fontSize": 12,
      "fontFace": "DIN",
      "color": "#252423"
    },
    "header": {
      "fontSize": 12,
      "fontFace": "Segoe UI Semibold",
      "color": "#252423"
    },
    "label": {
      "fontSize": 10,
      "fontFace": "Segoe UI",
      "color": "#252423"
    }
  },
  "visualStyles": {
    "*": {
      "*": {
        "*": [
          {
            "wordWrap": true
          }
        ],
        "line": [
          {
            "transparency": 0
          }
        ],
        "outline": [
          {
            "transparency": 0
          }
        ],
        "plotArea": [
          {
            "transparency": 0
          }
        ],
        "categoryAxis": [
          {
            "showAxisTitle": true,
            "gridlineStyle": "dotted",
            "concatenateLabels": false
          }
        ],
        "valueAxis": [
          {
            "showAxisTitle": true,
            "gridlineStyle": "dotted"
          }
        ],
        "y2Axis": [
          {
            "show": true
          }
        ],
        "title": [
          {
            "titleWrap": true
          }
        ],
        "lineStyles": [
          {
            "strokeWidth": 3
          }
        ],
        "wordWrap": [
          {
            "show": true
          }
        ],
        "background": [
          {
            "show": true,
            "transparency": 0
          }
        ],
        "border": [
          {
            "width": 1
          }
        ],
        "outspacePane": [
          {
            "backgroundColor": {
              "solid": {
                "color": "#ffffff"
              }
            },
            "transparency": 0,
            "border": true,
            "borderColor": {
              "solid": {
                "color": "#B3B0AD"
              }
            }
          }
        ],
        "filterCard": [
          {
            "$id": "Applied",
            "transparency": 0,
            "foregroundColor": {
              "solid": {
                "color": "#252423"
              }
            },
            "border": true
          },
          {
            "$id": "Available",
            "transparency": 0,
            "foregroundColor": {
              "solid": {
                "color": "#252423"
              }
            },
            "border": true
          }
        ]
      }
    },
    "scatterChart": {
      "*": {
        "bubbles": [
          {
            "bubbleSize": -10,
            "markerRangeType": "auto"
          }
        ],
        "general": [
          {
            "responsive": true
          }
        ],
        "fillPoint": [
          {
            "show": true
          }
        ],
        "legend": [
          {
            "showGradientLegend": true
          }
        ]
      }
    },
    "lineChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ],
        "forecast": [
          {
            "matchSeriesInterpolation": true
          }
        ]
      }
    },
    "map": {
      "*": {
        "bubbles": [
          {
            "bubbleSize": -10,
            "markerRangeType": "auto"
          }
        ]
      }
    },
    "azureMap": {
      "*": {
        "bubbleLayer": [
          {
            "bubbleRadius": 8,
            "minBubbleRadius": 8,
            "maxRadius": 40
          }
        ],
        "barChart": [
          {
            "barHeight": 3,
            "thickness": 3
          }
        ]
      }
    },
    "pieChart": {
      "*": {
        "legend": [
          {
            "show": true,
            "position": "RightCenter"
          }
        ],
        "labels": [
          {
            "labelStyle": "Data value, percent of total"
          }
        ]
      }
    },
    "donutChart": {
      "*": {
        "legend": [
          {
            "show": true,
            "position": "RightCenter"
          }
        ],
        "labels": [
          {
            "labelStyle": "Data value, percent of total"
          }
        ]
      }
    },
    "tableEx": {
      "*": {
        "columnHeaders": [
          {
            "columnAdjustment": "growToFit"
          }
        ]
      }
    },
    "pivotTable": {
      "*": {
        "rowHeaders": [
          {
            "showExpandCollapseButtons": true,
            "legacyStyleDisabled": true
          }
        ]
      }
    },
    "multiRowCard": {
      "*": {
        "card": [
          {
            "outlineWeight": 2,
            "barShow": true,
            "barWeight": 2
          }
        ]
      }
    },
    "kpi": {
      "*": {
        "trendline": [
          {
            "transparency": 20
          }
        ]
      }
    },
    "cardVisual": {
      "*": {
        "layout": [
          {
            "maxTiles": 3
          },
          {
            "$id": "default",
            "cellPadding": 12,
            "paddingIndividual": false,
            "paddingUniform": 12,
            "backgroundShow": true
          }
        ],
        "overflow": [
          {
            "type": 0
          }
        ],
        "image": [
          {
            "$id": "default",
            "position": "Left",
            "imageAreaSize": 20,
            "padding": 12,
            "rectangleRoundedCurve": 4,
            "fit": "Normal",
            "fixedSize": false
          }
        ],
        "referenceLabel": [
          {
            "$id": "default",
            "backgroundColor": {
              "solid": {
                "color": "backgroundLight"
              }
            },
            "paddingUniform": 12,
            "rectangleRoundedCurveCustomStyle": true,
            "rectangleRoundedCurveLeftBottom": 4,
            "rectangleRoundedCurveRightBottom": 4
          }
        ],
        "referenceLabelDetail": [
          {
            "$id": "default",
            "detailBackgroundColor": {
              "solid": {
                "color": "foreground"
              }
            },
            "detailFontColor": {
              "solid": {
                "color": "background"
              }
            }
          }
        ],
        "referenceLabelTitle": [
          {
            "$id": "default",
            "titleFontColor": {
              "solid": {
                "color": "foregroundNeutralSecondary"
              }
            }
          }
        ],
        "referenceLabelValue": [
          {
            "$id": "default",
            "valueFontFamily": "'Segoe UI Semibold', wf_segoe-ui_semibold, helvetica, arial, sans-serif",
            "fontColor": {
              "solid": {
                "color": "foreground"
              }
            }
          }
        ],
        "value": [
          {
            "$id": "default",
            "fontFamily": "'Segoe UI Semibold', wf_segoe-ui_semibold, helvetica, arial, sans-serif"
          }
        ],
        "label": [
          {
            "$id": "default",
            "position": "belowValue",
            "fontColor": {
              "solid": {
                "color": "foregroundNeutralSecondary"
              }
            }
          }
        ],
        "spacing": [
          {
            "$id": "default",
            "verticalSpacing": 2
          }
        ],
        "outline": [
          {
            "$id": "default",
            "lineColor": {
              "solid": {
                "color": "backgroundLight"
              }
            }
          }
        ],
        "divider": [
          {
            "$id": "default",
            "dividerColor": {
              "solid": {
                "color": "backgroundLight"
              }
            }
          }
        ],
        "shapeCustomRectangle": [
          {
            "$id": "default",
            "tileShape": "rectangleRoundedByPixel",
            "rectangleRoundedCurve": 4
          }
        ],
        "smallMultiplesOuterShape": [
          {
            "$id": "default",
            "rectangleRoundedCurveCustomStyle": false,
            "rectangleRoundedCurve": 4
          }
        ],
        "smallMultiplesHeader": [
          {
            "$id": "default",
            "paddingIndividual": false,
            "paddingUniform": 12,
            "backgroundColor": {
              "solid": {
                "color": "backgroundLight"
              }
            }
          }
        ],
        "smallMultiplesLayout": [
          {
            "style": "Table",
            "cellPadding": 12,
            "rowCount": 3,
            "orientation": 1,
            "titleOrientation": 1,
            "headerPosition": "Top"
          }
        ],
        "smallMultiplesGrid": [
          {
            "gridlineColor": {
              "solid": {
                "color": "foregroundNeutralTertiary"
              }
            }
          }
        ],
        "smallMultiplesBorder": [
          {
            "$id": "default",
            "gridlineColor": {
              "solid": {
                "color": "foregroundNeutralTertiary"
              }
            }
          }
        ],
        "padding": [
          {
            "$id": "default",
            "paddingUniform": 12,
            "paddingIndividual": false
          }
        ]
      }
    },
    "advancedSlicerVisual": {
      "*": {
        "layout": [
          {
            "maxTiles": 3
          }
        ],
        "shapeCustomRectangle": [
          {
            "$id": "default",
            "tileShape": "rectangleRoundedByPixel",
            "rectangleRoundedCurve": 4
          }
        ],
        "selectionIcon": [
          {
            "$id": "default",
            "size": 12
          }
        ],
        "value": [
          {
            "$id": "default",
            "fontFamily": "'''Segoe UI Semibold'', wf_segoe-ui_semibold, helvetica, arial, sans-serif'"
          }
        ]
      }
    },
    "slicer": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "date": [
          {
            "hideDatePickerButton": false
          }
        ],
        "items": [
          {
            "padding": 4,
            "accessibilityContrastProperties": true
          }
        ]
      }
    },
    "waterfallChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ]
      }
    },
    "columnChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "legend": [
          {
            "showGradientLegend": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "clusteredColumnChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "legend": [
          {
            "showGradientLegend": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "hundredPercentStackedColumnChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "legend": [
          {
            "showGradientLegend": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "barChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "legend": [
          {
            "showGradientLegend": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "clusteredBarChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "legend": [
          {
            "showGradientLegend": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "hundredPercentStackedBarChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "legend": [
          {
            "showGradientLegend": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "areaChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "stackedAreaChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "lineClusteredColumnComboChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "lineStackedColumnComboChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "ribbonChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ],
        "valueAxis": [
          {
            "show": true
          }
        ]
      }
    },
    "hundredPercentStackedAreaChart": {
      "*": {
        "general": [
          {
            "responsive": true
          }
        ],
        "smallMultiplesLayout": [
          {
            "backgroundTransparency": 0,
            "gridLineType": "inner"
          }
        ]
      }
    },
    "group": {
      "*": {
        "background": [
          {
            "show": false
          }
        ]
      }
    },
    "basicShape": {
      "*": {
        "background": [
          {
            "show": false
          }
        ],
        "general": [
          {
            "keepLayerOrder": true
          }
        ],
        "visualHeader": [
          {
            "show": false
          }
        ]
      }
    },
    "shape": {
      "*": {
        "background": [
          {
            "show": false
          }
        ],
        "general": [
          {
            "keepLayerOrder": true
          }
        ],
        "visualHeader": [
          {
            "show": false
          }
        ]
      }
    },
    "image": {
      "*": {
        "background": [
          {
            "show": false
          }
        ],
        "general": [
          {
            "keepLayerOrder": true
          }
        ],
        "visualHeader": [
          {
            "show": false
          }
        ],
        "padding": [
          {
            "left": 0,
            "top": 0,
            "right": 0,
            "bottom": 0
          }
        ]
      }
    },
    "actionButton": {
      "*": {
        "background": [
          {
            "show": false
          }
        ],
        "visualHeader": [
          {
            "show": false
          }
        ]
      }
    },
    "pageNavigator": {
      "*": {
        "background": [
          {
            "show": false
          }
        ],
        "visualHeader": [
          {
            "show": false
          }
        ]
      }
    },
    "bookmarkNavigator": {
      "*": {
        "background": [
          {
            "show": false
          }
        ],
        "visualHeader": [
          {
            "show": false
          }
        ]
      }
    },
    "textbox": {
      "*": {
        "general": [
          {
            "keepLayerOrder": true
          }
        ],
        "visualHeader": [
          {
            "show": false
          }
        ]
      }
    },
    "page": {
      "*": {
        "outspace": [
          {
            "color": {
              "solid": {
                "color": "#FFFFFF"
              }
            }
          }
        ],
        "background": [
          {
            "transparency": 100
          }
        ]
      }
    }
  }
}
"""

PLATFORM_JSON_TEXT = r"""{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  "metadata": {
    "type": "Report",
    "displayName": "IBP Backtest - Actual vs Predicted",
    "description": "Generated PBIR-Legacy report for IBP backtest analysis."
  },
  "config": {
    "version": "2.0",
    "logicalId": "00000000-0000-0000-0000-000000000000"
  }
}
"""

DEFINITION_PBIR_TEMPLATE_TEXT = r"""{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
  "version": "4.0",
  "datasetReference": {
    "byConnection": {
      "connectionString": "Data Source=\"powerbi://api.powerbi.com/v1.0/myorg/Aura Bot\";initial catalog=\"IBP Forecast Model\";integrated security=ClaimsToken;semanticmodelid=e5f9030e-3901-408c-9d6c-47a97112dfa1"
    }
  }
}
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def b64_text(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def b64_json(obj: dict) -> str:
    return b64_text(json.dumps(obj, indent=2))


def wait_for_lro(resp: requests.Response, headers: dict):
    if resp.status_code not in (200, 201, 202):
        raise Exception(f"Request failed: {resp.status_code} -- {resp.text}")

    if resp.status_code in (200, 201):
        try:
            return resp.json()
        except Exception:
            return None

    op_id = resp.headers.get("x-ms-operation-id")
    location = resp.headers.get("Location")
    retry_after = int(resp.headers.get("Retry-After", "5"))

    for _ in range(40):
        time.sleep(retry_after)

        if op_id:
            poll = requests.get(
                f"https://api.fabric.microsoft.com/v1/operations/{op_id}",
                headers=headers,
            )
            if poll.status_code == 200:
                body = poll.json()
                status = body.get("status")
                logger.info("LRO status: %s", status)
                if status in ("Succeeded", "Completed"):
                    result = requests.get(
                        f"https://api.fabric.microsoft.com/v1/operations/{op_id}/result",
                        headers=headers,
                    )
                    if result.status_code == 200:
                        try:
                            return result.json()
                        except Exception:
                            return None
                    return None
                if status == "Failed":
                    raise Exception(f"LRO failed: {poll.text}")

        elif location:
            poll = requests.get(location, headers=headers)
            if poll.status_code == 200:
                body = poll.json()
                status = body.get("status")
                logger.info("LRO status: %s", status)
                if status in ("Succeeded", "Completed"):
                    return body
                if status == "Failed":
                    raise Exception(f"LRO failed: {poll.text}")

    raise Exception("LRO timed out")


# ---------------------------------------------------------------------------
# Resolve current workspace and semantic model
# ---------------------------------------------------------------------------
ws_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}",
    headers=headers,
)
ws_resp.raise_for_status()
workspace_name = ws_resp.json()["displayName"]
logger.info("Workspace: %s (%s)", workspace_name, workspace_id)

sm_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=SemanticModel",
    headers=headers,
)
sm_resp.raise_for_status()
sm_match = [m for m in sm_resp.json().get("value", []) if m["displayName"] == SEMANTIC_MODEL_NAME]
if not sm_match:
    raise Exception(f"Semantic model '{SEMANTIC_MODEL_NAME}' not found. Run notebook 15 first.")
semantic_model_id = sm_match[0]["id"]
logger.info("Semantic model: %s", semantic_model_id)

# ---------------------------------------------------------------------------
# Resolve optional reports folder
# ---------------------------------------------------------------------------
reports_folder_id = None
folders_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders",
    headers=headers,
)
if folders_resp.status_code == 200:
    for f in folders_resp.json().get("value", []):
        if f["displayName"] == cfg("reports_folder"):
            reports_folder_id = f["id"]
            break
logger.info("Reports folder: %s", reports_folder_id or "workspace root")

# ---------------------------------------------------------------------------
# Patch extracted files for current environment
# ---------------------------------------------------------------------------
report_json = json.loads(REPORT_JSON_TEXT)

theme_json = json.loads(THEME_JSON_TEXT)

platform_json = json.loads(PLATFORM_JSON_TEXT)
platform_json["metadata"]["displayName"] = REPORT_NAME
platform_json["metadata"]["description"] = REPORT_DESCRIPTION

definition_pbir = json.loads(DEFINITION_PBIR_TEMPLATE_TEXT)
definition_pbir["datasetReference"]["byConnection"]["connectionString"] = (
    f'Data Source="powerbi://api.powerbi.com/v1.0/myorg/{workspace_name}";'
    f'initial catalog="{SEMANTIC_MODEL_NAME}";'
    f'integrated security=ClaimsToken;'
    f'semanticmodelid={semantic_model_id}'
)

# ---------------------------------------------------------------------------
# Build report definition payload from decoded source files
# ---------------------------------------------------------------------------
definition = {
    "parts": [
        {
            "path": "definition.pbir",
            "payload": b64_json(definition_pbir),
            "payloadType": "InlineBase64",
        },
        {
            "path": "StaticResources/SharedResources/BaseThemes/CY25SU11.json",
            "payload": b64_json(theme_json),
            "payloadType": "InlineBase64",
        },
        {
            "path": "report.json",
            "payload": b64_json(report_json),
            "payloadType": "InlineBase64",
        },
        {
            "path": ".platform",
            "payload": b64_json(platform_json),
            "payloadType": "InlineBase64",
        },
    ]
}

# ---------------------------------------------------------------------------
# Create or update report
# ---------------------------------------------------------------------------
existing_reports = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Report",
    headers=headers,
)
existing_reports.raise_for_status()
existing = [r for r in existing_reports.json().get("value", []) if r["displayName"] == REPORT_NAME]

report_id = None

if existing:
    report_id = existing[0]["id"]
    logger.info("Report already exists (%s) — skipping definition update to preserve manual edits.", report_id)
else:
    logger.info("Creating new report...")
    body = {
        "displayName": REPORT_NAME,
        "description": REPORT_DESCRIPTION,
        "definition": definition,
    }
    if reports_folder_id:
        body["folderId"] = reports_folder_id

    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports",
        headers=headers,
        json=body,
    )

    result = wait_for_lro(resp, headers)
    if result and isinstance(result, dict):
        report_id = result.get("id")

    if not report_id:
        time.sleep(3)
        verify = requests.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Report",
            headers=headers,
        )
        verify.raise_for_status()
        match = [r for r in verify.json().get("value", []) if r["displayName"] == REPORT_NAME]
        if match:
            report_id = match[0]["id"]

    logger.info("Created: %s", report_id)

if not report_id:
    raise Exception("Report ID could not be resolved after create/update.")

logger.info("URL: https://app.fabric.microsoft.com/groups/%s/reports/%s", workspace_id, report_id)
logger.info("Complete.")
