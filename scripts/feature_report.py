# scripts/feature_report.py

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

def load_schema(schema_path: Path) -> Dict[str, Dict[str, str]]:
    """
    Load schema_v1.json and return mapping:
      feature_name -> {"type": <type>, "section": <section>}
    """
    if not schema_path.exists():
        alt = Path("schema_v1.json")
        if alt.exists():
            schema_path = alt
        else:
            raise FileNotFoundError(f"Schema file not found at {schema_path} or {alt}")

    with schema_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    features_meta: Dict[str, Dict[str, str]] = {}
    for section, arr in raw.get("features", {}).items():
        for fdef in arr:
            name = fdef["name"]
            ftype = fdef.get("type", "float")
            features_meta[name] = {"type": ftype, "section": section}
    return features_meta

def iter_jsonl_paths(inputs: List[str]) -> List[Path]:
    paths: List[Path] = []
    for item in inputs:
        input_path = Path(item)
        # glob pattern
        if any(ch in item for ch in "*?[]"):
            for g in Path().glob(item):
                if g.is_file():
                    paths.append(g)
        elif input_path.is_dir():
            paths.extend(sorted(input_path.glob("*.jsonl")))
        elif input_path.is_file():
            paths.append(input_path)
        else:
            print(f"Warning: {item} not found.")
    return paths

def init_stats(features_meta: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    stats: Dict[str, Dict[str, Any]] = {}
    for name, meta in features_meta.items():
        ftype = meta["type"]
        stats[name] = {
            "type": ftype,
            "section": meta["section"],
            "present": 0,
            "non_null": 0,
            "constant": True,
            "first_value": None,
            "non_zero": 0,
            "min": None,
            "max": None,
            "distinct": set() if ftype in ("bool", "string") else None,
        }
    return stats

def update_numeric_stat(feature_stat: Dict[str, Any], value: Any) -> None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return
    if feature_stat["first_value"] is None:
        feature_stat["first_value"] = numeric_value
    elif numeric_value != feature_stat["first_value"]:
        feature_stat["constant"] = False
    if numeric_value != 0.0:
        feature_stat["non_zero"] += 1
    if feature_stat["min"] is None or numeric_value < feature_stat["min"]:
        feature_stat["min"] = numeric_value
    if feature_stat["max"] is None or numeric_value > feature_stat["max"]:
        feature_stat["max"] = numeric_value

def update_bool_or_str_stat(feature_stat: Dict[str, Any], value: Any) -> None:
    if value is None:
        return
    normalized_value = bool(value) if feature_stat["type"] == "bool" else str(value)
    if feature_stat["first_value"] is None:
        feature_stat["first_value"] = normalized_value
    elif normalized_value != feature_stat["first_value"]:
        feature_stat["constant"] = False
    if feature_stat["type"] == "bool" and normalized_value:
        feature_stat["non_zero"] += 1
    if feature_stat["distinct"] is not None and len(feature_stat["distinct"]) < 10:
        feature_stat["distinct"].add(normalized_value)

def analyze_files(paths: List[Path], features_meta: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    stats = init_stats(features_meta)
    total_events = 0

    for path in paths:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                feats = obj.get("features", {}) or {}
                total_events += 1

                for name, meta in features_meta.items():
                    feature_stat = stats[name]
                    if name not in feats:
                        continue
                    value = feats[name]
                    feature_stat["present"] += 1
                    if value is None:
                        continue
                    feature_stat["non_null"] += 1
                    if meta["type"] in ("float", "int"):
                        update_numeric_stat(feature_stat, value)
                    elif meta["type"] in ("bool", "string"):
                        update_bool_or_str_stat(feature_stat, value)

    for feature_stat in stats.values():
        feature_stat["total_events"] = total_events
    return stats

def print_report(stats: Dict[str, Dict[str, Any]]) -> None:
    features_sorted = sorted(stats.items(), key=lambda kv: (kv[1]["section"], kv[0]))
    current_section = None

    for name, feature_stat in features_sorted:
        if feature_stat["total_events"] == 0:
            continue

        section = feature_stat["section"]
        if section != current_section:
            current_section = section
            print(f"\n=== {section.upper()} ===")
            print(f"{'feature':30} {'type':7} {'present':7} {'non_zero':9} {'varies':7} {'min':10} {'max':10} {'sample':15}")

        present = feature_stat["present"]
        non_zero = feature_stat["non_zero"]
        varies = (not feature_stat["constant"]) and (present > 0)
        ftype = feature_stat["type"]

        min_v = f"{feature_stat['min']:.4g}" if isinstance(feature_stat["min"], (int, float)) and feature_stat["min"] is not None else "-"
        max_v = f"{feature_stat['max']:.4g}" if isinstance(feature_stat["max"], (int, float)) and feature_stat["max"] is not None else "-"

        if ftype in ("bool", "string"):
            if feature_stat["distinct"]:
                sample = ",".join(map(str, sorted(feature_stat["distinct"])))[:15]
            else:
                sample = str(feature_stat["first_value"])
        else:
            sample = f"{feature_stat['first_value']:.4g}" if feature_stat["first_value"] is not None else "-"

        print(f"{name:30} {ftype:7} {present:7d} {non_zero:9d} {str(varies):7} {min_v:10} {max_v:10} {sample:15}")

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Feature completeness / variability report for WickEngine JSONL dataset."
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="JSONL file(s) or directory/glob patterns (e.g. data/*.jsonl)",
    )
    parser.add_argument(
        "--schema",
        default="schema/schema_v1.json",
        help="Path to schema_v1.json (fallback: ./schema_v1.json)",
    )
    args = parser.parse_args()

    features_meta = load_schema(Path(args.schema))
    paths = iter_jsonl_paths(args.paths)
    if not paths:
        print("No JSONL files found for given inputs.")
        return

    stats = analyze_files(paths, features_meta)
    print_report(stats)

if __name__ == "__main__":
    main()
