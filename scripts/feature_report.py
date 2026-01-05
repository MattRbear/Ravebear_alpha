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
        p = Path(item)
        # glob pattern
        if any(ch in item for ch in "*?[]"):
            for g in Path().glob(item):
                if g.is_file():
                    paths.append(g)
        elif p.is_dir():
            paths.extend(sorted(p.glob("*.jsonl")))
        elif p.is_file():
            paths.append(p)
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

def update_numeric_stat(st: Dict[str, Any], value: Any) -> None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return
    if st["first_value"] is None:
        st["first_value"] = v
    elif v != st["first_value"]:
        st["constant"] = False
    if v != 0.0:
        st["non_zero"] += 1
    if st["min"] is None or v < st["min"]:
        st["min"] = v
    if st["max"] is None or v > st["max"]:
        st["max"] = v

def update_bool_or_str_stat(st: Dict[str, Any], value: Any) -> None:
    if value is None:
        return
    v = bool(value) if st["type"] == "bool" else str(value)
    if st["first_value"] is None:
        st["first_value"] = v
    elif v != st["first_value"]:
        st["constant"] = False
    if st["type"] == "bool" and v:
        st["non_zero"] += 1
    if st["distinct"] is not None and len(st["distinct"]) < 10:
        st["distinct"].add(v)

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
                    st = stats[name]
                    if name not in feats:
                        continue
                    value = feats[name]
                    st["present"] += 1
                    if value is None:
                        continue
                    st["non_null"] += 1
                    if meta["type"] in ("float", "int"):
                        update_numeric_stat(st, value)
                    elif meta["type"] in ("bool", "string"):
                        update_bool_or_str_stat(st, value)

    for st in stats.values():
        st["total_events"] = total_events
    return stats

def print_report(stats: Dict[str, Dict[str, Any]]) -> None:
    features_sorted = sorted(stats.items(), key=lambda kv: (kv[1]["section"], kv[0]))
    current_section = None

    for name, st in features_sorted:
        if st["total_events"] == 0:
            continue

        section = st["section"]
        if section != current_section:
            current_section = section
            print(f"\n=== {section.upper()} ===")
            print(f"{'feature':30} {'type':7} {'present':7} {'non_zero':9} {'varies':7} {'min':10} {'max':10} {'sample':15}")

        present = st["present"]
        non_zero = st["non_zero"]
        varies = (not st["constant"]) and (present > 0)
        ftype = st["type"]

        min_v = f"{st['min']:.4g}" if isinstance(st["min"], (int, float)) and st["min"] is not None else "-"
        max_v = f"{st['max']:.4g}" if isinstance(st["max"], (int, float)) and st["max"] is not None else "-"

        if ftype in ("bool", "string"):
            if st["distinct"]:
                sample = ",".join(map(str, sorted(st["distinct"])))[:15]
            else:
                sample = str(st["first_value"])
        else:
            sample = f"{st['first_value']:.4g}" if st["first_value"] is not None else "-"

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
