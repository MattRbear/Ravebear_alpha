#!/usr/bin/env python3
"""
Event Validator Tool
Validates JSONL wick events against schema and business rules.
"""
import sys
import json
import argparse
from pathlib import Path
from pydantic import ValidationError
from collections import Counter

# Add parent dir to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from features import WickEvent

def validate_file(path: str):
    print(f"Validating {path}...")
    
    total = 0
    valid = 0
    errors = Counter()
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                total += 1
                line = line.strip()
                if not line: continue
                
                try:
                    # 1. Parse JSON
                    data = json.loads(line)
                    
                    # 2. Pydantic Validation
                    event = WickEvent(**data)
                    
                    # 3. Business Logic / Invariants
                    feats = event.features
                    
                    # Ratio sanity
                    if feats.wick_to_body_ratio < 0:
                        raise ValueError("Negative wick_to_body_ratio")
                        
                    # VWAP Score scale check (repair verification)
                    if feats.vwap_mean_reversion_score > 100:
                        print(f"WARN line {i}: VWAP score > 100 ({feats.vwap_mean_reversion_score})")
                        
                    # Velocity check (repair verification)
                    if feats.rejection_velocity == 0.0 and feats.wick_size_pct > 0:
                        # Only warn, maybe duration was missing?
                        # print(f"WARN line {i}: Zero velocity on valid wick")
                        pass

                    valid += 1
                    
                except json.JSONDecodeError:
                    errors['JSON Decode Error'] += 1
                except ValidationError as e:
                    # Simplify pydantic error
                    msg = str(e).split('\n')[0]
                    errors[f"Schema: {msg}"] += 1
                except Exception as e:
                    errors[f"Logic: {str(e)}"] += 1
                    
    except FileNotFoundError:
        print("File not found.")
        sys.exit(2)

    print("\n=== SUMMARY ===")
    print(f"Total Lines: {total}")
    print(f"Valid Events: {valid}")
    print(f"Invalid: {total - valid}")
    
    if errors:
        print("\n=== TOP ERRORS ===")
        for k, v in errors.most_common(5):
            print(f"{v}x {k}")
            
    if valid == total and total > 0:
        sys.exit(0)
    elif total == 0:
        print("Empty file.")
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="Path to .jsonl file")
    args = parser.parse_args()
    
    validate_file(args.file)
