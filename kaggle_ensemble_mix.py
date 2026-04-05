import pandas as pd
import json
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
log = logging.getLogger("ensemble_mixer")

def format_cove_to_sharegpt(row):
    """
    Standardizes the CoVe rows to precisely match our `synthetic_cot.jsonl` keys!
    """
    user_prompt = f"PROMPT:\n{row['prompt']}\n\nPlease generate the logical reasoning trace leading to the answer."
    
    # We must extract the CoVe trace AND manually inject the Kaggle \boxed{} extraction wrapper at the very end
    assistant_response = f"<think>\n{row['cot']}\n</think>\n\\boxed{{{row['answer']}}}"
    
    return {
        "prompt": user_prompt,
        "cot": assistant_response,
        "answer": str(row['answer'])
    }

import re

def format_legacy_sharegpt(row):
    """
    Flattens the Phase 11 `conversations` array back into raw prompt, cot, answer strings
    so both datasets merge with the EXACT same PyArrow schema!
    """
    conv = row.get("conversations", [])
    if len(conv) == 2:
        user_prompt = conv[0].get("value", "")
        assistant_resp = conv[1].get("value", "")
        
        # Regex to yank the boxed answer out of the legacy trace
        ans_match = re.search(r'\\boxed{([^}]+)}', assistant_resp)
        ans = ans_match.group(1).strip() if ans_match else ""
        
        return {
            "prompt": user_prompt,
            "cot": assistant_resp,
            "answer": ans
        }
    return {}

def main():
    # File paths
    legacy_file = "synthetic_cot.jsonl" # Phase 11 (337 traces)
    cove_file = "data/tot_react_cove_dataset.jsonl" # The 0.61 Grandmaster dataset
    out_file = "master_ensemble_dataset.jsonl"

    log.info("Loading baseline Phase 11 dataset...")
    try:
        baseline_dfRaw = pd.read_json(legacy_file, lines=True)
        # Magic Fix: Flatten the old ShareGPT format out of the legacy dataset!
        legacy_formatted_list = baseline_dfRaw.apply(format_legacy_sharegpt, axis=1).tolist()
        baseline_df = pd.DataFrame([x for x in legacy_formatted_list if x])
        log.info(f"Loaded and flattened {len(baseline_df)} traces from {legacy_file}")
    except Exception as e:
        log.warning(f"Could not load {legacy_file}. Ensure it exists in the active directory! ({e})")
        baseline_df = pd.DataFrame()

    log.info("Loading 0.61 CoVe dataset...")
    try:
        cove_df = pd.read_json(cove_file, lines=True)
        log.info(f"Loaded {len(cove_df)} traces from {cove_file}")
    except Exception as e:
        log.error(f"Fatal error loading CoVe dataset: {e}")
        return

    # Standardize the CoVe format to match
    log.info("Formatting CoVe traces into identical schema...")
    cove_formatted_list = cove_df.apply(format_cove_to_sharegpt, axis=1).tolist()
    cove_formatted_df = pd.DataFrame(cove_formatted_list)

    # Concatenate BOTH identical flat datasets
    log.info("Merging datasets...")
    frames = []
    if not baseline_df.empty:
        frames.append(baseline_df)
    frames.append(cove_formatted_df)
    
    master_df = pd.concat(frames, ignore_index=True)
    
    # Shuffle the dataset so Nemotron learns all logic frameworks uniformly!
    log.info("Shuffling the Master Dataset uniformly...")
    master_df = master_df.sample(frac=1, random_state=42).reset_index(drop=True)

    # Save out to JSONL
    with open(out_file, "w", encoding="utf-8") as f:
        for record in master_df.to_dict(orient="records"):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    log.info(f"✨ Schema-Matched Master-Data Mix Complete! Saved {len(master_df)} unified traces to {out_file}.")
    log.info("You can now securely upload this file to Colab and run Unsloth!")

if __name__ == "__main__":
    main()
