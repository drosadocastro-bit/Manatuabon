import os
import json
import logging
import pandas as pd
import requests
import re
import concurrent.futures

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
log = logging.getLogger("kaggle_pipeline")

def extract_boxed_answer(text: str) -> str:
    match = re.search(r'\\boxed{([^}]+)}', text)
    if match:
        return match.group(1).strip()
    return None

def generate_single_cot(prompt: str, answer: str, api_key: str) -> str:
    system = """You are an elite mathematical logician. Reverse-engineer the puzzle below. I will give you the Prompt and the Final Answer. Your job is to output the step-by-step thinking process that logically arrives at that precise answer. 
    Strict formatting rules:
    1. Your entire response MUST follow this EXACT wrapping structure:
    
<think>
[GIVEN] Extract the core facts.
[HYPOTHESIS] Identify the pattern or mathematical theorem to use.
[PROOF] Execute the logic step-by-step.
[VERIFICATION] Check the result using an alternative secondary method.
</think>
\\boxed{...}

    2. Avoid unnecessary rambling. Do exactly NO brute-forcing of large combinations; find the conceptual pattern.
    3. The final answer MUST be output identically to the provided CORRECT FINAL ANSWER, wrapped in the LaTeX \\boxed{} command."""
    
    user_prompt = f"PROMPT:\n{prompt}\n\nCORRECT FINAL ANSWER:\n{answer}\n\nPlease generate the logical reasoning trace leading to this answer."
    
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01"
    }
    
    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 8192,
        "system": system,
        "messages": [
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.7  # Higher temperature to generate slightly different reasoning paths for consistency check
    }
    
    resp = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=payload, timeout=300)
    if not resp.ok:
        raise RuntimeError(f"Claude API failed: {resp.text}")
        
    return resp.json()["content"][0]["text"]

def generate_cot_consistency(prompt: str, answer: str, api_key: str) -> str:
    """Generates 3 traces concurrently and enforces absolute Consistency Max."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(generate_single_cot, prompt, answer, api_key) for _ in range(3)]
        
        traces = []
        for future in concurrent.futures.as_completed(futures):
            try:
                traces.append(future.result())
            except Exception as e:
                log.error(f"Generation failed: {e}")
                return None
                
    if len(traces) != 3:
        return None
        
    # Extract boxed answers to ensure 3-way consensus
    answers = [extract_boxed_answer(t) for t in traces]
    
    # Check for empty extractions
    if any(ans is None for ans in answers):
        log.warning("One or more traces failed to output \\boxed{}. Discarding.")
        return None
        
    # Consistency check
    if answers[0] == answers[1] == answers[2]:
        log.info(f"✨ 3-Way CONSENSUS ACHIEVED! Boxed Answer: {answers[0]}")
        # Return the first trace, since they all agree
        return traces[0]
    else:
        log.warning(f"❌ Consensus failed! Traces produced differing answers: {answers}. Discarding.")
        return None

def format_sharegpt(prompt: str, generated_response: str) -> dict:
    return {
        "conversations": [
            {"from": "user", "value": prompt},
            {"from": "assistant", "value": generated_response}
        ]
    }

def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        api_key = input("Enter your Anthropic API Key (sk-ant-...): ").strip()
    
    csv_path = "D:/Manatuabon/train.csv"
    if not os.path.exists(csv_path):
        log.error(f"{csv_path} not found.")
        return
        
    df = pd.read_csv(csv_path)
    
    # DATASET REFINEMENT: Filter for complex puzzles
    # Require prompt length > 300 characters to ensure we only get substantive logic grids and math
    complex_df = df[df['prompt'].str.len() > 300]
    log.info(f"Filtered down to {len(complex_df)} high-complexity puzzles from {len(df)} total.")
    
    # Topping off the Phase 11 generation since 111 traces were discarded for hallucinations.
    # Asking for 175 more so the surviving traces push us right up against ~325 total.
    SAMPLE_SIZE = 175
    sampled_df = complex_df.sample(SAMPLE_SIZE, random_state=42)
    
    out_file = "D:/Manatuabon/synthetic_cot_grandmaster.jsonl"
    
    valid_count = 0
    with open(out_file, "a", encoding="utf-8") as f: # Changed to append mode
        for idx, row in sampled_df.iterrows():
            prompt = row["prompt"]
            answer = str(row["answer"])
            
            log.info(f"Processing ID: {row['id']} via Consistency Max...")
            try:
                response = generate_cot_consistency(prompt, answer, api_key)
                if not response:
                    continue  # Consensus failed or extraction failed
                
                # Double check length and specific tags
                if len(response) > 5000:
                    log.warning(f"Trace too long ({len(response)} chars) - rambling. Discarding.")
                    continue
                if "<think>" not in response or "</think>" not in response:
                    continue
                    
                dataset_row = format_sharegpt(prompt, response)
                f.write(json.dumps(dataset_row, ensure_ascii=False) + "\n")
                f.flush()
                valid_count += 1
                
            except Exception as e:
                log.error(f"Failed on ID {row['id']}: {e}")
                
    log.info(f"✨ Consistency Max Complete! Successfully generated {valid_count} pure traces. Saved to {out_file}")

if __name__ == "__main__":
    main()
