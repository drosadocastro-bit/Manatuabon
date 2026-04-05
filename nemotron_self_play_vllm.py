import os
import json
import logging
import pandas as pd
import re
from vllm import LLM, SamplingParams
from vllm.lora.request import LoRARequest

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
log = logging.getLogger("self_play")

def extract_boxed_answer(text: str) -> str:
    """Extracts the math sequence inside \boxed{}"""
    match = re.search(r'\\boxed{([^}]+)}', text)
    if match:
        return match.group(1).strip()
    return None

def format_sharegpt(prompt: str, generated_response: str) -> dict:
    """Format into standard ShareGPT conversational format"""
    return {
        "conversations": [
            {"from": "user", "value": prompt},
            {"from": "assistant", "value": generated_response}
        ]
    }

def main():
    # Configure these paths depending on if you are running locally or on Colab
    csv_path = "train.csv"  # Ensure train.csv is in the working directory
    base_model_path = "nvidia/Nemotron-3-Nano-30B-Base" # Update to local path if downloaded via KaggleHub
    lora_dir = "nemotron_reasoning_lora" # The clean directory containing adapter_config.json and safetensors
    out_file = "self_play_dataset.jsonl"
    
    if not os.path.exists(csv_path):
        log.error(f"{csv_path} not found. Please place training data in the active directory.")
        return
        
    df = pd.read_csv(csv_path)
    
    # DATASET REFINEMENT: Filter for complex puzzles
    complex_df = df[df['prompt'].str.len() > 250]
    
    # Sample 10,000 unseen puzzles to attempt
    SAMPLE_SIZE = 10000
    if len(complex_df) > SAMPLE_SIZE:
        sampled_df = complex_df.sample(SAMPLE_SIZE, random_state=42)
    else:
        sampled_df = complex_df
        
    log.info(f"Loaded {len(sampled_df)} high-complexity puzzles for Rejection Sampling.")
    
    # The System-Baked formatting structural enforcing prompt
    system_prompt = """You are an elite mathematical logician. Reverse-engineer the puzzle below. I will give you the Prompt. Your job is to output the step-by-step thinking process that logically arrives at the precise answer. 
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
    3. The final answer MUST be wrapped in the LaTeX \\boxed{} command."""

    # Compile the prompts
    prompts = []
    ground_truths = []
    
    for idx, row in sampled_df.iterrows():
        # Notice we do NOT feed the answer to the model! It must reason it out itself.
        user_prompt = f"PROMPT:\n{row['prompt']}\n\nPlease generate the logical reasoning trace leading to the answer."
        full_text = f"System: {system_prompt}\n\nUser: {user_prompt}\n\nAssistant: "
        prompts.append(full_text)
        ground_truths.append(str(row["answer"]).strip())
        
    log.info("Loading vLLM Engine. This requires significant VRAM...")
    
    # Initialize vLLM with LoRA enabled
    try:
        llm = LLM(
            model=base_model_path,
            trust_remote_code=True, # Required for Nemotron-3 custom tokenizers and logic
            enable_lora=True,
            max_lora_rank=32,
            max_model_len=8192,
            tensor_parallel_size=1, # Change to 2 or 4 if using multiple GPUs
            gpu_memory_utilization=0.95
        )
    except Exception as e:
        log.error(f"Failed to load vLLM Engine: {e}")
        return

    # Kaggle evaluation parameters
    sampling_params = SamplingParams(
        temperature=0.0, # Greedy deterministic decoding
        top_p=1.0,
        max_tokens=6000
    )
    
    log.info("Executing Batch Inference on all puzzles...")
    
    # Pass the Base Model + LoRA Request dynamically
    outputs = llm.generate(
        prompts,
        sampling_params,
        lora_request=LoRARequest("nemotron_adapter", 1, lora_dir)
    )
    
    valid_count = 0
    with open(out_file, "w", encoding="utf-8") as f:
        for idx, output in enumerate(outputs):
            generated_text = output.outputs[0].text
            ground_truth = ground_truths[idx]
            
            # Outcome Validation
            model_answer = extract_boxed_answer(generated_text)
            
            if model_answer is None:
                log.warning(f"Puzzle {idx}: Model failed to output \\boxed{{}}. Rejecting.")
                continue
                
            # THE REJECTION SAMPLING FILTER
            if model_answer == ground_truth:
                # The model perfectly reasoned through the unseen puzzle! Save the trace.
                dataset_row = format_sharegpt(prompts[idx], generated_text)
                f.write(json.dumps(dataset_row, ensure_ascii=False) + "\n")
                valid_count += 1
            else:
                # The model hallucinated or math was wrong. Discard the toxic trace.
                pass
                
    log.info(f"✨ Self-Play Complete! The model successfully solved {valid_count} unseen puzzles.")
    log.info(f"Saved {valid_count} zero-cost pristine traces to {out_file}.")

if __name__ == "__main__":
    main()
