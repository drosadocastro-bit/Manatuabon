import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment
base_path = Path(r"D:\Manatuabon")
load_dotenv(base_path / ".env")

from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic

def test_cloud_fallback():
    print("--------------------------------------------------")
    print(" MANATUABON ARCHITECTURE TESTER ")
    print("--------------------------------------------------")
    print("Initializing SQL Database Connection...")
    db_path = base_path / "manatuabon.db"
    db = SQLDatabase.from_uri(f"sqlite:///{db_path.resolve()}")
    
    print(f"Database dialect: {db.dialect}")
    print(f"Tables found: {db.get_usable_table_names()}")
    
    # Cloud LLM setup
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        print("\n[!] WARNING: ANTHROPIC_API_KEY is not set in your .env file!")
        print("Please add it so the Claude 3.5 Sonnet fallback can be tested properly.")
        sys.exit(1)
        
    llm_cloud = ChatAnthropic(
        model="claude-4.6-sonnet", 
        api_key=anthropic_key, 
        temperature=0.4
    )
    
    sql_prompt = """
    You are MANATUABON, an autonomous astrophysics intelligence.
    Analyze the `mast_queue` table and tell me:
    1. How many total items are there?
    2. How many are in each status state (e.g. pending vs done)?
    3. What is the target_name of the most recently queued item?
    """
    
    print("\n[Phase 1] Attempting with Local Agent (Nemotron 30B)...")
    try:
        # Simulate local LLM failure explicitly to force escalation logic evaluation
        print("Executing local prompt...")
        raise Exception("Nemotron model experienced VRAM overflow or syntax parsing failure on complex prompt structure.")
    except Exception as e:
        print(f"[-] Local SQL Agent threw exception: {e}")
        print("\n[Phase 2] [ESCALATING TO CLOUD] Handoff sequence initiated. Calling Claude 3.5 Sonnet...")
        
        try:
            cloud_agent = create_sql_agent(
                llm=llm_cloud,
                db=db,
                agent_type="zero-shot-react-description",
                verbose=False,
                handle_parsing_errors=True
            )
            
            response = cloud_agent.invoke({"input": sql_prompt})
            output = response.get("output", "")
            print(f"\n[+] CLOUD RESPONSE SUCCESSFUL: \n{output}")
        except Exception as cloud_e:
            print(f"\n[-] Cloud Fallback also failed: {repr(cloud_e)}")
            sys.exit(1)

if __name__ == "__main__":
    test_cloud_fallback()
