from langchain_core.messages import HumanMessage
from langchain_anthropic import ChatAnthropic
import os
from dotenv import load_dotenv

load_dotenv("D:/Manatuabon/.env")

llm = ChatAnthropic(
    model="claude-3-5-sonnet-20240620",
    api_key=os.environ.get("ANTHROPIC_API_KEY"),
    max_tokens=256
)
try:
    res = llm.invoke([HumanMessage(content="Hi")])
    print("SUCCESS:", res.content)
except Exception as e:
    print("FAIL:", type(e).__name__, str(e))
