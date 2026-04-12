"""
Smoke test for Anthropic API connectivity.
Skipped when ANTHROPIC_API_KEY is not set.
"""

import os
import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY not set — skip connectivity smoke test",
)


def test_anthropic_connection():
    from dotenv import load_dotenv
    load_dotenv("D:/Manatuabon/.env")

    from langchain_core.messages import HumanMessage
    from langchain_anthropic import ChatAnthropic

    llm = ChatAnthropic(
        model="claude-sonnet-4-20250514",
        api_key=os.environ["ANTHROPIC_API_KEY"],
        max_tokens=256,
    )
    res = llm.invoke([HumanMessage(content="Hi")])
    assert res.content, "Empty response from Anthropic"
