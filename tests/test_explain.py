import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from intelligence.explain import (
    build_facts,
    build_prompt,
    deterministic_explanation,
    explain_signal,
    find_ungrounded_numbers,
    validate_explanation,
)
from intelligence.signals import run_pipeline
from test_trends import make_events


def get_signal():
    result = run_pipeline(make_events(), created_at="2026-10-09T00:00:00+00:00")
    assert result["signals"]
    return result["signals"][0]


def test_facts_contain_only_signal_values():
    signal = get_signal()
    facts = build_facts(signal)
    assert signal["topic"] in facts
    assert f"{signal['score']:.0f} out of 100" in facts
    assert "Network: not available" in facts
    assert "not a calibrated probability" in facts
    prompt = build_prompt(facts)
    assert "ONLY the facts" in prompt and facts in prompt
    print("facts ok")


def test_validation():
    facts = "Trend: 6 mentions before and 24 mentions now, growth +300%\nConfidence proxy: 72%"
    assert validate_explanation("Mentions rose from 6 to 24, up 300%.", facts) == (True, "ok")
    assert validate_explanation("Mentions rose by 450%.", facts) == (False, "ungrounded_numbers")
    assert validate_explanation("", facts) == (False, "empty")
    assert validate_explanation("x" * 1000, facts) == (False, "too_long")
    assert find_ungrounded_numbers("confidence 72 percent and 99 users", facts) == [99.0]
    print("validation ok")


def test_explain_paths():
    signal = get_signal()
    plain = explain_signal(signal, llm=None)
    assert plain["source"] == "evidence_summary" and plain["text"] == deterministic_explanation(signal)

    unavailable = explain_signal(signal, llm=lambda prompt: None)
    assert unavailable["source"] == "evidence_summary" and unavailable["reason"] == "llm_unavailable"

    def raising(prompt):
        raise RuntimeError("boom")

    assert explain_signal(signal, llm=raising)["reason"] == "llm_unavailable"

    hallucinating = explain_signal(signal, llm=lambda prompt: "Mentions grew by 98765% across 77 cities.")
    assert hallucinating["source"] == "evidence_summary" and hallucinating["reason"] == "ungrounded_numbers"

    grounded_text = f"The topic '{signal['topic']}' is a {signal['type']} signal with priority {signal['priority']}."
    good = explain_signal(signal, llm=lambda prompt: grounded_text)
    assert good["source"] == "gemini" and good["text"] == grounded_text
    seen = {}
    explain_signal(signal, llm=lambda prompt: seen.setdefault("prompt", prompt) and grounded_text)
    assert signal["topic"] in seen["prompt"]
    print("explain paths ok")


if __name__ == "__main__":
    test_facts_contain_only_signal_values()
    test_validation()
    test_explain_paths()
    print("all explain tests passed")