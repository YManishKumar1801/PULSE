import re

MAX_EXPLANATION_CHARS = 900
NUMBER_PATTERN = re.compile(r"\d+(?:\.\d+)?")
NUMBER_TOLERANCE = 0.051


def _pct(value):
    return f"{round(value * 100)}%"


def build_facts(signal):
    evidence = signal["evidence"]
    trend = evidence["trend"]
    sentiment = evidence["sentiment"]
    audience = evidence["audience"]
    network = evidence["network"]

    lines = [
        f"Topic: {signal['topic']}",
        f"Topic terms: {', '.join(signal['terms'])}",
        f"Signal type: {signal['type']}",
        f"Priority: {signal['priority']}",
        f"Signal score: {signal['score']:.0f} out of 100",
        f"Confidence proxy: {_pct(signal['confidence_proxy'])} (a heuristic, not a calibrated probability)",
        f"Window: latest {signal['window_hours']:g} hours compared with the {signal['window_hours']:g} hours before",
    ]

    if trend["growth_pct"] is None:
        lines.append(f"Trend: new topic, 0 mentions before and {trend['current']} mentions now")
    else:
        lines.append(
            f"Trend: {trend['previous']} mentions before and {trend['current']} mentions now, "
            f"growth {trend['growth_pct']:+.0f}%, velocity {trend['velocity']} ({trend['velocity_per_hour']:+.2f} messages per hour)"
        )

    if sentiment["available"]:
        text = f"Sentiment: overall {sentiment['label']}, {_pct(sentiment['negative_share'])} of analyzed messages negative"
        if sentiment.get("shift") is not None:
            text += (
                f", {sentiment['direction']} than before "
                f"(negative share {_pct(sentiment['previous_negative_share'])} to {_pct(sentiment['negative_share'])})"
            )
        lines.append(text)
    else:
        lines.append("Sentiment: not available")

    if audience["available"]:
        lines.append(
            f"Audience: {_pct(audience['share'])} of the topic's messages come from the segment '{audience['segment']}', "
            f"{audience['lift']}x that segment's share of all messages"
        )
    else:
        lines.append("Audience: not available")

    if network["available"]:
        lines.append(
            f"Network: the topic appears in {network['communities_reached']} of {network['total_communities']} detected communities, "
            f"spread level {network['spread']}"
        )
    else:
        lines.append("Network: not available")

    corroborating = ", ".join(signal["corroborating_vectors"]) or "none"
    missing = ", ".join(signal["missing_vectors"]) or "none"
    lines.append(f"Vectors corroborating the trend: {corroborating}")
    lines.append(f"Vectors missing: {missing}")
    return "\n".join(lines)


def build_prompt(facts):
    return (
        "You are explaining an emerging-signal alert to a non-technical analyst.\n"
        "Use ONLY the facts listed below. Do not add numbers, causes, people, events or predictions that are not in the facts.\n"
        "If a vector is marked not available, say that evidence is missing instead of guessing.\n"
        "Describe the confidence proxy as a heuristic, never as a probability.\n"
        "Write 3 to 4 plain sentences covering what is happening, who is involved, how people feel, how it spreads and why it deserves attention.\n"
        "Do not use bullet points or headings.\n\n"
        f"Facts:\n{facts}"
    )


def deterministic_explanation(signal):
    parts = [signal["what"], signal["who"], signal["how_they_feel"], signal["how_it_spreads"], signal["why_it_matters"]]
    return " ".join(part.strip() for part in parts if part)


def _numbers(text):
    return [float(match) for match in NUMBER_PATTERN.findall(text)]


def find_ungrounded_numbers(text, facts):
    allowed = _numbers(facts)
    allowed += [round(value) for value in allowed]
    ungrounded = []
    for value in _numbers(text):
        if not any(abs(value - known) <= NUMBER_TOLERANCE for known in allowed):
            ungrounded.append(value)
    return ungrounded


def validate_explanation(text, facts):
    if not text or not text.strip():
        return False, "empty"
    if len(text) > MAX_EXPLANATION_CHARS:
        return False, "too_long"
    ungrounded = find_ungrounded_numbers(text, facts)
    if ungrounded:
        return False, "ungrounded_numbers"
    return True, "ok"


def explain_signal(signal, llm=None):
    facts = build_facts(signal)
    fallback = deterministic_explanation(signal)
    if llm is None:
        return {"text": fallback, "source": "evidence_summary", "reason": "no_llm", "facts": facts}
    try:
        generated = llm(build_prompt(facts))
    except Exception:
        generated = None
    if not generated:
        return {"text": fallback, "source": "evidence_summary", "reason": "llm_unavailable", "facts": facts}
    valid, reason = validate_explanation(generated, facts)
    if not valid:
        return {"text": fallback, "source": "evidence_summary", "reason": reason, "facts": facts}
    return {"text": generated.strip(), "source": "gemini", "reason": "ok", "facts": facts}