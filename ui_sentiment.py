import html
import math

LABELS = ("Positive", "Neutral", "Negative")
RING_COLORS = {"Positive": "#639922", "Neutral": "#B4B2A9", "Negative": "#E24B4A"}
TEXT_COLORS = {"Positive": "#3B6D11", "Neutral": "#5F5E5A", "Negative": "#A32D2D"}
TILE_STYLES = {
    "Positive": ("#F2F8EA", "#CFE0B8"),
    "Neutral": ("#F5F4F0", "#DEDBD0"),
    "Negative": ("#FEF4F3", "#F0CBC7"),
}
MOOD_WORDS = {"Positive": "positive", "Neutral": "neutral", "Negative": "negative"}
SIMILAR_NET_POINTS = 3
VIEW_BOX = 160
CENTER = VIEW_BOX / 2


def fractions(counts, total):
    if not total:
        return {label: 0.0 for label in LABELS}
    return {label: counts.get(label, 0) / total for label in LABELS}


def percentages(counts, total):
    return {label: round(value * 100) for label, value in fractions(counts, total).items()}


def dominant_label(pcts):
    best = max(LABELS, key=lambda label: pcts[label])
    return best


def mood_phrase(pcts):
    label = dominant_label(pcts)
    return f"Mostly {MOOD_WORDS[label]}"


def net_mood(pcts):
    return pcts["Positive"] - pcts["Negative"]


def _ring(fracs, radius, stroke, opacity=1.0):
    circumference = 2 * math.pi * radius
    parts = [
        f'<circle cx="{CENTER}" cy="{CENTER}" r="{radius}" fill="none" stroke="#F0EEE8" stroke-width="{stroke}"/>'
    ]
    offset = 0.0
    for label in LABELS:
        length = circumference * fracs[label]
        if length > 0:
            gap = circumference - length
            parts.append(
                f'<circle cx="{CENTER}" cy="{CENTER}" r="{radius}" fill="none" stroke="{RING_COLORS[label]}" '
                f'stroke-width="{stroke}" stroke-opacity="{opacity}" stroke-dasharray="{length:.2f} {gap:.2f}" '
                f'stroke-dashoffset="{-offset:.2f}" transform="rotate(-90 {CENTER} {CENTER})"/>'
            )
        offset += length
    return "".join(parts)


def donut_svg(rings, top_text, bottom_text, size, top_color="#2C2C2A", top_size=30):
    body = "".join(_ring(fracs, radius, stroke, opacity) for fracs, radius, stroke, opacity in rings)
    return (
        f'<svg width="{size}" height="{size}" viewBox="0 0 {VIEW_BOX} {VIEW_BOX}" role="img">'
        f"{body}"
        f'<text x="{CENTER}" y="{CENTER - 2}" text-anchor="middle" font-size="{top_size}" font-weight="700" fill="{top_color}">{html.escape(top_text)}</text>'
        f'<text x="{CENTER}" y="{CENTER + 16}" text-anchor="middle" font-size="11" fill="#5F5E5A">{html.escape(bottom_text)}</text>'
        "</svg>"
    )


def _tile(label, pct, count):
    background, border = TILE_STYLES[label]
    return (
        f'<div style="background:{background};border:1px solid {border};border-radius:12px;padding:12px 14px;text-align:center;">'
        f'<div style="display:flex;align-items:center;justify-content:center;gap:6px;font-size:13px;color:#5F5E5A;font-weight:600;">'
        f'<span style="width:9px;height:9px;border-radius:50%;background:{RING_COLORS[label]};display:inline-block;"></span>{label}</div>'
        f'<div style="font-size:30px;font-weight:800;color:{TEXT_COLORS[label]};line-height:1.15;margin-top:4px;">{pct}%</div>'
        f'<div style="font-size:12px;color:#888780;margin-top:2px;">{count:,} messages</div>'
        "</div>"
    )


def single_summary_html(counts, total):
    fracs = fractions(counts, total)
    pcts = percentages(counts, total)
    label = dominant_label(pcts)
    svg = donut_svg([(fracs, 62, 18, 1.0)], f"{pcts[label]}%", MOOD_WORDS[label], 210, TEXT_COLORS[label], 32)
    tiles = "".join(_tile(name, pcts[name], counts.get(name, 0)) for name in LABELS)
    return (
        '<div style="background:#fff;border:1px solid #E5E2D9;border-radius:16px;padding:22px 22px 20px;'
        'box-shadow:0 1px 3px rgba(44,44,42,0.05);margin-bottom:16px;">'
        '<div style="display:flex;flex-direction:column;align-items:center;">'
        f'<div style="font-size:18px;font-weight:700;color:#2C2C2A;margin-bottom:6px;">{mood_phrase(pcts)}</div>'
        f"{svg}"
        "</div>"
        f'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:14px;">{tiles}</div>'
        "</div>"
    )


def compare_headline(name_a, pcts_a, name_b, pcts_b):
    net_a = net_mood(pcts_a)
    net_b = net_mood(pcts_b)
    if abs(net_a - net_b) < SIMILAR_NET_POINTS:
        return "Both topics get a similar overall mood."
    leader, other = (name_a, name_b) if net_a > net_b else (name_b, name_a)
    leader_net, other_net = (net_a, net_b) if net_a > net_b else (net_b, net_a)
    return (
        f"{leader} gets the more positive reaction "
        f"(net mood {leader_net:+d} versus {other_net:+d} for {other})."
    )


def _bar_segments(fracs, pcts):
    parts = []
    for label in LABELS:
        width = fracs[label] * 100
        if width <= 0:
            continue
        text = f"{pcts[label]}%" if pcts[label] >= 7 else ""
        text_color = "#3D3D3A" if label == "Neutral" else "#FFFFFF"
        parts.append(
            f'<div style="width:{width:.2f}%;background:{RING_COLORS[label]};display:flex;align-items:center;'
            f'justify-content:center;color:{text_color};font-size:14px;font-weight:700;">{text}</div>'
        )
    return "".join(parts)


def _bar_row(name, total, fracs, pcts):
    return (
        '<div style="margin-bottom:18px;">'
        '<div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;margin-bottom:6px;">'
        f'<span style="font-size:16px;font-weight:700;color:#2C2C2A;">{html.escape(name)}</span>'
        f'<span style="font-size:12px;color:#888780;white-space:nowrap;">{total:,} messages</span>'
        "</div>"
        '<div style="display:flex;height:42px;border-radius:10px;overflow:hidden;background:#F0EEE8;">'
        f"{_bar_segments(fracs, pcts)}"
        "</div>"
        "</div>"
    )


def _legend():
    items = "".join(
        f'<span style="display:inline-flex;align-items:center;gap:6px;">'
        f'<span style="width:10px;height:10px;border-radius:3px;background:{RING_COLORS[label]};display:inline-block;"></span>{label}</span>'
        for label in LABELS
    )
    return f'<div style="display:flex;gap:18px;justify-content:center;font-size:12px;color:#5F5E5A;margin:2px 0 4px;">{items}</div>'


def _line(name, pct):
    return (
        '<div style="display:flex;justify-content:space-between;gap:10px;font-size:13px;">'
        f'<span style="color:#5F5E5A;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{html.escape(name)}</span>'
        f'<span style="font-weight:700;color:#2C2C2A;">{pct}%</span>'
        "</div>"
    )


def _head_to_head_tile(label, name_a, pct_a, name_b, pct_b):
    background, border = TILE_STYLES[label]
    diff = pct_a - pct_b
    if diff == 0:
        verdict = "Same share"
    else:
        higher = name_a if diff > 0 else name_b
        verdict = f"{html.escape(higher)} is higher by {abs(diff)} pts"
    return (
        f'<div style="background:{background};border:1px solid {border};border-radius:12px;padding:12px 14px;">'
        f'<div style="display:flex;align-items:center;gap:6px;font-size:13px;font-weight:700;color:{TEXT_COLORS[label]};margin-bottom:8px;">'
        f'<span style="width:9px;height:9px;border-radius:50%;background:{RING_COLORS[label]};display:inline-block;"></span>{label}</div>'
        f"{_line(name_a, pct_a)}{_line(name_b, pct_b)}"
        f'<div style="font-size:12px;color:#5F5E5A;margin-top:8px;padding-top:8px;border-top:1px dashed {border};">{verdict}</div>'
        "</div>"
    )


def _mini_tile(label, pct, count):
    background, border = TILE_STYLES[label]
    return (
        f'<div style="background:{background};border:1px solid {border};border-radius:10px;padding:9px 6px;text-align:center;">'
        f'<div style="display:flex;align-items:center;justify-content:center;gap:5px;font-size:12px;color:#5F5E5A;font-weight:600;">'
        f'<span style="width:8px;height:8px;border-radius:50%;background:{RING_COLORS[label]};display:inline-block;"></span>{label}</div>'
        f'<div style="font-size:22px;font-weight:800;color:{TEXT_COLORS[label]};line-height:1.2;margin-top:2px;">{pct}%</div>'
        f'<div style="font-size:11px;color:#888780;">{count:,}</div>'
        "</div>"
    )


def _donut_card(name, counts, total):
    fracs = fractions(counts, total)
    pcts = percentages(counts, total)
    label = dominant_label(pcts)
    svg = donut_svg([(fracs, 62, 18, 1.0)], f"{pcts[label]}%", MOOD_WORDS[label], 190, TEXT_COLORS[label], 30)
    tiles = "".join(_mini_tile(item, pcts[item], counts.get(item, 0)) for item in LABELS)
    return (
        '<div style="background:#fff;border:1px solid #E5E2D9;border-radius:16px;padding:18px 16px 16px;'
        'box-shadow:0 1px 3px rgba(44,44,42,0.05);min-width:0;">'
        '<div style="display:flex;flex-direction:column;align-items:center;">'
        f'<div style="font-size:17px;font-weight:700;color:#2C2C2A;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{html.escape(name)}</div>'
        f'<div style="font-size:12px;color:#888780;margin:2px 0 6px;">{total:,} messages · {mood_phrase(pcts).lower()}</div>'
        f"{svg}"
        "</div>"
        f'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px;">{tiles}</div>'
        "</div>"
    )


def compare_summary_html(name_a, counts_a, total_a, name_b, counts_b, total_b):
    pcts_a = percentages(counts_a, total_a)
    pcts_b = percentages(counts_b, total_b)
    headline = html.escape(compare_headline(name_a, pcts_a, name_b, pcts_b))
    donuts = (
        '<div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;">'
        f"{_donut_card(name_a, counts_a, total_a)}{_donut_card(name_b, counts_b, total_b)}"
        "</div>"
    )
    tiles = "".join(
        _head_to_head_tile(label, name_a, pcts_a[label], name_b, pcts_b[label]) for label in LABELS
    )
    note = (
        '<div style="font-size:12px;color:#888780;margin-top:12px;">'
        "Net mood is the share of positive messages minus the share of negative ones. "
        "Differences of a few points can be random, especially when one topic has fewer messages."
        "</div>"
    )
    return (
        f'<div style="font-size:17px;font-weight:700;color:#2C2C2A;margin:4px 0 14px;text-align:center;">{headline}</div>'
        f"{donuts}"
        '<div style="background:#fff;border:1px solid #E5E2D9;border-radius:16px;padding:18px;margin:16px 0;'
        'box-shadow:0 1px 3px rgba(44,44,42,0.05);">'
        '<div style="font-size:14px;font-weight:700;color:#2C2C2A;margin-bottom:12px;">Head to head</div>'
        f'<div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;">{tiles}</div>'
        f"{note}</div>"
    )


LANGUAGE_NAMES = {
    "en": "English", "hi": "Hindi", "id": "Indonesian", "so": "Somali", "tl": "Tagalog", "es": "Spanish",
    "fr": "French", "de": "German", "pt": "Portuguese", "it": "Italian", "nl": "Dutch", "ru": "Russian",
    "ar": "Arabic", "ur": "Urdu", "bn": "Bengali", "mr": "Marathi", "ta": "Tamil", "te": "Telugu",
    "gu": "Gujarati", "pa": "Punjabi", "ne": "Nepali", "tr": "Turkish", "ja": "Japanese", "ko": "Korean",
    "zh-cn": "Chinese", "zh-tw": "Chinese", "sw": "Swahili", "af": "Afrikaans", "ca": "Catalan",
    "cy": "Welsh", "sv": "Swedish", "no": "Norwegian", "da": "Danish", "fi": "Finnish", "pl": "Polish",
    "ro": "Romanian", "vi": "Vietnamese", "th": "Thai", "sq": "Albanian", "et": "Estonian", "hr": "Croatian",
}
EMOTION_COLORS = {
    "joy": "#639922", "surprise": "#2BA6A0", "neutral": "#B4B2A9", "sadness": "#378ADD",
    "fear": "#7F5AA2", "anger": "#E24B4A", "disgust": "#9A7B3A",
}
DEFAULT_EMOTION_COLOR = "#888780"


def language_name(value):
    text = str(value or "").strip()
    if text.lower() in ("", "unknown", "und"):
        return "Not detected"
    return LANGUAGE_NAMES.get(text.lower(), text if len(text) > 3 else text.upper())


def overall_headline(counts, total):
    if not total:
        return "No analyzed messages yet."
    pcts = percentages(counts, total)
    label = dominant_label(pcts)
    others = [name for name in LABELS if name != label]
    others.sort(key=lambda name: -pcts[name])
    return (
        f"Overall mood is {MOOD_WORDS[label]}: {pcts[label]}% {MOOD_WORDS[label]}, "
        f"{pcts[others[0]]}% {MOOD_WORDS[others[0]]} and {pcts[others[1]]}% {MOOD_WORDS[others[1]]}."
    )


def _card(inner):
    return (
        '<div style="background:#fff;border:1px solid #E5E2D9;border-radius:16px;padding:22px 22px 8px;'
        f'box-shadow:0 1px 3px rgba(44,44,42,0.05);margin-bottom:16px;">{inner}</div>'
    )


def stacked_bars_html(rows):
    if not rows:
        return ""
    bars = "".join(
        _bar_row(
            row["name"], row["total"],
            fractions(row["counts"], row["total"]), percentages(row["counts"], row["total"]),
        )
        for row in rows
    )
    return _card(f"{bars}{_legend()}<div style='height:10px;'></div>")


def ranked_bars_html(rows, total):
    if not rows or not total:
        return ""
    top = max(count for _, count in rows) or 1
    lines = []
    for name, count in rows:
        width = max(2.0, count / top * 100)
        color = EMOTION_COLORS.get(str(name).lower(), DEFAULT_EMOTION_COLOR)
        share = round(count / total * 100)
        lines.append(
            '<div style="margin-bottom:14px;">'
            '<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:5px;">'
            f'<span style="font-size:14px;font-weight:600;color:#2C2C2A;">{html.escape(str(name).title())}</span>'
            f'<span style="font-size:12px;color:#5F5E5A;"><b>{share}%</b> · {count:,} messages</span>'
            "</div>"
            '<div style="height:12px;border-radius:6px;background:#F0EEE8;overflow:hidden;">'
            f'<div style="width:{width:.2f}%;height:100%;background:{color};border-radius:6px;"></div>'
            "</div></div>"
        )
    return _card("".join(lines))


def mood_bar_html(shares):
    fracs = {
        "Positive": shares.get("positive", 0.0),
        "Neutral": shares.get("neutral", 0.0),
        "Negative": shares.get("negative", 0.0),
    }
    pcts = {label: round(value * 100) for label, value in fracs.items()}
    return (
        '<div style="display:flex;height:30px;border-radius:9px;overflow:hidden;background:#F0EEE8;">'
        f"{_bar_segments(fracs, pcts)}"
        "</div>"
        f"{_legend()}"
    )