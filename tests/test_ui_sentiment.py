import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ui_sentiment import (
    language_name,
    overall_headline,
    ranked_bars_html,
    stacked_bars_html,
    compare_headline,
    compare_summary_html,
    dominant_label,
    mood_phrase,
    percentages,
    single_summary_html,
)


def test_percentages():
    pcts = percentages({"Positive": 1, "Neutral": 2, "Negative": 1}, 4)
    assert pcts == {"Positive": 25, "Neutral": 50, "Negative": 25}
    assert percentages({}, 0) == {"Positive": 0, "Neutral": 0, "Negative": 0}
    assert dominant_label(pcts) == "Neutral"
    assert mood_phrase({"Positive": 60, "Neutral": 30, "Negative": 10}) == "Mostly positive"
    print("percentages ok")


def test_single_summary():
    markup = single_summary_html({"Positive": 5, "Neutral": 3, "Negative": 2}, 10)
    assert "<svg" in markup and "50%" in markup and "5 messages" in markup
    empty = single_summary_html({}, 0)
    assert "0%" in empty and "nan" not in empty.lower()
    print("single summary ok")


def test_compare():
    a = {"Positive": 50, "Neutral": 30, "Negative": 20}
    b = {"Positive": 20, "Neutral": 30, "Negative": 50}
    pa = percentages(a, 100)
    pb = percentages(b, 100)
    assert "Alpha gets the more positive reaction" in compare_headline("Alpha", pa, "Beta", pb)
    assert "similar" in compare_headline("Alpha", pa, "Beta", pa)
    markup = compare_summary_html("Alpha", a, 100, "<Beta>", b, 100)
    assert markup.count("<svg") == 2 and "Head to head" in markup
    assert "&lt;Beta&gt;" in markup and "<Beta>" not in markup
    assert "Alpha is higher by 30 pts" in markup and "&lt;Beta&gt; is higher by 30 pts" in markup
    assert "Same share" in markup
    assert "50%" in markup and "100 messages" in markup
    zero = compare_summary_html("A", {}, 0, "B", {}, 0)
    assert "nan" not in zero.lower()
    print("compare ok")


def test_sentiment_page_helpers():
    assert language_name("id") == "Indonesian" and language_name("Hindi") == "Hindi"
    assert language_name("Unknown") == "Not detected" and language_name(None) == "Not detected"
    assert language_name("xx") == "XX"
    assert overall_headline({"Positive": 36, "Neutral": 42, "Negative": 22}, 100).startswith("Overall mood is neutral: 42% neutral")
    assert overall_headline({}, 0) == "No analyzed messages yet."
    rows = [{"name": "A <b>", "total": 10, "counts": {"Positive": 5, "Neutral": 3, "Negative": 2}}]
    markup = stacked_bars_html(rows)
    assert "A &lt;b&gt;" in markup and "10 messages" in markup
    assert stacked_bars_html([]) == ""
    ranked = ranked_bars_html([("joy", 30), ("anger", 10)], 40)
    assert "Joy" in ranked and "75%" in ranked and "25%" in ranked
    assert ranked_bars_html([], 0) == ""
    print("sentiment page helpers ok")


if __name__ == "__main__":
    test_percentages()
    test_single_summary()
    test_compare()
    test_sentiment_page_helpers()
    print("all ui sentiment tests passed")