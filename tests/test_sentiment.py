import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from intelligence.sentiment import analyze_text, analyze_texts


def check_schema(analysis):
    assert set(analysis.keys()) >= {
        "language", "cleaned_text", "sentiment", "emotion", "sarcasm", "engine_version", "analyzed_at"
    }
    assert analysis["sentiment"]["label"] in ("Positive", "Neutral", "Negative")
    assert 0.0 <= analysis["sentiment"]["confidence_proxy"] <= 1.0


def run():
    positive = analyze_text("I absolutely love this phone, the camera is amazing and the battery lasts all day")
    check_schema(positive)
    assert positive["language"]["code"] == "en"
    assert positive["sentiment"]["label"] == "Positive"
    assert positive["emotion"] is not None
    print("positive ok:", positive["sentiment"]["label"], positive["emotion"])

    negative = analyze_text("This is terrible, the worst purchase I have ever made and the support is useless")
    check_schema(negative)
    assert negative["sentiment"]["label"] == "Negative"
    print("negative ok:", negative["sentiment"]["label"], negative["emotion"])

    hindi = analyze_text("यह फोन बहुत अच्छा है और मुझे बहुत पसंद आया")
    check_schema(hindi)
    assert hindi["language"]["code"] == "hi"
    assert hindi["emotion"] is None
    assert hindi["sarcasm"] is None
    print("hindi ok:", hindi["language"], hindi["sentiment"]["label"])

    empty = analyze_text("   ")
    check_schema(empty)
    assert empty["sentiment"]["label"] == "Neutral"
    assert empty["sentiment"]["agreement"] == 0
    print("empty ok")

    batch = analyze_texts([
        "Great product, highly recommended",
        "",
        "Bahut bekar quality hai, paisa barbaad",
        "Just got the update installed today"
    ])
    assert len(batch) == 4
    for item in batch:
        check_schema(item)
    assert batch[1]["sentiment"]["models"] == {}
    print("batch ok:", [b["sentiment"]["label"] for b in batch])

    print("all sentiment tests passed")


if __name__ == "__main__":
    run()