import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from intelligence import sentiment

VALID_LABELS = ("Positive", "Neutral", "Negative")


class StubPipeline:
    def __init__(self, label, score=0.9, fail_batch=False, fail_always=False):
        self.label = label
        self.score = score
        self.fail_batch = fail_batch
        self.fail_always = fail_always
        self.calls = []

    def __call__(self, texts, **kwargs):
        self.calls.append((texts, kwargs))
        if self.fail_always:
            raise RuntimeError("model crashed")
        if isinstance(texts, list):
            if self.fail_batch:
                raise RuntimeError("batch failed")
            return [{"label": self.label, "score": self.score} for _ in texts]
        return [{"label": self.label, "score": self.score}]


def install(bert="POSITIVE", xlm="positive", emotion="joy", sarcasm="not_sarcastic", **kwargs):
    stubs = {
        "bert": StubPipeline(bert, **kwargs),
        "xlm": StubPipeline(xlm, **kwargs),
        "emotion": StubPipeline(emotion),
        "sarcasm": StubPipeline(sarcasm),
    }
    sentiment._resources.update(stubs)
    return stubs


def check_valid(analysis):
    assert analysis["sentiment"]["label"] in VALID_LABELS
    assert 0.0 <= analysis["sentiment"]["confidence_proxy"] <= 1.0
    assert "cleaned_text" in analysis and "language" in analysis


def test_empty_and_emoji_only_text():
    stubs = install()
    results = sentiment.analyze_texts(["", "   ", None, "😀😀😀🔥🔥", "!!! ???"])
    assert len(results) == 5
    for analysis in results[:4]:
        check_valid(analysis)
        assert analysis["sentiment"]["label"] == "Neutral"
        assert analysis["sentiment"]["confidence_proxy"] == 0.0
        assert analysis["emotion"] is None and analysis["sarcasm"] is None
    sent_texts = stubs["bert"].calls[0][0] if stubs["bert"].calls else []
    assert "" not in sent_texts
    assert sentiment.analyze_texts([]) == []
    assert sentiment.analyze_texts(["", None])[0]["sentiment"]["models"] == {}
    print("empty and emoji-only ok")


def test_huge_text_is_truncated_for_models():
    stubs = install()
    huge = "this phone is absolutely wonderful " * 6000
    results = sentiment.analyze_texts([huge, "short but fine message here"])
    assert len(results) == 2
    check_valid(results[0])
    kwargs = stubs["bert"].calls[0][1]
    assert kwargs.get("truncation") is True and kwargs.get("max_length") == sentiment.MAX_LENGTH
    print("huge text ok")


def test_emoji_heavy_text_keeps_words():
    install()
    text = "😍😍😍 love this phone 🔥🔥🔥 so good 👍👍👍"
    cleaned = sentiment.clean_text(text)
    assert "love this phone" in cleaned and "so good" in cleaned
    assert not any(ord(char) > 0x1F000 for char in cleaned)
    result = sentiment.analyze_texts([text])[0]
    check_valid(result)
    print("emoji-heavy ok")


def test_hindi_hinglish_and_mixed_language():
    install()
    hindi = "यह फोन बहुत अच्छा है और कैमरा शानदार है"
    hinglish = "yaar ye phone bahut badhiya hai but battery thodi weak hai"
    mixed = "Great phone! फोन बहुत अच्छा है, camera bhi mast hai"
    assert "फोन" in sentiment.clean_text(hindi)
    results = sentiment.analyze_texts([hindi, hinglish, mixed])
    for analysis in results:
        check_valid(analysis)
    assert results[0]["language"]["name"] != "English"
    assert results[0]["emotion"] is None and results[0]["sarcasm"] is None
    print("hindi hinglish mixed ok, languages:", [r["language"]["name"] for r in results])


def test_sarcasm_flag_is_boolean_and_english_only():
    install(sarcasm="sarcastic")
    english = "Oh great, another software update that breaks everything again"
    result = sentiment.analyze_texts([english])[0]
    if result["language"]["code"] == "en":
        assert result["sarcasm"] is not None and isinstance(result["sarcasm"]["flag"], bool)
        assert result["sarcasm"]["flag"] is True
    hindi = sentiment.analyze_texts(["यह फोन बहुत अच्छा है और कैमरा शानदार है"])[0]
    assert hindi["sarcasm"] is None
    print("sarcasm ok")


def test_model_failures_degrade_gracefully():
    install(fail_batch=True)
    results = sentiment.analyze_texts(["I really like this product a lot", "terrible experience, never again"])
    for analysis in results:
        check_valid(analysis)
    install(fail_always=True)
    results = sentiment.analyze_texts(["I really like this product a lot"])
    check_valid(results[0])
    assert results[0]["sentiment"]["models"]["distilbert"]["label"] == "Neutral"
    assert results[0]["sentiment"]["models"]["xlm_roberta"]["label"] == "Neutral"
    print("model failures ok")


def test_ensemble_rules():
    assert sentiment.ensemble("Positive", "Positive", "Negative") == ("Positive", 2)
    assert sentiment.ensemble("Positive", "Negative", "Neutral") == ("Neutral", 1)
    assert sentiment.ensemble("Negative", "Negative", "Negative") == ("Negative", 3)
    print("ensemble rules ok")


if __name__ == "__main__":
    test_empty_and_emoji_only_text()
    test_huge_text_is_truncated_for_models()
    test_emoji_heavy_text_keeps_words()
    test_hindi_hinglish_and_mixed_language()
    test_sarcasm_flag_is_boolean_and_english_only()
    test_model_failures_degrade_gracefully()
    test_ensemble_rules()
    print("all nlp robustness tests passed")