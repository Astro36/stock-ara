import translators as ts


def translate(text: str) -> dict:
    return ts.translate_text(text, translator="papago", from_language="en", to_language="ko")
