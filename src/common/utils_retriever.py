import re

TOKEN_PATTERN = re.compile(r"[a-zà-ÿ0-9]+(?:[/-][a-zà-ÿ0-9]+)*")

def tokenize(text: str) -> list[str]:
    text = (text or "").lower()
    raw_tokens = TOKEN_PATTERN.findall(text)

    tokens: list[str] = []
    for token in raw_tokens:
        tokens.append(token)

        if "/" in token or "-" in token:
            parts = re.split(r"[/-]", token)
            tokens.extend([p for p in parts if p])

    return tokens