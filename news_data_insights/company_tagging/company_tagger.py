from pathlib import Path
import json
import re
import spacy
from typing import List, Set

# ===============================
# LOAD MASTER COMPANIES
# ===============================

BASE_DIR = Path(__file__).resolve().parents[1]
MASTER_PATH = BASE_DIR / "data" / "master_companies.json"

with open(MASTER_PATH, "r") as f:
    master_company_dict = json.load(f)

print(f"[CompanyTagger] Total companies loaded: {len(master_company_dict)}")

# ===============================
# NLP SETUP
# ===============================

nlp = spacy.load("en_core_web_sm")

SUFFIXES = ["ltd", "limited", "corp", "corporation", "private", "pvt", "inc"]

# Add Entity Ruler BEFORE NER
if "entity_ruler" not in nlp.pipe_names:
    ruler = nlp.add_pipe("entity_ruler", before="ner")
else:
    ruler = nlp.get_pipe("entity_ruler")

patterns = []

for ticker, info in master_company_dict.items():

    # 1. Ticker (CASE-SENSITIVE)
    patterns.append({
        "label": "ORG",
        "pattern": [{"TEXT": ticker}],
        "id": ticker
    })

    # 2. Company name (CASE-INSENSITIVE, suffix-aware)
    full_name = info["name"].lower().replace(".", "")
    name_tokens = [w for w in full_name.split() if w not in SUFFIXES]

    token_pattern = [{"LOWER": w} for w in name_tokens]

    token_pattern.append({"LOWER": {"IN": SUFFIXES}, "OP": "?"})
    token_pattern.append({"IS_PUNCT": True, "OP": "?"})
    token_pattern.append({"LOWER": {"IN": SUFFIXES}, "OP": "?"})

    patterns.append({
        "label": "ORG",
        "pattern": token_pattern,
        "id": ticker
    })

ruler.add_patterns(patterns)

# ===============================
# TEXT NORMALIZATION
# ===============================

def normalize_news_text(text: str) -> str:
    def replace_func(match):
        word = match.group(0)
        return word if word.isupper() else word.lower()

    return re.sub(r"\b\w+\b", replace_func, text)


# ===============================
# TAGGING FUNCTION
# ===============================

def get_company_tags(text: str) -> List[str]:
    if not text:
        return []

    doc = nlp(normalize_news_text(text))
    companies: Set[str] = {
        ent.ent_id_ for ent in doc.ents if ent.ent_id_
    }

    return list(companies)
