def build_company_prompt(company: str, articles: list[dict]) -> str:
    article_block = ""

    for a in articles:
        title = (a.get("title") or "").strip()
        desc = (a.get("description") or "").strip()
        article_block += f"\n- {title}\n  {desc}\n"

    return f"""
You are a senior financial market analyst.

Company: {company}

Below are key news articles (deduplicated by topic):

{article_block}

Provide:
1. Summary (concise, factual)
2. Sentiment (Positive / Neutral / Negative)
3. Market Impact (short-term, or say 'No direct market impact')

Rules:
- No numbers unless explicitly stated
- No investment advice
- Do NOT repeat articles
"""
