from datetime import datetime
from pymongo import MongoClient

from news_data_insights.config import MongoConfig
from news_data_insights.insights.article_selector import pick_article_from_cluster
from news_data_insights.insights.prompt_builder import build_company_prompt
from news_data_insights.insights.llm_client import generate_insight

cfg = MongoConfig()
mongo = MongoClient(cfg.uri)
db = mongo[cfg.db]

clusters_col = db.company_clusters_active
insights_col = db.company_insights

def run_company_insight(company: str):
    clusters = list(clusters_col.find({"company": company}))
    if not clusters:
        return

    cluster_ids = sorted(c["cluster_id"] for c in clusters)

    existing = insights_col.find_one(
        {"company": company},
        sort=[("generated_at", -1)],
    )

    if existing and sorted(existing.get("used_cluster_ids", [])) == cluster_ids:
        print(f"♻️ Reusing insight for {company}")
        return

    articles = []
    used_article_ids = []

    for c in clusters:
        article = pick_article_from_cluster(c["article_ids"])
        if article:
            articles.append(article)
            used_article_ids.append(article["article_id"])

    if not articles:
        return

    prompt = build_company_prompt(company, articles)
    llm_output = generate_insight(prompt)

    insights_col.update_one(
        {"company": company},
        {
            "$set": {
                "company": company,
                "summary": llm_output,
                "used_cluster_ids": cluster_ids,
                "used_article_ids": used_article_ids,
                "generated_at": datetime.utcnow(),
            }
        },
        upsert=True
    )

    print(f"✅ Generated new insight for {company}")
