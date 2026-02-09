from pymongo import MongoClient
from dotenv import load_dotenv

from news_data_insights.config import MongoConfig

load_dotenv(".env")


def print_company_insights(limit: int | None = None):
    cfg = MongoConfig()
    mongo = MongoClient(cfg.uri)
    db = mongo[cfg.db]

    insights_col = db.company_insights
    news_col = db.portfolio_market_news

    cursor = insights_col.find().sort("generated_at", -1)
    if limit:
        cursor = cursor.limit(limit)

    count = 0

    for ins in cursor:
        count += 1

        print("\n" + "=" * 90)
        print(f"🏢 Company       : {ins.get('company')}")
        print("=" * 90)

        # -------- SUMMARY --------
        print("\n🧠 Summary:")
        print(ins.get("summary", "").strip())

        # -------- SENTIMENT --------
        if ins.get("sentiment"):
            print(f"\n📊 Sentiment     : {ins.get('sentiment')}")

        # -------- MARKET IMPACT --------
        if ins.get("market_impact"):
            print(f"\n📈 Market Impact : {ins.get('market_impact')}")

        # -------- CLUSTERS --------
        print("\n🧩 Used Clusters:")
        for cid in ins.get("used_cluster_ids", []):
            print(f"  - {cid}")

        # -------- ARTICLES (ID + TITLE) --------
        print("\n📰 Used Articles:")
        used_articles = ins.get("used_article_ids")

        if not used_articles:
            print("  ⚠️  Not available (legacy insight)")
        else:
            for aid in used_articles:
                article = news_col.find_one(
                    {"article_id": aid},
                    {"title": 1},
                )

                title = (article.get("title") if article else None) or "[Title not found]"

                print(f"  - {aid}")
                print(f"    ↳ {title}")

        # -------- TIME --------
        print(f"\n⏱ Generated At  : {ins.get('generated_at')}")

    if count == 0:
        print("❌ No company insights found")
    else:
        print(f"\n✅ Printed {count} company insights")


if __name__ == "__main__":
    print_company_insights(limit=None)  # change limit if needed
