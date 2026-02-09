from pymongo import MongoClient
from collections import defaultdict

from news_data_insights.config import MongoConfig


def print_all_company_cluster_summaries(
    min_articles_per_cluster: int = 1,
    max_companies: int | None = None,
    max_text_len: int = 300,
):
    """
    Print company-wise cluster summaries with article text.
    """

    cfg = MongoConfig()
    mongo = MongoClient(cfg.uri)
    db = mongo[cfg.db]

    cluster_col = db.company_clusters_active
    news_col = db.portfolio_market_news

    clusters = list(cluster_col.find())

    if not clusters:
        print("❌ No clusters found")
        return

    # Group clusters by company
    company_map = defaultdict(list)
    for c in clusters:
        company_map[c["company"]].append(c)

    printed = 0

    for company, company_clusters in sorted(company_map.items()):
        if max_companies and printed >= max_companies:
            break

        print("\n" + "=" * 80)
        print(f"🏢 Company: {company}")
        print("=" * 80)

        for idx, cluster in enumerate(company_clusters, start=1):
            article_ids = cluster.get("article_ids", [])
            count = len(article_ids)

            if count < min_articles_per_cluster:
                continue

            print(f"\n  🔹 Cluster {idx}")
            print(f"     Cluster ID    : {cluster['cluster_id']}")
            print(f"     Article count : {count}")

            for aid in article_ids:
                article = news_col.find_one(
                    {"article_id": aid},
                    {"title": 1, "description": 1},
                )

                if not article:
                    continue

                title = (article.get("title") or "").strip()
                desc = (article.get("description") or "").strip()

                if not title:
                     title = "[No title]"
                if not desc:
                    desc = "[No description available]"


                if len(desc) > max_text_len:
                    desc = desc[:max_text_len] + "..."

                print("\n       📰 Article ID:", aid)
                print("       🏷️ Title:", title)
                print("       🧾 Description:", desc)

        printed += 1

    print("\n✅ Done printing company cluster summaries with text")


if __name__ == "__main__":
    print_all_company_cluster_summaries(
        min_articles_per_cluster=3,   # change to 2 for stronger clusters
        max_companies=None,           # None = all companies
        max_text_len=300,             # trim long descriptions
    )
