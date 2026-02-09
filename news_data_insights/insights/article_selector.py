from pymongo import MongoClient
from news_data_insights.config import MongoConfig

cfg = MongoConfig()
mongo = MongoClient(cfg.uri)
db = mongo[cfg.db]

news_col = db["portfolio_market_news"]

def pick_article_from_cluster(article_ids: list[str]) -> dict | None:
    return news_col.find_one(
        {"article_id": {"$in": article_ids}},
        sort=[("pubDate", -1)],
        projection={
            "article_id": 1,
            "title": 1,
            "description": 1,
        },
    )
