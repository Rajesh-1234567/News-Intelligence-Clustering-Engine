from dataclasses import dataclass
import os


@dataclass
class MongoConfig:
    uri: str = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
    db: str = os.getenv("MONGO_DB", "market_external_db")

    # source articles
    collection: str = os.getenv(
        "MONGO_COLLECTION",
        "portfolio_market_news"
    )

    # article → companies mapping
    out_collection: str = os.getenv(
        "MONGO_OUT_COLLECTION",
        "portfolio_article_company_map"
    )
