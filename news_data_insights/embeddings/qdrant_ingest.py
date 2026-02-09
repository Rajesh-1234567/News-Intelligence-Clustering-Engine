from datetime import datetime
import uuid

from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from qdrant_client.models import PointStruct

from news_data_insights.config import MongoConfig
from news_data_insights.embeddings.qdrant_setup import (
    get_qdrant,
    QDRANT_COLLECTION,
)


# --------------------------------------------------
# Helper: fetch already embedded article_ids
# --------------------------------------------------

def get_existing_article_ids(qdrant, collection, limit=10000):
    """
    Fetch article_ids already stored in Qdrant.
    Used to avoid duplicate embeddings.
    """
    existing_ids = set()
    offset = None

    while True:
        points, offset = qdrant.scroll(
            collection_name=collection,
            with_payload=True,
            limit=500,
            offset=offset,
        )

        for p in points:
            if p.payload and "article_id" in p.payload:
                existing_ids.add(p.payload["article_id"])

        if offset is None:
            break

        if len(existing_ids) >= limit:
            break

    return existing_ids


# --------------------------------------------------
# INIT
# --------------------------------------------------

model = SentenceTransformer(
    "sentence-transformers/all-MiniLM-L6-v2",
    device="cpu"
)

mongo_cfg = MongoConfig()
mongo = MongoClient(mongo_cfg.uri)
db = mongo[mongo_cfg.db]

# Source collections
article_map_col = db[mongo_cfg.out_collection]          # article_company_map
news_col = db["portfolio_market_news"]                  # raw articles

# Qdrant
qdrant = get_qdrant()


# --------------------------------------------------
# INGEST LOGIC
# --------------------------------------------------

def run_embedding_ingest(limit_new_articles: int = 300):
    """
    Embed and store ONLY new unique articles into Qdrant.
    One vector per (article_id, company).
    """

    # 1️⃣ Find what is already embedded
    existing_article_ids = get_existing_article_ids(
        qdrant,
        QDRANT_COLLECTION,
    )

    print(f"Already embedded unique articles: {len(existing_article_ids)}")

    points = []
    new_articles_added = 0

    cursor = article_map_col.find({})

    for mapping in cursor:
        article_id = mapping["article_id"]

        # 🔒 Skip if already embedded
        if article_id in existing_article_ids:
            continue

        companies = mapping["companies"]

        article = news_col.find_one(
            {"article_id": article_id},
            {"title": 1, "description": 1, "pubDate": 1},
        )

        if not article:
            continue

        text = f"{article.get('title','')}\n{article.get('description','')}"
        if not text.strip():
            continue

        # 2️⃣ Generate embedding ONCE per article
        vector = model.encode(text).tolist()
        published_at = article.get("pubDate")

        # 3️⃣ Create one vector per company
        for company in companies:
            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=vector,
                    payload={
                        "article_id": article_id,
                        "company": company,
                        "published_at": published_at,
                        "ingested_at": datetime.utcnow().isoformat(),
                    },
                )
            )

        new_articles_added += 1

        # 4️⃣ Batch upsert
        if len(points) >= 100:
            qdrant.upsert(
                collection_name=QDRANT_COLLECTION,
                points=points,
            )
            points = []

        # 🛑 Stop after required number of NEW articles
        if new_articles_added >= limit_new_articles:
            break

    # Final flush
    if points:
        qdrant.upsert(
            collection_name=QDRANT_COLLECTION,
            points=points,
        )

    print(f"New unique articles embedded: {new_articles_added}")


# --------------------------------------------------
# CLI ENTRY
# --------------------------------------------------

if __name__ == "__main__":
    run_embedding_ingest(limit_new_articles=300)
    print("Embedding ingest completed")
