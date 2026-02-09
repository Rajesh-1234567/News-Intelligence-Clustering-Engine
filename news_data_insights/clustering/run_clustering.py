import os
from dotenv import load_dotenv
from pymongo import MongoClient
from qdrant_client import QdrantClient
from collections import defaultdict

from news_data_insights.config import MongoConfig
from news_data_insights.clustering.cluster_engine import cluster_article

load_dotenv(".env")


def run_company_clustering(limit_vectors=500):
    mongo_cfg = MongoConfig()
    mongo = MongoClient(mongo_cfg.uri)
    db = mongo[mongo_cfg.db]

    qdrant = QdrantClient(
        url=os.getenv("QDRANT_URL"),
        api_key=os.getenv("QDRANT_API_KEY"),
    )

    collection = os.getenv("QDRANT_COLLECTION")

    # Fetch vectors from Qdrant
    points, _ = qdrant.scroll(
        collection_name=collection,
        with_payload=True,
        with_vectors=True,
        limit=limit_vectors,
    )

    # Group by company
    company_vectors = defaultdict(list)

    for p in points:
        payload = p.payload
        company = payload["company"]
        article_id = payload["article_id"]

        company_vectors[company].append(
            (article_id, p.vector)
        )

    # Cluster per company
    for company, vectors in company_vectors.items():
        for article_id, vector in vectors:
            cluster_article(
                mongo_db=db,
                company=company,
                article_id=article_id,
                vector=vector,
                company_vectors=[
                    (aid, v)
                    for aid, v in vectors
                    if aid != article_id
                ],
            )

    print("Company-wise clustering completed")


if __name__ == "__main__":
    run_company_clustering(limit_vectors=500)
