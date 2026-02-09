import os
from pathlib import Path
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(ENV_PATH)

from qdrant_client import QdrantClient
from qdrant_client.models import (
    VectorParams,
    Distance,
    HnswConfigDiff,
    PayloadSchemaType,
)


QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = os.getenv(
    "QDRANT_COLLECTION",
    "news_collections"
)

VECTOR_SIZE = 384  # MiniLM


def get_qdrant() -> QdrantClient:
    if not QDRANT_URL:
        raise RuntimeError("QDRANT_URL not set")

    return QdrantClient(
        url=QDRANT_URL,
        api_key=QDRANT_API_KEY,
    )


def ensure_collection():
    client = get_qdrant()

    existing = {c.name for c in client.get_collections().collections}

    if QDRANT_COLLECTION not in existing:
        client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(
                size=VECTOR_SIZE,
                distance=Distance.COSINE,
            ),
            hnsw_config=HnswConfigDiff(
                m=16,
                ef_construct=200,
                on_disk=False,
            ),
            on_disk_payload=True,
        )

    ensure_payload_indexes(client)


def ensure_payload_indexes(client: QdrantClient):
    keyword_fields = [
        "article_id",
        "company",
    ]

    datetime_fields = [
        "published_at",
    ]

    for field in keyword_fields:
        try:
            client.create_payload_index(
                collection_name=QDRANT_COLLECTION,
                field_name=field,
                field_schema=PayloadSchemaType.KEYWORD,
            )
        except Exception:
            pass

    for field in datetime_fields:
        try:
            client.create_payload_index(
                collection_name=QDRANT_COLLECTION,
                field_name=field,
                field_schema=PayloadSchemaType.DATETIME,
            )
        except Exception:
            pass


if __name__ == "__main__":
    ensure_collection()
    print("Qdrant collection ready")
