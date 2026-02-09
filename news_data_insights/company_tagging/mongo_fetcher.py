from pymongo import MongoClient
from typing import Iterator, List, Dict, Optional

from news_data_insights.config import MongoConfig


class MarketNewsMongoFetcher:
    def __init__(self):
        self.cfg = MongoConfig()
        self.client = MongoClient(self.cfg.uri)
        self.collection = self.client[self.cfg.db][self.cfg.collection]

    def fetch_batches(
        self,
        batch_size: int = 500,
        limit_docs: Optional[int] = None,
    ) -> Iterator[List[Dict]]:
        cursor = (
            self.collection
            .find({})
            .sort("created_at", -1)   # 🔑 latest first
        )

        if limit_docs:
            cursor = cursor.limit(limit_docs)

        batch = []
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch
