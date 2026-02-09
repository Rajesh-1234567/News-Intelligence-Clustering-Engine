from datetime import datetime
from pymongo import MongoClient, errors

from news_data_insights.config import MongoConfig
from news_data_insights.company_tagging.mongo_fetcher import MarketNewsMongoFetcher
from news_data_insights.company_tagging.company_tagger import get_company_tags


class CompanyTaggingPipeline:
    """
    Runs company tagging on latest news articles
    and stores unique article_id -> companies mapping.
    """

    def __init__(self):
        self.cfg = MongoConfig()
        self.client = MongoClient(self.cfg.uri)
        self.db = self.client[self.cfg.db]
        self.output_collection = self.db[self.cfg.out_collection]
        self.fetcher = MarketNewsMongoFetcher()

    def run(
        self,
        batch_size: int = 500,
        limit_docs: int = 15000,   # 🔑 latest 15k
    ):
        total_seen = 0
        total_tagged = 0
        total_inserted = 0

        for batch in self.fetcher.fetch_batches(
            batch_size=batch_size,
            limit_docs=limit_docs
        ):
            for doc in batch:
                total_seen += 1

                article_id = doc.get("article_id")
                if not article_id:
                    continue

                text = f"{doc.get('title','')}\n\n{doc.get('content','')}"
                companies = get_company_tags(text)

                if not companies:
                    continue

                total_tagged += 1

                try:
                    self.output_collection.insert_one({
                        "article_id": article_id,
                        "companies": companies,
                        "created_at": datetime.utcnow()
                    })
                    total_inserted += 1

                except errors.DuplicateKeyError:
                    # Already processed — skip safely
                    pass

            print(
                f"Seen: {total_seen}, "
                f"Tagged: {total_tagged}, "
                f"Inserted: {total_inserted}"
            )
