from dotenv import load_dotenv
load_dotenv(".env")

from pymongo import MongoClient
from news_data_insights.config import MongoConfig
from news_data_insights.insights.insight_pipeline import run_company_insight

cfg = MongoConfig()
mongo = MongoClient(cfg.uri)
db = mongo[cfg.db]

def run_all_company_insights(limit: int | None = None):
    companies = db.company_clusters_active.distinct("company")
    for i, company in enumerate(companies):
        if limit and i >= limit:
            break
        run_company_insight(company)

if __name__ == "__main__":
    run_all_company_insights(limit=None)
