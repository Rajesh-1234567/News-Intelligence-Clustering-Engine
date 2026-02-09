# news_data_insights/clustering/cluster_utils.py

from datetime import datetime, timedelta
import uuid


def create_new_cluster(db, company, article_id):
    cluster_id = f"{company}_C{uuid.uuid4().hex[:6]}"

    doc = {
        "cluster_id": cluster_id,
        "company": company,
        "article_ids": [article_id],
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "expires_at": datetime.utcnow() + timedelta(hours=24),
    }

    db.company_clusters_active.insert_one(doc)
    return cluster_id


def update_existing_cluster(db, cluster_id, article_id):
    db.company_clusters_active.update_one(
        {"cluster_id": cluster_id},
        {
            "$addToSet": {"article_ids": article_id},
            "$set": {
                "updated_at": datetime.utcnow(),
                "expires_at": datetime.utcnow() + timedelta(hours=24),
            },
        },
    )
