import numpy as np
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity

from news_data_insights.clustering.config import (
    SIMILARITY_THRESHOLD,
    ACTIVE_WINDOW_HOURS,
)
from news_data_insights.clustering.cluster_utils import (
    create_new_cluster,
    update_existing_cluster,
)

EPS = 1 - SIMILARITY_THRESHOLD


def cluster_article(
    *,
    mongo_db,
    company,
    article_id,
    vector,
    company_vectors,   # list of (article_id, vector)
):
    """
    Cluster ONE article inside ONE company
    using local cosine similarity + DBSCAN.
    """

    # No previous articles → new cluster
    if not company_vectors:
        return create_new_cluster(mongo_db, company, article_id)

    vectors = [vector] + [v for _, v in company_vectors]
    article_ids = [article_id] + [aid for aid, _ in company_vectors]

    X = np.array(vectors)

    # DBSCAN with cosine distance
    labels = DBSCAN(
        eps=EPS,
        min_samples=2,
        metric="cosine",
    ).fit_predict(X)

    # If article is noise → new cluster
    if labels[0] == -1:
        return create_new_cluster(mongo_db, company, article_id)

    # Articles in same cluster as new article
    cluster_articles = [
        article_ids[i]
        for i, lbl in enumerate(labels)
        if lbl == labels[0]
    ]

    # Check if cluster already exists
    existing_cluster = mongo_db.company_clusters_active.find_one(
        {
            "company": company,
            "article_ids": {"$in": cluster_articles},
        }
    )

    if existing_cluster:
        update_existing_cluster(
            mongo_db,
            existing_cluster["cluster_id"],
            article_id,
        )
        return existing_cluster["cluster_id"]

    return create_new_cluster(mongo_db, company, article_id)
