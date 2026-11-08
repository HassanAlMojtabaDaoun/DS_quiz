from datetime import datetime, timezone
from uuid import uuid4

from coordinator import search_index as si


def test_document_from_row():
    row = {"bucket": "b", "object_key": "logs/2026/app-error.json", "version": 3,
           "size": 1234, "created_at": datetime(2026, 11, 8, tzinfo=timezone.utc),
           "content_type": "application/json", "etag": "abc", "sha256": "abc",
           "object_id": uuid4(), "chunking": "fixed",
           "tags": {"team": "ops", "env": "prod"}}
    doc = si.to_document(row)
    assert doc["key"] == "logs/2026/app-error.json"
    assert doc["tags"] == {"team": "ops", "env": "prod"}
    assert "team ops" in doc["tags_text"]
    assert si.doc_id("b", row["object_key"]) == "b/logs/2026/app-error.json"


def test_query_contains_all_filters():
    q = si.build_query(q="error log", bucket="b", prefix="logs/", min_size=10,
                       max_size=99, created_after="2026-01-01T00:00:00Z",
                       tags={"team": "ops"}, sort="size", order="asc", limit=5)
    must = q["query"]["bool"]["must"][0]["multi_match"]
    assert must["query"] == "error log" and must["fuzziness"] == "AUTO"
    filters = q["query"]["bool"]["filter"]
    assert {"term": {"bucket": "b"}} in filters
    assert {"prefix": {"key.raw": "logs/"}} in filters
    assert {"range": {"size": {"gte": 10, "lte": 99}}} in filters
    assert {"term": {"tags.team": "ops"}} in filters
    assert q["sort"][0] == {"size": {"order": "asc"}}
    assert q["size"] == 5 and q["highlight"]["encoder"] == "html"


def test_empty_query_matches_all():
    q = si.build_query()
    assert q["query"]["bool"]["must"] == [{"match_all": {}}]
    assert q["query"]["bool"]["filter"] == []


def test_format_results():
    body = {"took": 4, "hits": {"total": {"value": 1}, "hits": [
        {"_score": 2.5, "_source": {"bucket": "b", "key": "x"},
         "highlight": {"key": ["<mark>x</mark>"]}}]},
            "aggregations": {"buckets": {"buckets": [{"key": "b", "doc_count": 1}]},
                             "content_types": {"buckets": []},
                             "sizes": {"buckets": []}}}
    r = si.format_results(body)
    assert r["total"] == 1 and r["results"][0]["score"] == 2.5
    assert r["facets"]["buckets"] == [{"value": "b", "count": 1}]


def test_key_tokenizer_splits_paths():
    pattern = si.INDEX_BODY["settings"]["analysis"]["tokenizer"]["key_tokenizer"]["pattern"]
    import re
    assert [t for t in re.split(pattern, "logs/2026/app-error.json") if t] == \
        ["logs", "2026", "app", "error", "json"]
