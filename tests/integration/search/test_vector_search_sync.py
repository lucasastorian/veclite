"""Test vector search with sync client."""
import pytest
from tests.fixtures.mock_embedder import make_random_vector, cosine_similarity


def test_vector_search_basic(sync_document_client, mock_embedder):
    """Test basic vector search with seeded random vectors."""
    # Generate deterministic vectors
    doc1_vec = make_random_vector(64, seed=1)
    doc2_vec = make_random_vector(64, seed=2)
    doc3_vec = make_random_vector(64, seed=3)
    query_vec = make_random_vector(64, seed=10)

    # Calculate ground truth similarities
    sim1 = cosine_similarity(doc1_vec, query_vec)
    sim2 = cosine_similarity(doc2_vec, query_vec)
    sim3 = cosine_similarity(doc3_vec, query_vec)

    # Add mock embeddings
    mock_embedder.add_mock("Python programming", doc1_vec)
    mock_embedder.add_mock("JavaScript tutorial", doc2_vec)
    mock_embedder.add_mock("Database design", doc3_vec)
    mock_embedder.add_mock("learning Python", query_vec)

    # Insert documents
    sync_document_client.table("documents").insert([
        {"title": "Doc 1", "content": "Python programming"},
        {"title": "Doc 2", "content": "JavaScript tutorial"},
        {"title": "Doc 3", "content": "Database design"},
    ]).execute()

    # Search
    result = sync_document_client.table("documents").vector_search(
        query="learning Python",
        topk=3
    ).execute()

    # Verify results ordered by similarity
    assert len(result.data) == 3

    # Sort expected by similarity descending
    expected_order = sorted(
        [(1, sim1), (2, sim2), (3, sim3)],
        key=lambda x: x[1],
        reverse=True
    )

    for i, (expected_id, expected_sim) in enumerate(expected_order):
        assert result.data[i]["id"] == expected_id
        assert abs(result.data[i]["_score"] - expected_sim) < 1e-6


def test_vector_search_with_filters(sync_document_client, mock_embedder):
    """Test vector search with WHERE filters."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    doc2_vec = make_random_vector(64, seed=2)
    doc3_vec = make_random_vector(64, seed=3)
    query_vec = make_random_vector(64, seed=10)

    # Calculate similarities
    sim1 = cosine_similarity(doc1_vec, query_vec)
    sim3 = cosine_similarity(doc3_vec, query_vec)

    # Add embeddings
    mock_embedder.add_mock("doc1", doc1_vec)
    mock_embedder.add_mock("doc2", doc2_vec)
    mock_embedder.add_mock("doc3", doc3_vec)
    mock_embedder.add_mock("query", query_vec)

    # Insert documents with categories
    sync_document_client.table("documents").insert([
        {"title": "Title 1", "content": "doc1", "category": "tech"},
        {"title": "Title 2", "content": "doc2", "category": "science"},
        {"title": "Title 3", "content": "doc3", "category": "tech"},
    ]).execute()

    # Search with category filter
    result = sync_document_client.table("documents").vector_search(
        query="query",
        topk=10
    ).eq("category", "tech").execute()

    # Should only return tech docs
    assert len(result.data) == 2

    # Verify ordering
    if sim1 > sim3:
        assert result.data[0]["id"] == 1
        assert result.data[1]["id"] == 3
        assert abs(result.data[0]["_score"] - sim1) < 1e-6
        assert abs(result.data[1]["_score"] - sim3) < 1e-6
    else:
        assert result.data[0]["id"] == 3
        assert result.data[1]["id"] == 1
        assert abs(result.data[0]["_score"] - sim3) < 1e-6
        assert abs(result.data[1]["_score"] - sim1) < 1e-6


def test_vector_search_topk_limit(sync_document_client, mock_embedder):
    """Test topk parameter limits results."""
    # Generate 10 documents
    vecs = [make_random_vector(64, seed=i) for i in range(10)]
    query_vec = make_random_vector(64, seed=100)

    # Add embeddings
    for i, vec in enumerate(vecs):
        mock_embedder.add_mock(f"doc{i}", vec)
    mock_embedder.add_mock("query", query_vec)

    # Insert documents
    docs = [{"title": f"Title {i}", "content": f"doc{i}"} for i in range(10)]
    sync_document_client.table("documents").insert(docs).execute()

    # Search with topk=3
    result = sync_document_client.table("documents").vector_search(
        query="query",
        topk=3
    ).execute()

    # Should return exactly 3 results
    assert len(result.data) == 3

    # Calculate all similarities and get top 3
    similarities = [(i, cosine_similarity(vecs[i], query_vec)) for i in range(10)]
    top3 = sorted(similarities, key=lambda x: x[1], reverse=True)[:3]

    # Verify we got the top 3
    result_ids = {r["id"] for r in result.data}
    expected_ids = {idx + 1 for idx, _ in top3}  # +1 because IDs are 1-indexed
    assert result_ids == expected_ids


def test_vector_search_with_multiple_filters(sync_document_client, mock_embedder):
    """Test vector search with multiple WHERE conditions."""
    # Generate vectors
    vecs = [make_random_vector(64, seed=i) for i in range(5)]
    query_vec = make_random_vector(64, seed=100)

    # Add embeddings
    for i, vec in enumerate(vecs):
        mock_embedder.add_mock(f"doc{i}", vec)
    mock_embedder.add_mock("query", query_vec)

    # Insert documents with varying categories and titles
    sync_document_client.table("documents").insert([
        {"title": "Python Guide", "content": "doc0", "category": "tech"},
        {"title": "Java Tutorial", "content": "doc1", "category": "tech"},
        {"title": "Science Book", "content": "doc2", "category": "science"},
        {"title": "Python Advanced", "content": "doc3", "category": "tech"},
        {"title": "Biology", "content": "doc4", "category": "science"},
    ]).execute()

    # Search with category=tech
    result = sync_document_client.table("documents").vector_search(
        query="query",
        topk=10
    ).eq("category", "tech").execute()

    # Should return 3 tech docs
    assert len(result.data) == 3
    for row in result.data:
        assert row["category"] == "tech"

    # Calculate expected order for tech docs (indices 0, 1, 3)
    tech_sims = [
        (1, cosine_similarity(vecs[0], query_vec)),
        (2, cosine_similarity(vecs[1], query_vec)),
        (4, cosine_similarity(vecs[3], query_vec)),
    ]
    tech_sims.sort(key=lambda x: x[1], reverse=True)

    for i, (expected_id, expected_sim) in enumerate(tech_sims):
        assert result.data[i]["id"] == expected_id
        assert abs(result.data[i]["_score"] - expected_sim) < 1e-6


def test_vector_search_with_null_category_filter(sync_document_client, mock_embedder):
    """Test vector search with IS NULL filter."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    doc2_vec = make_random_vector(64, seed=2)
    doc3_vec = make_random_vector(64, seed=3)
    query_vec = make_random_vector(64, seed=10)

    sim1 = cosine_similarity(doc1_vec, query_vec)
    sim3 = cosine_similarity(doc3_vec, query_vec)

    # Add embeddings
    mock_embedder.add_mock("doc1", doc1_vec)
    mock_embedder.add_mock("doc2", doc2_vec)
    mock_embedder.add_mock("doc3", doc3_vec)
    mock_embedder.add_mock("query", query_vec)

    # Insert documents - some with null category
    sync_document_client.table("documents").insert([
        {"title": "Title 1", "content": "doc1", "category": None},
        {"title": "Title 2", "content": "doc2", "category": "tech"},
        {"title": "Title 3", "content": "doc3", "category": None},
    ]).execute()

    # Search for documents with null category
    result = sync_document_client.table("documents").vector_search(
        query="query",
        topk=10
    ).is_null("category").execute()

    # Should return 2 docs with null category
    assert len(result.data) == 2

    # Verify ordering by similarity
    if sim1 > sim3:
        assert result.data[0]["id"] == 1
        assert result.data[1]["id"] == 3
    else:
        assert result.data[0]["id"] == 3
        assert result.data[1]["id"] == 1


def test_vector_search_no_results_with_filter(sync_document_client, mock_embedder):
    """Test vector search returns empty when filter matches nothing."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    query_vec = make_random_vector(64, seed=10)

    mock_embedder.add_mock("doc1", doc1_vec)
    mock_embedder.add_mock("query", query_vec)

    # Insert documents
    sync_document_client.table("documents").insert([
        {"title": "Title 1", "content": "doc1", "category": "tech"},
    ]).execute()

    # Search with non-matching filter
    result = sync_document_client.table("documents").vector_search(
        query="query",
        topk=10
    ).eq("category", "science").execute()

    assert len(result.data) == 0


def test_vector_search_explicit_column(sync_document_client, mock_embedder):
    """Test vector search with explicit column specification."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    query_vec = make_random_vector(64, seed=10)

    sim1 = cosine_similarity(doc1_vec, query_vec)

    mock_embedder.add_mock("doc1", doc1_vec)
    mock_embedder.add_mock("query", query_vec)

    # Insert document
    sync_document_client.table("documents").insert([
        {"title": "Title 1", "content": "doc1"},
    ]).execute()

    # Search with explicit column
    result = sync_document_client.table("documents").vector_search(
        query="query",
        column="content",
        topk=1
    ).execute()

    assert len(result.data) == 1
    assert result.data[0]["id"] == 1
    assert abs(result.data[0]["_score"] - sim1) < 1e-6


def test_vector_search_ordering_by_similarity(sync_document_client, mock_embedder):
    """Test that results are correctly ordered by cosine similarity."""
    # Generate vectors with known similarities
    # We'll create vectors at different angles to the query
    query_vec = make_random_vector(64, seed=100)

    # Generate diverse vectors
    vecs = [make_random_vector(64, seed=i) for i in range(5)]

    # Add embeddings
    for i, vec in enumerate(vecs):
        mock_embedder.add_mock(f"doc{i}", vec)
    mock_embedder.add_mock("query", query_vec)

    # Insert documents
    docs = [{"title": f"Title {i}", "content": f"doc{i}"} for i in range(5)]
    sync_document_client.table("documents").insert(docs).execute()

    # Search
    result = sync_document_client.table("documents").vector_search(
        query="query",
        topk=5
    ).execute()

    # Verify results are ordered by similarity (descending)
    assert len(result.data) == 5

    # Check that _score is monotonically decreasing
    for i in range(len(result.data) - 1):
        assert result.data[i]["_score"] >= result.data[i + 1]["_score"]

    # Verify against ground truth
    similarities = [(i, cosine_similarity(vecs[i], query_vec)) for i in range(5)]
    similarities.sort(key=lambda x: x[1], reverse=True)

    for i, (expected_idx, expected_sim) in enumerate(similarities):
        assert result.data[i]["id"] == expected_idx + 1  # IDs are 1-indexed
        assert abs(result.data[i]["_score"] - expected_sim) < 1e-6
