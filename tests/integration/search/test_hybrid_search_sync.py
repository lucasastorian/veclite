"""Test sync hybrid search operations."""
from tests.fixtures.mock_embedder import make_random_vector, cosine_similarity


def test_hybrid_search_basic(sync_document_client, mock_embedder):
    """Test basic hybrid search combining vector and keyword."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    doc2_vec = make_random_vector(64, seed=2)
    doc3_vec = make_random_vector(64, seed=3)
    query_vec = make_random_vector(64, seed=10)

    # Setup mock embeddings
    mock_embedder.add_mock("Python programming", doc1_vec)
    mock_embedder.add_mock("Java development", doc2_vec)
    mock_embedder.add_mock("Python guide", doc3_vec)
    mock_embedder.add_mock("query", query_vec)

    # Insert documents
    sync_document_client.table("documents").insert([
        {"title": "Doc 1", "content": "Python programming", "category": "tutorial"},
        {"title": "Doc 2", "content": "Java development", "category": "tutorial"},
        {"title": "Doc 3", "content": "Python guide", "category": "guide"},
    ]).execute()

    # Hybrid search combines vector similarity + keyword matching
    results = sync_document_client.table("documents").hybrid_search("query", topk=3).execute()

    assert len(results.data) == 3
    # All documents should have scores
    assert all("_score" in row for row in results.data)
    # Scores should be descending
    for i in range(len(results.data) - 1):
        assert results.data[i]["_score"] >= results.data[i + 1]["_score"]


def test_hybrid_search_with_filters(sync_document_client, mock_embedder):
    """Test hybrid search with predicates."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    doc2_vec = make_random_vector(64, seed=2)
    doc3_vec = make_random_vector(64, seed=3)
    query_vec = make_random_vector(64, seed=10)

    # Setup embeddings
    mock_embedder.add_mock("doc1", doc1_vec)
    mock_embedder.add_mock("doc2", doc2_vec)
    mock_embedder.add_mock("doc3", doc3_vec)
    mock_embedder.add_mock("query", query_vec)

    sync_document_client.table("documents").insert([
        {"title": "Doc 1", "content": "doc1", "category": "exclude"},
        {"title": "Doc 2", "content": "doc2", "category": "include"},
        {"title": "Doc 3", "content": "doc3", "category": "include"},
    ]).execute()

    # Search with filter
    results = sync_document_client.table("documents").hybrid_search("query", topk=3) \
        .eq("category", "include") \
        .execute()

    assert len(results.data) == 2
    titles = {row["title"] for row in results.data}
    assert "Doc 1" not in titles
    assert "Doc 2" in titles
    assert "Doc 3" in titles


def test_hybrid_search_alpha_weighting(sync_document_client, mock_embedder):
    """Test that alpha parameter affects vector vs keyword weighting."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    doc2_vec = make_random_vector(64, seed=2)
    query_vec = make_random_vector(64, seed=10)

    # Setup embeddings
    mock_embedder.add_mock("Python programming tutorial", doc1_vec)
    mock_embedder.add_mock("Java development guide", doc2_vec)
    mock_embedder.add_mock("query", query_vec)

    sync_document_client.table("documents").insert([
        {"title": "Doc 1", "content": "Python programming tutorial"},
        {"title": "Doc 2", "content": "Java development guide"},
    ]).execute()

    # High alpha = more vector weight
    results_high_alpha = sync_document_client.table("documents").hybrid_search(
        "query",
        topk=2,
        alpha=0.9  # 90% vector, 10% keyword
    ).execute()

    assert len(results_high_alpha.data) == 2
    # Verify scores are assigned
    assert all("_score" in row for row in results_high_alpha.data)


def test_hybrid_search_explicit_columns(sync_document_client, mock_embedder):
    """Test hybrid search with explicit column specification."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    query_vec = make_random_vector(64, seed=10)

    mock_embedder.add_mock("doc1", doc1_vec)
    mock_embedder.add_mock("query", query_vec)

    sync_document_client.table("documents").insert([
        {"title": "Doc 1", "content": "doc1"},
    ]).execute()

    # Specify both vector and keyword columns explicitly
    results = sync_document_client.table("documents").hybrid_search(
        "query",
        vector_column="content",
        keyword_column="content",
        topk=1
    ).execute()

    assert len(results.data) == 1
    assert results.data[0]["id"] == 1


def test_hybrid_search_topk_limit(sync_document_client, mock_embedder):
    """Test that topk limits hybrid search results."""
    # Generate vectors for 10 docs
    vecs = [make_random_vector(64, seed=i) for i in range(10)]
    query_vec = make_random_vector(64, seed=100)

    # Setup embeddings
    for i, vec in enumerate(vecs):
        mock_embedder.add_mock(f"doc{i}", vec)
    mock_embedder.add_mock("query", query_vec)

    docs = [{"title": f"Doc {i}", "content": f"doc{i}"} for i in range(10)]
    sync_document_client.table("documents").insert(docs).execute()

    # Search with topk=5
    results = sync_document_client.table("documents").hybrid_search("query", topk=5).execute()

    assert len(results.data) == 5
    # Verify scores are descending
    for i in range(len(results.data) - 1):
        assert results.data[i]["_score"] >= results.data[i + 1]["_score"]


def test_hybrid_search_with_null_filter(sync_document_client, mock_embedder):
    """Test hybrid search with is_null predicate."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    doc2_vec = make_random_vector(64, seed=2)
    query_vec = make_random_vector(64, seed=10)

    mock_embedder.add_mock("doc1", doc1_vec)
    mock_embedder.add_mock("doc2", doc2_vec)
    mock_embedder.add_mock("query", query_vec)

    sync_document_client.table("documents").insert([
        {"title": "Doc 1", "content": "doc1", "category": "a"},
        {"title": "Doc 2", "content": "doc2", "category": None},
    ]).execute()

    # Search with NULL filter
    results = sync_document_client.table("documents").hybrid_search("query", topk=10) \
        .is_null("category") \
        .execute()

    assert len(results.data) == 1
    assert results.data[0]["title"] == "Doc 2"


def test_hybrid_search_no_results_with_filter(sync_document_client, mock_embedder):
    """Test hybrid search when filter excludes all results."""
    # Generate vectors
    doc1_vec = make_random_vector(64, seed=1)
    query_vec = make_random_vector(64, seed=10)

    mock_embedder.add_mock("doc1", doc1_vec)
    mock_embedder.add_mock("query", query_vec)

    sync_document_client.table("documents").insert([
        {"title": "Doc 1", "content": "doc1", "category": "a"},
    ]).execute()

    # Filter that excludes everything
    results = sync_document_client.table("documents").hybrid_search("query", topk=10) \
        .eq("category", "nonexistent") \
        .execute()

    assert len(results.data) == 0
