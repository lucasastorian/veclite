"""Test async keyword search (FTS5/BM25) operations."""
import pytest


@pytest.mark.asyncio
async def test_keyword_search_basic(async_article_client):
    """Test basic keyword search."""
    # Insert test articles
    await async_article_client.table("articles").insert([
        {"title": "Python Basics", "content": "Python is a programming language", "category": "programming"},
        {"title": "Database Design", "content": "SQL databases store structured data", "category": "database"},
        {"title": "Python Advanced", "content": "Advanced Python programming techniques", "category": "programming"},
    ]).execute()

    # Search for "Python"
    results = await async_article_client.table("articles").keyword_search("Python").execute()

    assert len(results.data) == 2
    # Both Python articles should be returned
    titles = {row["title"] for row in results.data}
    assert "Python Basics" in titles
    assert "Python Advanced" in titles


@pytest.mark.asyncio
async def test_keyword_search_with_column_specified(async_article_client):
    """Test keyword search with explicit column."""
    await async_article_client.table("articles").insert([
        {"title": "Python Guide", "content": "Learn Python programming", "category": "tutorial"},
        {"title": "Java Guide", "content": "Learn Java programming", "category": "tutorial"},
    ]).execute()

    # Search in content column only
    results = await async_article_client.table("articles").keyword_search("Python", column="content").execute()

    assert len(results.data) == 1
    assert results.data[0]["title"] == "Python Guide"


@pytest.mark.asyncio
async def test_keyword_search_with_filters(async_article_client):
    """Test keyword search combined with predicates."""
    await async_article_client.table("articles").insert([
        {"title": "Machine Learning", "content": "ML algorithms and data science", "category": "ai", "views": 100},
        {"title": "Deep Learning", "content": "Neural networks and deep learning", "category": "ai", "views": 50},
        {"title": "Data Science", "content": "Data analysis and machine learning", "category": "data", "views": 75},
    ]).execute()

    # Search for "neural" with category filter (only in Deep Learning article)
    results = await async_article_client.table("articles").keyword_search("neural") \
        .eq("category", "ai") \
        .execute()

    assert len(results.data) == 1
    assert results.data[0]["title"] == "Deep Learning"


@pytest.mark.asyncio
async def test_keyword_search_with_comparison_filters(async_article_client):
    """Test keyword search with comparison predicates."""
    await async_article_client.table("articles").insert([
        {"title": "Popular Python", "content": "Python programming guide", "category": "programming", "views": 1000},
        {"title": "Python Basics", "content": "Introduction to Python", "category": "programming", "views": 500},
        {"title": "Python Advanced", "content": "Advanced Python concepts", "category": "programming", "views": 2000},
    ]).execute()

    # Search for Python articles with > 600 views
    results = await async_article_client.table("articles").keyword_search("Python") \
        .gt("views", 600) \
        .execute()

    assert len(results.data) == 2
    views = {row["views"] for row in results.data}
    assert 1000 in views
    assert 2000 in views


@pytest.mark.asyncio
async def test_keyword_search_no_results(async_article_client):
    """Test keyword search with no matches."""
    await async_article_client.table("articles").insert([
        {"title": "Python Guide", "content": "Learn Python programming", "category": "tutorial"},
    ]).execute()

    results = await async_article_client.table("articles").keyword_search("Rust").execute()

    assert len(results.data) == 0


@pytest.mark.asyncio
async def test_keyword_search_phrase_query(async_article_client):
    """Test keyword search with phrase."""
    await async_article_client.table("articles").insert([
        {"title": "ML Guide", "content": "Machine learning algorithms", "category": "ai"},
        {"title": "AI Guide", "content": "Artificial intelligence and learning machines", "category": "ai"},
    ]).execute()

    # Search for a specific term present in only one document
    results = await async_article_client.table("articles").keyword_search("algorithms").execute()

    assert len(results.data) == 1
    assert "algorithms" in results.data[0]["content"]


@pytest.mark.asyncio
async def test_keyword_search_multiple_filters(async_article_client):
    """Test keyword search with multiple chained predicates."""
    await async_article_client.table("articles").insert([
        {"title": "Python for Beginners", "content": "Learn Python programming", "category": "tutorial", "views": 1000},
        {"title": "Python Advanced", "content": "Advanced Python techniques", "category": "advanced", "views": 500},
        {"title": "Python Expert", "content": "Expert Python programming", "category": "tutorial", "views": 1500},
    ]).execute()

    # Search with multiple filters
    results = await async_article_client.table("articles").keyword_search("Python") \
        .eq("category", "tutorial") \
        .gte("views", 1000) \
        .execute()

    assert len(results.data) == 2
    for row in results.data:
        assert row["category"] == "tutorial"
        assert row["views"] >= 1000


@pytest.mark.asyncio
async def test_keyword_search_with_null_filter(async_article_client):
    """Test keyword search with NULL checks."""
    await async_article_client.table("articles").insert([
        {"title": "Python Guide", "content": "Learn Python", "category": "tutorial"},
        {"title": "Python Basics", "content": "Python fundamentals", "category": None},
    ]).execute()

    # Search with NULL filter
    results = await async_article_client.table("articles").keyword_search("Python") \
        .is_null("category") \
        .execute()

    assert len(results.data) == 1
    assert results.data[0]["title"] == "Python Basics"
