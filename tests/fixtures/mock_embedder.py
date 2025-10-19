"""Mock embedder for testing vector search without API calls."""
from typing import List
import numpy as np


def make_vector(dim: int, *values: float) -> List[float]:
    """Create a dim-dimensional vector with specified leading values, rest zeros.

    Args:
        dim: Dimension of the vector
        *values: Leading values (rest will be zeros)

    Returns:
        List of floats representing the vector
    """
    vec = [0.0] * dim
    for i, val in enumerate(values[:dim]):
        vec[i] = val
    return vec


def make_random_vector(dim: int, seed: int) -> List[float]:
    """Create a deterministic random unit vector with given seed.

    Args:
        dim: Dimension of the vector
        seed: Random seed for reproducibility

    Returns:
        Normalized vector (unit length) as list of floats
    """
    rng = np.random.RandomState(seed)
    vec = rng.randn(dim)
    # Normalize to unit length for cosine similarity
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec.tolist()


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Calculate cosine similarity between two vectors.

    Assumes vectors are already normalized to unit length.

    Args:
        vec1: First vector
        vec2: Second vector

    Returns:
        Cosine similarity (dot product for unit vectors)
    """
    return float(np.dot(vec1, vec2))


class MockEmbedder:
    """Mock embedder that returns predefined vectors for testing."""

    def __init__(self, dimension: int = 3):
        """Initialize mock embedder.

        Args:
            dimension: Dimension of the embedding vectors
        """
        self.dimension = dimension
        self.mock_embeddings = {}
        self.cache = None  # No caching for mock embedder

    def add_mock(self, text: str, vector: List[float]):
        """Add a predefined embedding for a text.

        Args:
            text: The text to mock
            vector: The vector to return for this text
        """
        if len(vector) != self.dimension:
            raise ValueError(f"Vector must have {self.dimension} dimensions, got {len(vector)}")
        self.mock_embeddings[text] = vector

    def embed(self, texts: List[str]) -> List[List[float]]:
        """Return mock embeddings for texts.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors
        """
        result = []
        for text in texts:
            if text in self.mock_embeddings:
                result.append(self.mock_embeddings[text])
            else:
                # Return a default vector if not mocked
                result.append([0.0] * self.dimension)
        return result

    def query_vector(self, query: str) -> List[float]:
        """Return mock embedding for a single query.

        Args:
            query: Query text to embed

        Returns:
            Embedding vector
        """
        if query in self.mock_embeddings:
            return self.mock_embeddings[query]
        return [0.0] * self.dimension

    def contextualized_embed(self, inputs: List[List[str]], **kwargs) -> List[List[List[float]]]:
        """Return mock contextualized embeddings.

        Args:
            inputs: List of documents, where each document is a list of text chunks
            **kwargs: Ignored (for compatibility with real embedder)

        Returns:
            List of documents, where each document is a list of embedding vectors
        """
        result = []
        for document in inputs:
            doc_embeddings = []
            for text in document:
                if text in self.mock_embeddings:
                    doc_embeddings.append(self.mock_embeddings[text])
                else:
                    doc_embeddings.append([0.0] * self.dimension)
            result.append(doc_embeddings)
        return result


class MockAsyncEmbedder:
    """Mock async embedder that returns predefined vectors for testing."""

    def __init__(self, dimension: int = 3):
        """Initialize mock embedder.

        Args:
            dimension: Dimension of the embedding vectors
        """
        self.dimension = dimension
        self.mock_embeddings = {}
        self.cache = None  # No caching for mock embedder

    def add_mock(self, text: str, vector: List[float]):
        """Add a predefined embedding for a text.

        Args:
            text: The text to mock
            vector: The vector to return for this text
        """
        if len(vector) != self.dimension:
            raise ValueError(f"Vector must have {self.dimension} dimensions, got {len(vector)}")
        self.mock_embeddings[text] = vector

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Return mock embeddings for texts.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors
        """
        result = []
        for text in texts:
            if text in self.mock_embeddings:
                result.append(self.mock_embeddings[text])
            else:
                # Return a default vector if not mocked
                result.append([0.0] * self.dimension)
        return result

    async def query_vector(self, query: str) -> List[float]:
        """Return mock embedding for a single query.

        Args:
            query: Query text to embed

        Returns:
            Embedding vector
        """
        if query in self.mock_embeddings:
            return self.mock_embeddings[query]
        return [0.0] * self.dimension

    async def contextualized_embed(self, inputs: List[List[str]], **kwargs) -> List[List[List[float]]]:
        """Return mock contextualized embeddings.

        Args:
            inputs: List of documents, where each document is a list of text chunks
            **kwargs: Ignored (for compatibility with real embedder)

        Returns:
            List of documents, where each document is a list of embedding vectors
        """
        result = []
        for document in inputs:
            doc_embeddings = []
            for text in document:
                if text in self.mock_embeddings:
                    doc_embeddings.append(self.mock_embeddings[text])
                else:
                    doc_embeddings.append([0.0] * self.dimension)
            result.append(doc_embeddings)
        return result
