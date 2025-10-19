"""Abstract base class for embedding providers."""

from abc import ABC, abstractmethod
from typing import List


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers.

    Subclasses must implement embed() and rerank() methods.
    This allows VecLite to support multiple embedding providers
    (Voyage, OpenAI, Cohere, etc.) with a consistent interface.
    """

    @abstractmethod
    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts.

        Args:
            texts: List of text strings to embed

        Returns:
            List of embedding vectors (each vector is a list of floats)
        """
        pass

    @abstractmethod
    async def rerank(self, query: str, documents: List[str], top_k: int = None) -> List[dict]:
        """Rerank documents by relevance to query.

        Args:
            query: Search query
            documents: List of document texts to rerank
            top_k: Optional number of top results to return

        Returns:
            List of dicts with 'index' and 'relevance_score' keys
        """
        pass

    @abstractmethod
    def close(self):
        """Close any open connections or resources."""
        pass
