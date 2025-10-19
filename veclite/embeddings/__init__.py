"""Embedding providers for VecLite."""

from veclite.embeddings.providers.voyage_sync import VoyageClient
from veclite.embeddings.providers.voyage_async import AsyncVoyageClient

# Export with both naming conventions for backwards compatibility
VoyageEmbedder = VoyageClient
AsyncVoyageEmbedder = AsyncVoyageClient

__all__ = [
    "VoyageClient",
    "AsyncVoyageClient",
    "VoyageEmbedder",
    "AsyncVoyageEmbedder",
]
