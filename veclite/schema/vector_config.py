"""Vector configuration for embedding fields."""
from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class VectorConfig:
    """Configuration for regular vector embeddings on a field.

    For contextualized embeddings, use contextualized=True on the field instead.
    Currently only supports Voyage AI as the embedding provider.

    Attributes:
        model: Voyage AI model name (e.g., "voyage-3.5-lite", "voyage-3")
        dimensions: Embedding dimensions (e.g., 512, 1024, 1536)
    """
    model: str
    dimensions: int

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'model': self.model,
            'dimensions': self.dimensions,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VectorConfig':
        """Deserialize from dictionary."""
        return cls(
            model=data['model'],
            dimensions=data['dimensions'],
        )

    @classmethod
    def voyage_lite(cls) -> 'VectorConfig':
        """Voyage 3.5 Lite - 512 dimensions."""
        return cls(model="voyage-3.5-lite", dimensions=512)

    @classmethod
    def voyage_3(cls) -> 'VectorConfig':
        """Voyage 3 - 1024 dimensions."""
        return cls(model="voyage-3", dimensions=1024)

    @classmethod
    def voyage_large(cls) -> 'VectorConfig':
        """Voyage 3.5 Large - 1536 dimensions."""
        return cls(model="voyage-3.5-large", dimensions=1536)

    @classmethod
    def mock(cls, dimensions: int = 64) -> 'VectorConfig':
        """Create config for testing with mock embedder."""
        return cls(model="mock", dimensions=dimensions)
