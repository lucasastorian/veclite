import os
import logging
from typing import List, Optional
from veclite.utils.voyage_limits import VoyageLimits
from veclite.embeddings.cache import EmbeddingCache

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")


class BaseVoyageClient:
    """Base class for Voyage clients with shared configuration and utility methods."""

    max_batch_size: int = 1000
    max_batch_tokens: int = 100_000

    def __init__(self, model: str = "voyage-3.5-lite", dimensions: int = 512, cache: bool = True,
                 rerank_model: str = "rerank-2.5"):
        """Initialize shared configuration for Voyage clients.

        Args:
            model: Voyage embedding model to use
            dimensions: Embedding dimension size
            cache: Whether to enable caching
            rerank_model: Voyage rerank model to use
        """
        self.provider = "voyageai"
        self.model = model
        self.dimensions = dimensions
        self.rerank_model = rerank_model

        # API key validation
        self.api_key = os.environ.get("VOYAGE_API_KEY")
        assert self.api_key is not None, "VOYAGE_API_KEY environment variable must be set. Get your API key from https://www.voyageai.com/"

        # Setup limits
        self.limits = VoyageLimits(model)
        self.max_tokens_per_request = self.limits.max_tokens_per_request
        self.max_tokens_per_minute = self.limits.max_tokens_per_minute
        self.max_requests_per_minute = self.limits.max_requests_per_minute

        # Note: Rate limiters will be created by subclasses (async vs sync)
        # self.token_rate_limiter = ... (created by subclass)
        # self.request_rate_limiter = ... (created by subclass)

        # Setup cache
        self.cache = EmbeddingCache(model=model, dimensions=dimensions) if cache else None

        # Separate cache for contextualized embeddings (voyage-context-3)
        # Uses the contextualized dimension if different from main dimension
        self.contextualized_cache = EmbeddingCache(
            model="voyage-context-3",
            dimensions=dimensions  # Will match contextualized_dim
        ) if cache else None

        # Separate cache for query embeddings (input_type="query")
        # Stores query vectors separately from document vectors
        self.query_cache = EmbeddingCache(
            model=f"{model}_query",  # e.g., "voyage-3.5-lite_query"
            dimensions=dimensions
        ) if cache else None

        # Separate cache for contextualized query embeddings
        self.contextualized_query_cache = EmbeddingCache(
            model="voyage-context-3_query",
            dimensions=dimensions
        ) if cache else None

        # Note: Client will be created by subclasses (async vs sync)
        # self.client = ... (created by subclass)

    def _check_contextualized_cache(self, inputs: List[List[str]]) -> tuple:
        """Check cache for each document independently.

        Args:
            inputs: List of documents to check

        Returns:
            Tuple of (cached_results, uncached_inputs, uncached_indices)
        """
        cached_results = []
        uncached_inputs = []
        uncached_indices = []

        for i, document in enumerate(inputs):
            cache_key = self._compute_document_cache_key(document)
            cached_embeddings = self.contextualized_cache.get(cache_key) if self.contextualized_cache else None

            if cached_embeddings is None:
                # Cache miss - need to embed this document
                uncached_inputs.append(document)
                uncached_indices.append(i)
                cached_results.append(None)  # Placeholder
            else:
                # Cache hit - use cached embeddings
                cached_results.append(cached_embeddings)

        return cached_results, uncached_inputs, uncached_indices

    def _store_contextualized_cache(self, documents: List[List[str]], indices: List[int],
                                    embeddings: List[List[float]], results: List) -> None:
        """Store newly generated embeddings to cache and insert into results.

        Args:
            documents: List of documents
            indices: Indices of uncached documents
            embeddings: Generated embeddings
            results: Results list to update
        """
        for document, idx, doc_embeddings in zip(documents, indices, embeddings):
            cache_key = self._compute_document_cache_key(document)
            if self.contextualized_cache:
                self.contextualized_cache.set(cache_key, doc_embeddings)
            results[idx] = doc_embeddings

    def _count_tokens(self, texts: List[str], model: str = None) -> int:
        """Count tokens for a list of texts.

        Args:
            texts: List of text strings
            model: Model to use for token counting (defaults to self.model)

        Returns:
            Total token count
        """
        model = model or self.model
        return self.client.count_tokens(texts=texts, model=model)

    def _split_batch_by_tokens(self, texts: List[str], max_tokens: int) -> List[List[str]]:
        """Split texts into batches respecting token limits.

        Args:
            texts: List of text strings to batch
            max_tokens: Maximum tokens per batch

        Returns:
            List of text batches
        """
        batches = []
        current_batch = []
        current_tokens = 0

        for text in texts:
            text_tokens = self._count_tokens([text])

            # Check if we need to start a new batch
            batch_length_hit = len(current_batch) >= self.max_batch_size
            token_limit_hit = current_batch and current_tokens + text_tokens > max_tokens

            if batch_length_hit or token_limit_hit:
                if current_batch:
                    batches.append(current_batch)
                current_batch = [text]
                current_tokens = text_tokens
            else:
                current_batch.append(text)
                current_tokens += text_tokens

        if current_batch:
            batches.append(current_batch)

        return batches

    @staticmethod
    def _compute_document_cache_key(document: List[str]) -> str:
        """Compute cache key for a single document (list of chunk texts).

        Args:
            document: List of chunk texts

        Returns:
            SHA256 hash of the document
        """
        import hashlib
        import json

        canonical = json.dumps(document, sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def _preprocess_contextualized_inputs(self, inputs: List[List[str]], max_tokens: int = 32000) -> tuple:
        """Split documents exceeding max_tokens into smaller sub-documents.

        Args:
            inputs: List of documents (each document is a list of chunk texts)
            max_tokens: Maximum tokens per document (default 32k for voyage-context-3)

        Returns:
            Tuple of (processed_inputs, doc_to_original) where:
            - processed_inputs: List with oversized documents split into sub-documents
            - doc_to_original: List mapping each processed doc index to original doc index
        """
        import logging

        processed_inputs = []
        doc_to_original = []  # Maps each processed doc to its original index

        for i, document in enumerate(inputs):
            total_tokens = self.client.count_tokens(document, model="voyage-context-3")

            if total_tokens <= max_tokens:
                # Document fits, keep as-is
                processed_inputs.append(document)
                doc_to_original.append(i)
            else:
                # Split document by actively tracking tokens
                logging.info(f"Document {i} has {total_tokens} tokens, splitting into sub-documents")

                current_sub_doc = []
                current_tokens = 0

                for chunk in document:
                    chunk_tokens = self.client.count_tokens([chunk], model="voyage-context-3")

                    # Check if adding this chunk would exceed the limit
                    if current_tokens + chunk_tokens > max_tokens and current_sub_doc:
                        # Finalize current sub-document
                        processed_inputs.append(current_sub_doc)
                        doc_to_original.append(i)

                        # Start new sub-document with this chunk
                        current_sub_doc = [chunk]
                        current_tokens = chunk_tokens
                    else:
                        # Add chunk to current sub-document
                        current_sub_doc.append(chunk)
                        current_tokens += chunk_tokens

                # Don't forget the last sub-document
                if current_sub_doc:
                    processed_inputs.append(current_sub_doc)
                    doc_to_original.append(i)

        return processed_inputs, doc_to_original

    def _batch_contextualized_embed(self, inputs: List[List[str]]) -> List[List[List[str]]]:
        """Creates batches for contextualized embeddings.

        Args:
            inputs: List of documents to batch

        Returns:
            List of document batches
        """
        batches = []
        batch = []

        num_tokens: int = 0
        for document in inputs:
            doc_tokens = self.client.count_tokens(texts=document, model="voyage-context-3")

            if num_tokens + doc_tokens > 64_000 and batch:
                batches.append(batch)
                num_tokens = 0
                batch = []

            batch.append(document)
            num_tokens += doc_tokens

        if batch:
            batches.append(batch)

        return batches

    @staticmethod
    def _merge_split_embeddings(embeddings: List[List[float]], doc_to_original: List[int],
                                num_original_docs: int) -> List[List[float]]:
        """Merge embeddings from split documents back into original structure.

        Args:
            embeddings: List of embedding lists (one per processed document)
            doc_to_original: Maps each processed doc index to its original doc index
            num_original_docs: Number of documents in the original input

        Returns:
            List of embedding lists matching the original input structure
        """
        # Group embeddings by original document index
        merged_results = [[] for _ in range(num_original_docs)]

        for processed_idx, original_idx in enumerate(doc_to_original):
            merged_results[original_idx].extend(embeddings[processed_idx])

        return merged_results
