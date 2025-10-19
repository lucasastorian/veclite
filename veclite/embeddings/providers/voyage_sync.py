import logging
from typing import List, Literal, Optional, Dict

from .voyage_base import BaseVoyageClient


class VoyageClient(BaseVoyageClient):
    """Sync implementation of Voyage client (for backwards compatibility)."""

    def __init__(self, model: str = "voyage-3.5-lite", dimensions: int = 512, cache: bool = True,
                 rerank_model: str = "rerank-2.5"):
        import voyageai

        super().__init__(model=model, dimensions=dimensions, cache=cache, rerank_model=rerank_model)

        self.token_rate_limiter = None
        self.request_rate_limiter = None

        self.client = voyageai.Client(api_key=self.api_key)

    def query_vector(self, query: str) -> List[float]:
        """Generates a single query vector

        Args:
            query: Query text

        Returns:
            Query embedding vector
        """
        # Check cache first
        if self.query_cache:
            cached = self.query_cache.get(query)
            if cached is not None:
                logging.debug(f"💾 Query cache hit: '{query[:50]}...'")
                return cached

        # Cache miss - generate embedding
        if self.query_cache:
            logging.debug(f"💾 Query cache miss: '{query[:50]}...'")

        result = self._embed(texts=[query], input_type="query")
        embedding = result[0]

        # Store in cache
        if self.query_cache:
            self.query_cache.set(query, embedding)

        return embedding

    def contextual_query_vector(self, query: str) -> List[float]:
        """Generates a contextual query vector

        Args:
            query: Query text

        Returns:
            Contextual query embedding vector
        """
        # Check cache first
        if self.contextualized_query_cache:
            cached = self.contextualized_query_cache.get(query)
            if cached is not None:
                logging.debug(f"💾 Contextualized query cache hit: '{query[:50]}...'")
                return cached

        # Cache miss - generate embedding
        if self.contextualized_query_cache:
            logging.debug(f"💾 Contextualized query cache miss: '{query[:50]}...'")

        result = self._contextualized_embed(inputs=[[query]], model="voyage-context-3",
                                           input_type="query", output_dimension=self.dimensions)
        embedding = result[0][0]

        # Store in cache
        if self.contextualized_query_cache:
            self.contextualized_query_cache.set(query, embedding)

        return embedding

    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generates a flat list of embeddings for all texts.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors
        """
        if not self.cache:
            logging.debug(f"📦 Batching {len(texts)} texts (cache disabled)")
            batches = self._batch_texts(texts=texts)
            logging.debug(f"📦 Split into {len(batches)} batches")

            all_embeddings = []
            for i, batch in enumerate(batches):
                logging.debug(f"📦 Processing batch {i+1}/{len(batches)} ({len(batch)} texts)")
                batch_embeddings = self._embed(batch, input_type="document")
                all_embeddings.extend(batch_embeddings)
            return all_embeddings

        cached = self.cache.get_many(texts)

        uncached_texts = []
        uncached_indices = []
        for i, (text, cached_emb) in enumerate(zip(texts, cached)):
            if cached_emb is None:
                uncached_texts.append(text)
                uncached_indices.append(i)

        if uncached_texts:
            logging.debug(f"💾 Cache miss: {len(uncached_texts)}/{len(texts)} texts need embedding")
            batches = self._batch_texts(texts=uncached_texts)
            logging.debug(f"📦 Split uncached texts into {len(batches)} batches")

            new_embeddings = []
            for i, batch in enumerate(batches):
                logging.debug(f"📦 Processing batch {i+1}/{len(batches)} ({len(batch)} texts)")
                batch_embeddings = self._embed(batch, input_type="document")
                new_embeddings.extend(batch_embeddings)

            self.cache.set_many(uncached_texts, new_embeddings)

        else:
            logging.debug(f"💾 Cache hit: {len(texts)}/{len(texts)} texts (no API calls needed)")
            new_embeddings = []

        results = cached[:]
        for idx, emb in zip(uncached_indices, new_embeddings):
            results[idx] = emb

        return results

    def count_tokens(self, texts: List[str], model: str = "voyage-3.5-lite") -> int:
        """Returns the number of tokens

        Args:
            texts: List of texts
            model: Model to use for token counting

        Returns:
            Total token count
        """
        return self.client.count_tokens(texts=texts, model=model)

    # @retry(
    #     stop=stop_after_attempt(3),
    #     wait=wait_exponential(multiplier=1, min=4, max=10),
    #     retry=retry_if_exception_type((
    #             voyageai.error.ServiceUnavailableError,
    #             voyageai.error.APIConnectionError,
    #             voyageai.error.RateLimitError,
    #             ConnectionError,
    #             TimeoutError
    #     ))
    # )
    def _embed(self, texts: List[str], input_type: Literal['document', 'query']) -> List[List[float]]:
        """Embeds a batch of texts with the Voyage API

        Args:
            texts: List of texts to embed
            input_type: Type of input ('document' or 'query')

        Returns:
            List of embedding vectors
        """
        import time

        estimated_tokens = self.client.count_tokens(texts, model=self.model)

        logging.debug(
            f"🚀 Voyage API call starting: {len(texts)} texts, "
            f"~{estimated_tokens} tokens, model={self.model}, type={input_type}"
        )

        try:
            t0 = time.time()

            # TODO: Add sync rate limiter context managers when available
            # For now, calling API directly without rate limiting
            response = self.client.embed(
                texts=texts,
                model=self.model,
                input_type=input_type,
                output_dimension=self.dimensions
            )

            elapsed = time.time() - t0
            actual_tokens = response.total_tokens

            logging.debug(
                f"✓ Voyage API call completed in {elapsed:.2f}s: "
                f"{len(response.embeddings)} embeddings, {actual_tokens} tokens"
            )

            return [embedding for embedding in response.embeddings]

        except voyageai.error.RateLimitError as e:
            logging.warning(f"Voyage API rate limit hit: {e}")
            raise

        except Exception as e:
            if "rate limit" in str(e).lower():
                logging.warning(f"Local rate limit hit: {e}")
            raise

    def _batch_texts(self, texts: List[str]) -> List[List[str]]:
        """Split a list of texts into batches respecting Voyage API limits.

        Args:
            texts: List of texts to batch

        Returns:
            List of text batches
        """
        batches = []
        current_batch = []
        current_tokens = 0

        for text in texts:
            text_tokens = self.client.count_tokens([text], model=self.model)

            batch_length_hit = len(current_batch) >= self.max_batch_size
            token_limit_hit = current_batch and current_tokens + text_tokens > self.max_tokens_per_request

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

    # @retry(
    #     stop=stop_after_attempt(3),
    #     wait=wait_exponential(multiplier=1, min=4, max=10),
    #     retry=retry_if_exception_type((
    #             voyageai.error.ServiceUnavailableError,
    #             voyageai.error.APIConnectionError,
    #             voyageai.error.RateLimitError,
    #             ConnectionError,
    #             TimeoutError
    #     ))
    # )
    def rerank(self, query: str, documents: List[str], top_k: Optional[int] = None, model: Optional[str] = None,
               truncation: bool = False) -> List[Dict]:
        """Rerank documents by relevance to query using Voyage rerank API.

        Args:
            query: Query text
            documents: List of documents to rerank
            top_k: Number of top results to return
            model: Rerank model to use
            truncation: Whether to truncate documents

        Returns:
            List of reranked results with index, document, and relevance_score
        """
        if not documents:
            return []

        if len(documents) > 1000:
            raise ValueError(f"Rerank API supports max 1000 documents, got {len(documents)}")

        model = model or self.rerank_model

        try:
            # TODO: Add sync rate limiter context manager when available
            response = self.client.rerank(
                query=query,
                documents=documents,
                model=model,
                top_k=top_k,
                truncation=truncation
            )

            results = []
            for r in response.results:
                results.append({
                    "index": r.index,
                    "document": r.document,
                    "relevance_score": r.relevance_score
                })

            return results

        except voyageai.error.RateLimitError as e:
            logging.warning(f"Voyage rerank API rate limit hit: {e}")
            raise
        except Exception as e:
            if "rate limit" in str(e).lower():
                logging.warning(f"Local rate limit hit during rerank: {e}")
            raise

    def contextualized_embed(self, inputs: List[List[str]], model: str = "voyage-context-3",
                            input_type: str = "document", output_dimension: int = 512) -> List[List[float]]:
        """Generate contextualized embeddings using voyage-context-3 model.

        Returns nested list of embeddings (one inner list per document).
        Each inner list (document) is cached independently.

        Args:
            inputs: List of documents, where each document is a list of chunk texts
            model: Contextualized embedding model to use
            input_type: "document" or "query" (optional)
            output_dimension: Embedding dimension

        Returns:
            Nested list of embeddings: List[List[float]] where each inner list
            contains embeddings for one document's chunks
        """
        if not inputs:
            return []

        if len(inputs) > 1000:
            raise ValueError(f"voyage-context-3 supports max 1000 documents, got {len(inputs)}")

        processed_inputs, doc_to_original = self._preprocess_contextualized_inputs(inputs)

        for i, doc_chunks in enumerate(processed_inputs):
            if len(doc_chunks) > 1000:
                raise ValueError(
                    f"Processed document {i} has {len(doc_chunks)} chunks. "
                    f"voyage-context-3 supports max 1000 chunks per document."
                )

        if not self.cache:
            response = self._contextualized_embed(processed_inputs, model, input_type, output_dimension)
            return self._merge_split_embeddings(response, doc_to_original, len(inputs))

        cached_results, uncached_inputs, uncached_indices = self._check_contextualized_cache(processed_inputs)

        if uncached_inputs:
            logging.debug(f"Contextualized cache miss: {len(uncached_inputs)}/{len(processed_inputs)} documents")

            new_embeddings = self._contextualized_embed(
                uncached_inputs, model, input_type, output_dimension
            )

            self._store_contextualized_cache(uncached_inputs, uncached_indices, new_embeddings, cached_results)
        else:
            logging.debug(f"Contextualized cache hit: {len(processed_inputs)}/{len(processed_inputs)} documents")

        return self._merge_split_embeddings(cached_results, doc_to_original, len(inputs))


    def _contextualized_embed(
            self,
            inputs: List[List[str]],
            model: str,
            input_type: str = "document",
            output_dimension: int = 512,
    ) -> List[List[List[float]]]:
        """Make Voyage API calls for contextualized embeddings serially.

        Args:
            inputs: List of documents to embed
            model: Model to use
            input_type: Type of input
            output_dimension: Embedding dimension

        Returns:
            Nested list of embeddings
        """
        batches = self._batch_contextualized_embed(inputs)
        results = []

        try:
            for i, batch in enumerate(batches):
                # TODO: Add sync rate limiter context manager when available
                response = self.client.contextualized_embed(
                    inputs=batch,
                    model=model,
                    input_type=input_type,
                    output_dimension=output_dimension,
                )

                batch_output = [[embedding for embedding in doc_result.embeddings]
                                for doc_result in response.results]
                results.extend(batch_output)

            return results

        except voyageai.error.RateLimitError as e:
            logging.warning(f"Voyage API rate limit hit: {e}")
            raise

        except Exception as e:
            if "rate limit" in str(e).lower():
                logging.warning(f"Local rate limit hit during contextualized embed: {e}")
            logging.exception("Contextualized embedding failed")
            raise
