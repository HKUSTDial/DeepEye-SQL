from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
import numpy as np
from chromadb.api.types import Documents, Embeddings


class QwenEmbeddingFunction(SentenceTransformerEmbeddingFunction):
    
    """
    A wrapper of the SentenceTransformerEmbeddingFunction for Qwen embedding model.
    It uses the query prompt for the embedding.
    """
    
    def __call__(self, input: Documents) -> Embeddings:
        """Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        embeddings = self._model.encode(
            list(input),
            prompt_name="query",
            convert_to_numpy=True,
            normalize_embeddings=self.normalize_embeddings,
        )

        return [np.array(embedding, dtype=np.float32) for embedding in embeddings]