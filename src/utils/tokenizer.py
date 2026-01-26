"""
Tokenization utilities for contract text processing.

Uses NLTK for word tokenization, sentence splitting, and n-gram extraction.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Flag to track if NLTK data has been downloaded
_nltk_initialized = False


def _ensure_nltk_data():
    """Ensure required NLTK data is available."""
    global _nltk_initialized
    if _nltk_initialized:
        return

    try:
        import nltk

        # Try to use the tokenizers, download if missing
        try:
            nltk.data.find("tokenizers/punkt")
        except LookupError:
            logger.info("Downloading NLTK punkt tokenizer data...")
            nltk.download("punkt", quiet=True)

        try:
            nltk.data.find("tokenizers/punkt_tab")
        except LookupError:
            logger.info("Downloading NLTK punkt_tab tokenizer data...")
            nltk.download("punkt_tab", quiet=True)

        _nltk_initialized = True

    except ImportError as e:
        raise ImportError(
            "NLTK is required for tokenization. Install with: pip install nltk"
        ) from e


@dataclass
class TokenizationResult:
    """Result of tokenizing text."""

    tokens: list[str] = field(default_factory=list)
    token_count: int = 0
    sentences: list[str] = field(default_factory=list)
    sentence_count: int = 0
    bigrams: list[str] = field(default_factory=list)
    trigrams: list[str] = field(default_factory=list)

    # Statistics
    unique_tokens: int = 0
    avg_token_length: float = 0.0
    avg_sentence_length: float = 0.0

    def compute_stats(self) -> None:
        """Compute statistics after tokenization."""
        self.token_count = len(self.tokens)
        self.sentence_count = len(self.sentences)
        self.unique_tokens = len(set(self.tokens))

        if self.tokens:
            self.avg_token_length = sum(len(t) for t in self.tokens) / len(self.tokens)

        if self.sentences:
            # Average tokens per sentence
            sentence_lengths = [len(s.split()) for s in self.sentences]
            self.avg_sentence_length = sum(sentence_lengths) / len(sentence_lengths)


class ContractTokenizer:
    """
    Tokenize contract text using NLTK.

    Provides word tokenization, sentence splitting, and n-gram extraction
    with optional filtering for common stopwords and short tokens.

    Usage:
        tokenizer = ContractTokenizer()
        result = tokenizer.tokenize(contract_text)
        print(f"Tokens: {result.token_count}")
        print(f"Sentences: {result.sentence_count}")
    """

    # Common English stopwords to optionally filter
    STOPWORDS = frozenset([
        "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
        "be", "have", "has", "had", "do", "does", "did", "will", "would",
        "could", "should", "may", "might", "must", "shall", "can", "need",
        "this", "that", "these", "those", "it", "its", "they", "them",
        "their", "we", "us", "our", "you", "your", "he", "she", "his", "her",
        "not", "no", "all", "any", "some", "each", "every", "both", "few",
        "more", "most", "other", "into", "through", "during", "before",
        "after", "above", "below", "between", "under", "again", "further",
        "then", "once", "here", "there", "when", "where", "why", "how",
        "what", "which", "who", "whom", "if", "because", "until", "while",
        "about", "against", "over", "such", "only", "own", "same", "so",
        "than", "too", "very", "just", "also", "now", "per",
    ])

    def __init__(
        self,
        min_token_length: int = 2,
        remove_stopwords: bool = False,
        lowercase: bool = True,
        extract_ngrams: bool = True,
        max_ngrams: int = 100,
    ):
        """
        Initialize the tokenizer.

        Args:
            min_token_length: Minimum token length to include.
            remove_stopwords: Whether to filter out common stopwords.
            lowercase: Whether to convert tokens to lowercase.
            extract_ngrams: Whether to extract bigrams and trigrams.
            max_ngrams: Maximum number of n-grams to return (most common).
        """
        self.min_token_length = min_token_length
        self.remove_stopwords = remove_stopwords
        self.lowercase = lowercase
        self.extract_ngrams = extract_ngrams
        self.max_ngrams = max_ngrams
        self._nltk = None

    def _lazy_import_nltk(self):
        """Lazy import NLTK."""
        if self._nltk is None:
            _ensure_nltk_data()
            import nltk
            self._nltk = nltk
        return self._nltk

    def tokenize(self, text: str) -> TokenizationResult:
        """
        Tokenize text into words and sentences.

        Args:
            text: Input text to tokenize.

        Returns:
            TokenizationResult with tokens, sentences, and n-grams.
        """
        if not text or not text.strip():
            return TokenizationResult()

        nltk = self._lazy_import_nltk()
        result = TokenizationResult()

        # Clean text
        text = self._clean_text(text)

        # Sentence tokenization
        try:
            result.sentences = nltk.sent_tokenize(text)
        except Exception as e:
            logger.warning(f"Sentence tokenization failed: {e}")
            # Fallback: split on common sentence endings
            result.sentences = self._fallback_sentence_split(text)

        # Word tokenization
        try:
            raw_tokens = nltk.word_tokenize(text)
        except Exception as e:
            logger.warning(f"Word tokenization failed: {e}")
            # Fallback: simple whitespace split
            raw_tokens = text.split()

        # Filter and process tokens
        result.tokens = self._filter_tokens(raw_tokens)

        # Extract n-grams if enabled
        if self.extract_ngrams and len(result.tokens) >= 2:
            result.bigrams = self._extract_ngrams(result.tokens, 2)
            if len(result.tokens) >= 3:
                result.trigrams = self._extract_ngrams(result.tokens, 3)

        result.compute_stats()
        return result

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text before tokenization."""
        # Replace multiple whitespace with single space
        text = re.sub(r"\s+", " ", text)

        # Remove non-printable characters
        text = "".join(c for c in text if c.isprintable() or c in "\n\t")

        return text.strip()

    def _filter_tokens(self, tokens: list[str]) -> list[str]:
        """Filter tokens based on configuration."""
        filtered = []

        for token in tokens:
            # Skip punctuation-only tokens
            if not any(c.isalnum() for c in token):
                continue

            # Apply lowercase if configured
            if self.lowercase:
                token = token.lower()

            # Skip short tokens
            if len(token) < self.min_token_length:
                continue

            # Skip stopwords if configured
            if self.remove_stopwords and token.lower() in self.STOPWORDS:
                continue

            filtered.append(token)

        return filtered

    def _extract_ngrams(self, tokens: list[str], n: int) -> list[str]:
        """
        Extract n-grams from token list.

        Args:
            tokens: List of tokens.
            n: N-gram size (2 for bigrams, 3 for trigrams).

        Returns:
            List of most common n-grams as space-separated strings.
        """
        if len(tokens) < n:
            return []

        # Generate n-grams
        ngrams = [
            " ".join(tokens[i : i + n])
            for i in range(len(tokens) - n + 1)
        ]

        # Count and return most common
        counter = Counter(ngrams)
        return [ngram for ngram, _ in counter.most_common(self.max_ngrams)]

    def _fallback_sentence_split(self, text: str) -> list[str]:
        """Fallback sentence splitting without NLTK."""
        # Split on common sentence endings
        sentences = re.split(r"(?<=[.!?])\s+", text)
        return [s.strip() for s in sentences if s.strip()]

    def tokenize_for_search(self, text: str) -> list[str]:
        """
        Tokenize text optimized for search indexing.

        Returns lowercase, stopword-filtered tokens suitable for
        full-text search or similarity matching.

        Args:
            text: Input text.

        Returns:
            List of filtered tokens.
        """
        # Use settings optimized for search
        original_stopwords = self.remove_stopwords
        original_lowercase = self.lowercase

        self.remove_stopwords = True
        self.lowercase = True

        try:
            result = self.tokenize(text)
            return result.tokens
        finally:
            self.remove_stopwords = original_stopwords
            self.lowercase = original_lowercase
