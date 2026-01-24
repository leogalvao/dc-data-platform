"""
Transformers package for Gold and Diamond layer data transformations.

This package provides:
- BaseTransformer: Abstract base class for all transformers
- Gold transformers: Aggregation transformers for the Gold layer
- Diamond transformers: Feature engineering transformers for the Diamond layer
"""

from .base import BaseTransformer

__all__ = ["BaseTransformer"]
