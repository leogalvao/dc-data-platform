"""Pipeline adapters for different execution environments."""

from pipelines.adapters.local import LocalAdapter
from pipelines.adapters.ms_fabric import MSFabricAdapter

__all__ = ["LocalAdapter", "MSFabricAdapter"]
