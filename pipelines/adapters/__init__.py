"""Pipeline adapters for different execution environments."""

from factory.pipelines.adapters.local import LocalAdapter
from factory.pipelines.adapters.ms_fabric import MSFabricAdapter

__all__ = ["LocalAdapter", "MSFabricAdapter"]
