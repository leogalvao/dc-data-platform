"""Lineage graph construction and traversal."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class NodeType(str, Enum):
    """Types of lineage nodes."""

    SOURCE = "source"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    DIAMOND = "diamond"
    WAREHOUSE = "warehouse"
    DASHBOARD = "dashboard"


@dataclass
class LineageNode:
    """Node in the lineage graph."""

    name: str
    node_type: NodeType
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash((self.name, self.node_type))

    def __eq__(self, other):
        if not isinstance(other, LineageNode):
            return False
        return self.name == other.name and self.node_type == other.node_type


@dataclass
class LineageEdge:
    """Edge in the lineage graph."""

    source: LineageNode
    target: LineageNode
    transformation: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class LineageGraph:
    """Lineage graph for tracking data flow."""

    def __init__(self):
        self.nodes: dict[str, LineageNode] = {}
        self.edges: list[LineageEdge] = []
        self._adjacency: dict[str, list[str]] = {}
        self._reverse_adjacency: dict[str, list[str]] = {}

    def add_node(self, node: LineageNode) -> None:
        """Add a node to the graph."""
        key = f"{node.node_type.value}:{node.name}"
        self.nodes[key] = node
        if key not in self._adjacency:
            self._adjacency[key] = []
        if key not in self._reverse_adjacency:
            self._reverse_adjacency[key] = []

    def add_edge(self, edge: LineageEdge) -> None:
        """Add an edge to the graph."""
        source_key = f"{edge.source.node_type.value}:{edge.source.name}"
        target_key = f"{edge.target.node_type.value}:{edge.target.name}"

        # Ensure nodes exist
        if source_key not in self.nodes:
            self.add_node(edge.source)
        if target_key not in self.nodes:
            self.add_node(edge.target)

        self.edges.append(edge)
        self._adjacency[source_key].append(target_key)
        self._reverse_adjacency[target_key].append(source_key)

    def get_upstream(self, node_key: str) -> list[LineageNode]:
        """Get all upstream (source) nodes."""
        upstream = []
        visited = set()

        def dfs(key: str):
            if key in visited:
                return
            visited.add(key)
            for parent_key in self._reverse_adjacency.get(key, []):
                if parent_key in self.nodes:
                    upstream.append(self.nodes[parent_key])
                dfs(parent_key)

        dfs(node_key)
        return upstream

    def get_downstream(self, node_key: str) -> list[LineageNode]:
        """Get all downstream (dependent) nodes."""
        downstream = []
        visited = set()

        def dfs(key: str):
            if key in visited:
                return
            visited.add(key)
            for child_key in self._adjacency.get(key, []):
                if child_key in self.nodes:
                    downstream.append(self.nodes[child_key])
                dfs(child_key)

        dfs(node_key)
        return downstream

    @classmethod
    def from_manifest(cls, manifest_path: Path) -> LineageGraph:
        """Build graph from manifest file."""
        graph = cls()

        with open(manifest_path) as f:
            manifest = yaml.safe_load(f)

        for dataset_name, dataset_info in manifest.get("datasets", {}).items():
            layer = dataset_info.get("layer", "silver")
            node = LineageNode(
                name=dataset_name,
                node_type=NodeType(layer),
                description=dataset_info.get("description", ""),
            )
            graph.add_node(node)

            # Add edges from sources
            for source_name in dataset_info.get("sources", []):
                source_layer = dataset_info.get("source_layer", "source")
                source_node = LineageNode(
                    name=source_name,
                    node_type=NodeType(source_layer),
                )
                edge = LineageEdge(
                    source=source_node,
                    target=node,
                    transformation=dataset_info.get("transformation", ""),
                )
                graph.add_edge(edge)

        return graph

    def to_dict(self) -> dict[str, Any]:
        """Convert graph to dictionary representation."""
        return {
            "nodes": [
                {
                    "name": node.name,
                    "type": node.node_type.value,
                    "description": node.description,
                }
                for node in self.nodes.values()
            ],
            "edges": [
                {
                    "source": f"{edge.source.node_type.value}:{edge.source.name}",
                    "target": f"{edge.target.node_type.value}:{edge.target.name}",
                    "transformation": edge.transformation,
                }
                for edge in self.edges
            ],
        }
