"""Lineage visualization utilities."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from lineage.graph import LineageGraph, NodeType

logger = logging.getLogger(__name__)


def generate_mermaid_diagram(graph: LineageGraph) -> str:
    """
    Generate Mermaid diagram from lineage graph.

    Args:
        graph: LineageGraph to visualize

    Returns:
        Mermaid diagram string
    """
    lines = ["graph LR"]

    # Add subgraphs for each layer
    layers = {}
    for key, node in graph.nodes.items():
        layer = node.node_type.value
        if layer not in layers:
            layers[layer] = []
        layers[layer].append(node)

    layer_order = ["source", "bronze", "silver", "gold", "diamond", "warehouse"]

    for layer in layer_order:
        if layer not in layers:
            continue

        lines.append(f"    subgraph {layer.upper()}")
        for node in layers[layer]:
            node_id = f"{layer}_{node.name}".replace("-", "_")
            lines.append(f"        {node_id}[{node.name}]")
        lines.append("    end")

    # Add edges
    for edge in graph.edges:
        source_id = f"{edge.source.node_type.value}_{edge.source.name}".replace("-", "_")
        target_id = f"{edge.target.node_type.value}_{edge.target.name}".replace("-", "_")
        lines.append(f"    {source_id} --> {target_id}")

    return "\n".join(lines)


def generate_ascii_diagram(graph: LineageGraph) -> str:
    """
    Generate ASCII diagram from lineage graph.

    Args:
        graph: LineageGraph to visualize

    Returns:
        ASCII diagram string
    """
    lines = []
    lines.append("=" * 60)
    lines.append("LINEAGE DIAGRAM")
    lines.append("=" * 60)

    # Group by layer
    layers = {}
    for key, node in graph.nodes.items():
        layer = node.node_type.value
        if layer not in layers:
            layers[layer] = []
        layers[layer].append(node)

    layer_order = ["source", "bronze", "silver", "gold", "diamond", "warehouse"]

    for layer in layer_order:
        if layer not in layers:
            continue

        lines.append(f"\n[{layer.upper()}]")
        for node in layers[layer]:
            # Find upstream nodes
            node_key = f"{layer}:{node.name}"
            upstream = [
                f"{n.node_type.value}:{n.name}"
                for n in graph.get_upstream(node_key)
            ]
            if upstream:
                sources = ", ".join(upstream[:3])
                lines.append(f"  {node.name} <- ({sources})")
            else:
                lines.append(f"  {node.name}")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


def export_lineage_html(graph: LineageGraph, output_path: Path) -> None:
    """
    Export lineage graph as interactive HTML.

    Args:
        graph: LineageGraph to export
        output_path: Output HTML file path
    """
    mermaid_code = generate_mermaid_diagram(graph)

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>DC Data Platform - Lineage</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 40px;
            background: #f5f5f5;
        }}
        h1 {{ color: #1a1a2e; }}
        .mermaid {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
    </style>
</head>
<body>
    <h1>DC Data Platform - Data Lineage</h1>
    <div class="mermaid">
{mermaid_code}
    </div>
    <script>
        mermaid.initialize({{ startOnLoad: true, theme: 'default' }});
    </script>
</body>
</html>
"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(html)

    logger.info(f"Lineage diagram exported to {output_path}")
