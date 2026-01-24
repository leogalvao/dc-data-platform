"""Microsoft Fabric adapter for pipelines (placeholder for future implementation)."""

from __future__ import annotations

import logging
from typing import Any

from factory.config.settings import Settings
from factory.pipelines.base import Pipeline, PipelineResult, TaskStatus

logger = logging.getLogger(__name__)


class MSFabricAdapter:
    """
    Microsoft Fabric execution adapter.

    This is a placeholder for future Microsoft Fabric integration.
    When implemented, this will translate pipeline definitions to
    Fabric pipeline format and execute them in Azure.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        workspace_id: str | None = None,
        lakehouse_id: str | None = None,
    ):
        """
        Initialize Microsoft Fabric adapter.

        Args:
            settings: Configuration settings
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
        """
        self.settings = settings or Settings()
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id

        if not self._check_fabric_available():
            logger.warning(
                "Microsoft Fabric SDK not available. "
                "Install 'semantic-link' package for Fabric support."
            )

    def _check_fabric_available(self) -> bool:
        """Check if Microsoft Fabric SDK is available."""
        try:
            import sempy  # noqa: F401
            return True
        except ImportError:
            return False

    def run_pipeline(
        self,
        pipeline_name: str,
        dry_run: bool = False,
    ) -> PipelineResult:
        """
        Run a pipeline in Microsoft Fabric.

        Args:
            pipeline_name: Name of pipeline definition
            dry_run: If True, only validate configuration

        Returns:
            PipelineResult with execution details

        Raises:
            NotImplementedError: Fabric integration not yet implemented
        """
        raise NotImplementedError(
            "Microsoft Fabric integration is not yet implemented. "
            "Use LocalAdapter for local execution."
        )

    def translate_pipeline(self, pipeline: Pipeline) -> dict[str, Any]:
        """
        Translate a pipeline definition to Microsoft Fabric format.

        Args:
            pipeline: Pipeline definition

        Returns:
            Fabric pipeline JSON representation

        Raises:
            NotImplementedError: Translation not yet implemented
        """
        raise NotImplementedError(
            "Pipeline translation to Fabric format is not yet implemented."
        )

    def list_fabric_pipelines(self) -> list[str]:
        """
        List pipelines in Microsoft Fabric workspace.

        Returns:
            List of Fabric pipeline names

        Raises:
            NotImplementedError: Not yet implemented
        """
        raise NotImplementedError(
            "Fabric pipeline listing is not yet implemented."
        )


# Backwards compatibility alias
FabricAdapter = MSFabricAdapter
