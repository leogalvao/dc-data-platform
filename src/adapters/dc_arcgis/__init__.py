"""DC ArcGIS REST API adapters."""

from .affordable_housing import (
    AffordableHousingAdapter,
    PublicHousingAdapter,
)
from .affordable_housing import (
    get_default_config as get_housing_config,
)
from .base import DCArcGISBaseAdapter
from .contracts import ContractsAdapter
from .contracts import get_default_config as get_contracts_config
from .itspe_property_land import (
    ITSPEPropertyLandAdapter,
)
from .itspe_property_land import (
    get_default_config as get_itspe_property_land_config,
)
from .location_verifier import (
    LocationVerifierAdapter,
    LocationVerifierRecord,
)
from .location_verifier import (
    get_default_config as get_location_verifier_config,
)
from .opendata import (
    CBEBusinessesAdapter,
    DCOpenDataAdapter,
    SolicitationsAdapter,
)
from .opendata import (
    get_default_config as get_opendata_config,
)
from .payments import PaymentsAdapter
from .payments import get_default_config as get_payments_config
from .property_sales_cama import (
    PropertySalesCAMAAdapter,
)
from .property_sales_cama import (
    get_default_config as get_property_sales_cama_config,
)
from .propertyquest_highlights import (
    PropertyQuestHighlightsAdapter,
    PropertyQuestHighlightsRecord,
)
from .propertyquest_highlights import (
    get_default_config as get_propertyquest_highlights_config,
)
from .purchase_orders import PurchaseOrdersAdapter
from .purchase_orders import get_default_config as get_purchase_orders_config
from .record_lots import (
    RecordLotsAdapter,
)
from .record_lots import (
    get_default_config as get_record_lots_config,
)
from .residential_cama import (
    ResidentialCAMAAdapter,
)
from .residential_cama import (
    get_default_config as get_residential_cama_config,
)
from .tax_lots import (
    TaxLotsAdapter,
)
from .tax_lots import (
    get_default_config as get_tax_lots_config,
)
from .ward_2022 import (
    Ward2022Adapter,
)
from .ward_2022 import (
    get_default_config as get_ward_2022_config,
)

__all__ = [
    "DCArcGISBaseAdapter",
    "ContractsAdapter",
    "PurchaseOrdersAdapter",
    "PaymentsAdapter",
    "DCOpenDataAdapter",
    "CBEBusinessesAdapter",
    "SolicitationsAdapter",
    "AffordableHousingAdapter",
    "PublicHousingAdapter",
    "ResidentialCAMAAdapter",
    # DC Property Data Extraction (Cohesive Scraper Map)
    "ITSPEPropertyLandAdapter",
    "LocationVerifierAdapter",
    "LocationVerifierRecord",
    "PropertyQuestHighlightsAdapter",
    "PropertyQuestHighlightsRecord",
    # New DC GIS adapters
    "Ward2022Adapter",
    "TaxLotsAdapter",
    "RecordLotsAdapter",
    "PropertySalesCAMAAdapter",
]


# =============================================================================
# Auto-register configs on module import
# =============================================================================

def _register_all_configs():
    """Register all DC ArcGIS configs with the registry."""
    from ...core.registry import ScraperRegistry

    # Register each adapter's config
    ScraperRegistry.register_config(get_contracts_config())
    ScraperRegistry.register_config(get_purchase_orders_config())
    ScraperRegistry.register_config(get_payments_config())

    # OpenData adapters
    for dataset in ["cbe_businesses", "solicitations"]:
        ScraperRegistry.register_config(get_opendata_config(dataset))

    # Housing adapters
    for dataset in ["affordable_housing", "public_housing"]:
        ScraperRegistry.register_config(get_housing_config(dataset))

    # Residential CAMA adapter
    ScraperRegistry.register_config(get_residential_cama_config())

    # DC Property Data Extraction (Cohesive Scraper Map) adapters
    ScraperRegistry.register_config(get_itspe_property_land_config())
    ScraperRegistry.register_config(get_location_verifier_config())
    ScraperRegistry.register_config(get_propertyquest_highlights_config())

    # New DC GIS adapters
    ScraperRegistry.register_config(get_ward_2022_config())
    ScraperRegistry.register_config(get_tax_lots_config())
    ScraperRegistry.register_config(get_record_lots_config())
    ScraperRegistry.register_config(get_property_sales_cama_config())


# Register configs when module is imported
_register_all_configs()
