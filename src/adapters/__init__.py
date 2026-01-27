"""Source-specific adapters that wrap existing scrapers."""

# DC ArcGIS adapters
from .dc_arcgis import (
    AffordableHousingAdapter,
    ContractsAdapter,
    DCArcGISBaseAdapter,
    DCOpenDataAdapter,
    PaymentsAdapter,
    PurchaseOrdersAdapter,
)

# DC Contracts adapters
from .dc_contracts import ContractAttachmentAdapter

# DC Facilities crawler
from .dc_facilities import DCFacilitiesCrawler

# Demographics adapters
from .demographics import ODNDemographicsAdapter

# Real estate adapters
from .real_estate import (
    CrossingAdapter,
    RealtorAdapter,
    RealtorBaseAdapter,
    RealtorForRentAdapter,
    RealtorForSaleAdapter,
    RealtorSoldAdapter,
    RentCastAdapter,
    RentCastBaseAdapter,
    RentCastMarketStatsAdapter,
    RentCastRentEstimatesAdapter,
    RentCastValueEstimatesAdapter,
    ZillowBaseAdapter,
    ZillowForecastAdapter,
    ZillowInventoryAdapter,
    ZillowMarketHeatAdapter,
    ZillowResearchAdapter,
    ZillowSalesCountAdapter,
    ZillowZhviMetroAdapter,
    ZillowZhviZipAdapter,
    ZillowZoriMetroAdapter,
    ZillowZoriZipAdapter,
)

__all__ = [
    # DC ArcGIS
    "DCArcGISBaseAdapter",
    "ContractsAdapter",
    "PurchaseOrdersAdapter",
    "PaymentsAdapter",
    "DCOpenDataAdapter",
    "AffordableHousingAdapter",
    # DC Contracts
    "ContractAttachmentAdapter",
    # DC Facilities
    "DCFacilitiesCrawler",
    # Real estate
    "CrossingAdapter",
    "RealtorBaseAdapter",
    "RealtorAdapter",
    "RealtorForSaleAdapter",
    "RealtorForRentAdapter",
    "RealtorSoldAdapter",
    "RentCastBaseAdapter",
    "RentCastAdapter",
    "RentCastMarketStatsAdapter",
    "RentCastRentEstimatesAdapter",
    "RentCastValueEstimatesAdapter",
    "ZillowBaseAdapter",
    "ZillowResearchAdapter",
    "ZillowZoriMetroAdapter",
    "ZillowZoriZipAdapter",
    "ZillowZhviMetroAdapter",
    "ZillowZhviZipAdapter",
    "ZillowMarketHeatAdapter",
    "ZillowInventoryAdapter",
    "ZillowSalesCountAdapter",
    "ZillowForecastAdapter",
    # Demographics
    "ODNDemographicsAdapter",
]
