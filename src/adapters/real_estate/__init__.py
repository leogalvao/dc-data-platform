"""Real estate source adapters."""

from .crossing import CrossingAdapter
from .realtor import (
    RealtorAdapter,
    RealtorBaseAdapter,
    RealtorForRentAdapter,
    RealtorForSaleAdapter,
    RealtorSoldAdapter,
)
from .rentcast import (
    RentCastAdapter,
    RentCastBaseAdapter,
    RentCastMarketStatsAdapter,
    RentCastRentEstimatesAdapter,
    RentCastValueEstimatesAdapter,
)
from .zillow import (
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
]
