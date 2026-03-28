"""Currency, units, and MAC normalisation."""

from __future__ import annotations

import logging
import math
from typing import Optional

from .models import AbatementRecord

logger = logging.getLogger(__name__)

# Approximate historical average exchange rates to GBP
# Format: {currency: rate_to_gbp} (multiply amount by rate to get GBP)
_FX_RATES_TO_GBP: dict[str, float] = {
    "GBP": 1.0,
    "USD": 0.79,
    "EUR": 0.86,
    "JPY": 0.0053,
    "CAD": 0.58,
    "AUD": 0.52,
    "CHF": 0.87,
    "CNY": 0.11,
    "INR": 0.0095,
    "BRL": 0.16,
    "ZAR": 0.043,
    "KRW": 0.00060,
    "MXN": 0.046,
    "SEK": 0.075,
    "NOK": 0.075,
    "DKK": 0.115,
    "NZD": 0.48,
    "SGD": 0.58,
    "HKD": 0.10,
}

# GDP deflator indices (approximate, UK base) relative to 2023=1.0
# Based on approximate UK CPI/GDP deflator
_GDP_DEFLATOR: dict[int, float] = {
    2010: 0.76,
    2011: 0.79,
    2012: 0.81,
    2013: 0.83,
    2014: 0.84,
    2015: 0.85,
    2016: 0.87,
    2017: 0.89,
    2018: 0.91,
    2019: 0.93,
    2020: 0.94,
    2021: 0.97,
    2022: 1.05,
    2023: 1.0,
    2024: 1.03,
    2025: 1.06,
}

# ISO 3166 + common region mapping
_GEOGRAPHY_MAP: dict[str, str] = {
    "uk": "GBR",
    "united kingdom": "GBR",
    "great britain": "GBR",
    "england": "GBR",
    "britain": "GBR",
    "gb": "GBR",
    "us": "USA",
    "usa": "USA",
    "united states": "USA",
    "united states of america": "USA",
    "america": "USA",
    "eu": "EU",
    "europe": "EU",
    "european union": "EU",
    "china": "CHN",
    "india": "IND",
    "germany": "DEU",
    "france": "FRA",
    "japan": "JPN",
    "australia": "AUS",
    "canada": "CAN",
    "brazil": "BRA",
    "global": "GLOBAL",
    "worldwide": "GLOBAL",
    "world": "GLOBAL",
    "international": "GLOBAL",
}


class Normaliser:
    """Normalises AbatementRecord fields to a consistent base currency and year."""

    def __init__(self, base_currency: str = "GBP", base_year: int = 2023) -> None:
        self.base_currency = base_currency.upper()
        self.base_year = base_year

    def normalise_record(self, record: AbatementRecord) -> AbatementRecord:
        """Apply all normalisation steps to a record.

        Returns a new record with normalised values.
        """
        data = record.model_dump()
        currency = (data.get("currency") or "GBP").upper()
        price_year = data.get("price_base_year") or data.get("publication_year") or self.base_year

        # Currency + deflation normalisation for monetary fields
        monetary_fields = ["capex", "opex_fixed", "opex_variable", "opex_delta", "mac"]
        for field_name in monetary_fields:
            val = data.get(field_name)
            if val is not None:
                try:
                    converted = self.convert_currency(val, currency, price_year)
                    data[field_name] = self.deflate_price(converted, price_year)
                except Exception as exc:
                    logger.debug("Could not normalise %s for %s: %s", field_name, record.record_id, exc)

        data["currency"] = self.base_currency
        data["price_base_year"] = self.base_year

        # Geography standardisation
        if data.get("geography"):
            data["geography"] = self.standardise_geography(data["geography"])

        updated = AbatementRecord(**data)
        updated = self.recalculate_mac(updated)
        return updated

    def convert_currency(self, amount: float, from_currency: str, year: int) -> float:
        """Convert amount from from_currency to base_currency using approximate rates.

        Uses fixed approximate rates. For production use, integrate a live FX API.
        """
        from_currency = from_currency.upper()
        if from_currency == self.base_currency:
            return amount

        # Convert from_currency -> GBP, then GBP -> base_currency
        rate_from = _FX_RATES_TO_GBP.get(from_currency)
        if rate_from is None:
            logger.debug("Unknown currency %s; assuming 1:1 with GBP", from_currency)
            rate_from = 1.0

        amount_gbp = amount * rate_from

        if self.base_currency == "GBP":
            return amount_gbp

        rate_to = _FX_RATES_TO_GBP.get(self.base_currency, 1.0)
        return amount_gbp / rate_to

    def deflate_price(self, amount: float, from_year: int) -> float:
        """Deflate amount from from_year prices to base_year prices using GDP deflator."""
        deflator_from = _GDP_DEFLATOR.get(from_year)
        deflator_base = _GDP_DEFLATOR.get(self.base_year, 1.0)

        if deflator_from is None:
            # Extrapolate with approximate 2% annual inflation
            years_diff = from_year - self.base_year
            deflator_from = deflator_base * (1.02 ** years_diff)

        if deflator_from == 0:
            return amount

        return amount * (deflator_base / deflator_from)

    def recalculate_mac(self, record: AbatementRecord) -> AbatementRecord:
        """Recalculate MAC from capex, opex_delta, abatement, and lifetime.

        MAC = (CAPEX × CRF + annual_opex_delta) / annual_abatement

        where CRF = discount_rate / (1 - (1 + discount_rate)^-lifetime)

        Flags divergence >20% from source MAC.
        """
        if (
            record.capex is None
            or record.abatement_potential_tco2e is None
            or record.lifetime_years is None
            or record.abatement_potential_tco2e == 0
        ):
            return record

        discount_rate = record.discount_rate or 0.035

        # Capital Recovery Factor
        if discount_rate > 0:
            crf = discount_rate / (1 - (1 + discount_rate) ** (-record.lifetime_years))
        else:
            crf = 1.0 / record.lifetime_years

        annual_capex = record.capex * crf
        annual_opex = record.opex_delta or 0.0
        calc_mac = (annual_capex + annual_opex) / record.abatement_potential_tco2e

        data = record.model_dump()

        if record.mac is not None:
            divergence = abs(calc_mac - record.mac) / max(abs(record.mac), 1e-9)
            if divergence > 0.20:
                flags = list(data.get("quality_flags") or [])
                if "mac_divergence" not in flags:
                    flags.append("mac_divergence")
                data["quality_flags"] = flags
                note = (
                    f"Calculated MAC {calc_mac:.1f} diverges >20% from source MAC "
                    f"{record.mac:.1f} ({100*divergence:.0f}%)"
                )
                existing_notes = data.get("notes") or ""
                data["notes"] = f"{existing_notes}\n{note}".strip() if existing_notes else note
        else:
            data["mac"] = calc_mac

        return AbatementRecord(**data)

    def standardise_geography(self, geography: str) -> str:
        """Map geography string to ISO 3166 alpha-3 or standard region name."""
        if not geography:
            return geography
        normalised = _GEOGRAPHY_MAP.get(geography.lower().strip())
        if normalised:
            return normalised
        # Return title-cased original if no mapping found
        return geography.strip()
