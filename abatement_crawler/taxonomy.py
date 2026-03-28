"""Abatement category taxonomy for the crawler."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AbatementCategory:
    """A top-level abatement category with its slug, display name, description, and subcategories."""

    slug: str
    name: str
    description: str
    subcategories: tuple[str, ...]


CATEGORIES: list[AbatementCategory] = [
    AbatementCategory(
        slug="demand_reduction",
        name="Demand & Activity Reduction",
        description=(
            "Cuts emissions by reducing service demand or output intensity. "
            "Top of the abatement hierarchy — often cheapest but hardest to quantify reliably."
        ),
        subcategories=(
            "energy service demand reduction (heat, cooling, lighting)",
            "transport demand reduction",
            "material demand reduction",
            "product lifetime extension",
            "behavioural change",
        ),
    ),
    AbatementCategory(
        slug="energy_efficiency",
        name="Energy Efficiency",
        description=(
            "Reduces energy per unit of output. "
            "Covers buildings (fabric and systems) and industry (process, heat recovery, equipment, systems). "
            "No fuel switching — boundary is energy intensity only."
        ),
        subcategories=(
            "building fabric (insulation, glazing, airtightness)",
            "building systems (HVAC, lighting, controls)",
            "industrial process optimisation",
            "industrial heat recovery",
            "industrial equipment efficiency",
            "industrial system optimisation (steam, compressed air)",
        ),
    ),
    AbatementCategory(
        slug="electrification",
        name="Electrification",
        description=(
            "Switches end-use energy from combustion to electricity. "
            "Demand-side shift — keep separate from renewables supply."
        ),
        subcategories=(
            "heat pumps",
            "electric process heat",
            "electric transport",
            "electric machinery",
        ),
    ),
    AbatementCategory(
        slug="low_carbon_supply",
        name="Low-Carbon Energy Supply",
        description=(
            "Decarbonises the energy input without changing the demand side. "
            "No demand-side measures."
        ),
        subcategories=(
            "onsite renewables",
            "grid decarbonisation (implicit / PPA / tariffs)",
            "bioenergy (with sustainability caveats)",
            "district heating (low-carbon sources)",
        ),
    ),
    AbatementCategory(
        slug="fuel_switching",
        name="Fuel Switching (Non-Electric)",
        description=(
            "Switches between fuels without electrification. "
            "Different cost structures and transition pathways from electrification."
        ),
        subcategories=(
            "fossil to lower-carbon fossil (coal to gas)",
            "fossil to hydrogen",
            "fossil to biofuels / biomethane",
            "aviation and marine fuels (SAF, ammonia, methanol)",
        ),
    ),
    AbatementCategory(
        slug="flexibility_storage",
        name="Energy System Flexibility & Storage",
        description=(
            "Reduces emissions indirectly via system optimisation. "
            "Often missed in marginal abatement curves but critical at system level."
        ),
        subcategories=(
            "batteries",
            "thermal storage",
            "demand response",
            "load shifting",
            "vehicle-to-grid (V2G)",
        ),
    ),
    AbatementCategory(
        slug="process_emissions",
        name="Process Emissions Abatement",
        description=(
            "Reduces non-combustion emissions from chemical and physical processes. "
            "These emissions are not energy-driven, so efficiency and electrification will not capture them."
        ),
        subcategories=(
            "clinker substitution (cement)",
            "alternative binders (LC3, geopolymers)",
            "CCS on process emissions (cement, lime)",
            "low-carbon steel routes (beyond EAF)",
            "nitric acid N2O abatement",
            "adipic acid N2O abatement",
            "hydrogen-based reduction (steel, ammonia feedstock)",
        ),
    ),
    AbatementCategory(
        slug="fugitive_fossil",
        name="Fugitive Emissions – Fossil Systems",
        description=(
            "Covers methane and other leaks from fossil energy supply chains. "
            "A large, low-cost abatement pool."
        ),
        subcategories=(
            "oil and gas methane leak detection and repair (LDAR)",
            "venting and flaring reduction",
            "pneumatic device replacement",
            "coal mine methane capture",
            "abandoned well sealing",
        ),
    ),
    AbatementCategory(
        slug="fugitive_fgases",
        name="Fugitive Emissions – F-Gases",
        description=(
            "Highly potent, often overlooked refrigerant and industrial gas emissions. "
            "Different physics and very high GWP distinguish this from buildings and industry categories."
        ),
        subcategories=(
            "refrigerant substitution (HFC to HFO / CO2 / ammonia)",
            "leak detection and maintenance",
            "end-of-life refrigerant recovery",
            "low-GWP foam blowing agents",
            "fire suppression system replacement",
        ),
    ),
    AbatementCategory(
        slug="mining_extractives",
        name="Mining & Extractives Emissions",
        description=(
            "Hybrid of fugitive methane, heavy diesel use, and process emissions specific to "
            "mining and extractive industries. Lumping into general industry loses analytical clarity."
        ),
        subcategories=(
            "coal mine methane capture and utilisation",
            "ventilation air methane (VAM) oxidation",
            "electrification of mining operations",
            "ore processing efficiency",
            "tailings management emissions reduction",
        ),
    ),
    AbatementCategory(
        slug="materials_circular",
        name="Materials & Circular Economy",
        description=(
            "Reduces emissions via material flows. "
            "Reduces upstream emissions — not always visible at the point of use."
        ),
        subcategories=(
            "recycling (closed-loop, open-loop)",
            "reuse and refurbishment",
            "material substitution",
            "lightweighting",
            "waste reduction",
        ),
    ),
    AbatementCategory(
        slug="carbon_capture",
        name="Carbon Capture (Point Source)",
        description="Captures emissions at source before they are released to the atmosphere.",
        subcategories=(
            "CCS on power generation",
            "CCS on industry",
            "CCS on hydrogen production",
            "BECCS (bioenergy with carbon capture and storage)",
        ),
    ),
    AbatementCategory(
        slug="cdr",
        name="Carbon Dioxide Removal (CDR)",
        description=(
            "Removes CO2 from the atmosphere. "
            "Keep separate from point-source CCS — different economics, policy, and permanence."
        ),
        subcategories=(
            "direct air capture (DAC)",
            "afforestation and reforestation",
            "soil carbon sequestration",
            "enhanced weathering",
            "ocean-based methods",
        ),
    ),
]

CATEGORY_SLUGS: list[str] = [c.slug for c in CATEGORIES]
CATEGORY_LOOKUP: dict[str, AbatementCategory] = {c.slug: c for c in CATEGORIES}
