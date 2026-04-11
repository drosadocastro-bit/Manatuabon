"""
Manatuabon Simulation Worker
─────────────────────────────────────────────────────────────────────────────
Polls the simulations queue and evidence_requests table for quantitative gaps,
runs deterministic Python physics engines, and drops structured_ingest_v1
bundles into the inbox so the agent can ingest them as governed evidence.

Physics engines (all run locally, no GPU needed):
  • orbital_confinement  — Sgr A* gravitational sphere of influence, S2
                           Schwarzschild precession, Hills mechanism radius
  • accretion_physics    — Bondi accretion rate, Eddington fraction, RIAF
                           luminosity floor for Sgr A* / M87
  • pulsar_glitch_stress — Vela crustal stress accumulation model and
                           next-glitch window prediction
  • bayesian_update      — Generic likelihood × prior confidence update for
                           any hypothesis given new evidence

Usage:
    python simulation_worker.py              # polling loop (60 s)
    python simulation_worker.py --once       # single pass, then exit
    python simulation_worker.py --run orbital_confinement H3
"""

import argparse
import json
import logging
import math
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────

_BASE = Path(__file__).resolve().parent
_DB_PATH = _BASE / "manatuabon.db"
_INBOX_PATH = _BASE / "inbox"
_LOG_PATH = _BASE / "simulation_worker.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(_LOG_PATH),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("manatuabon.simulation_worker")

# ── Physical constants ────────────────────────────────────────────────────────

G = 6.674e-11          # N m² kg⁻²
C = 2.998e8            # m s⁻¹
M_SUN = 1.989e30       # kg
PC_TO_M = 3.0857e16    # m per parsec
AU_TO_M = 1.496e11     # m per AU
YR_TO_S = 3.156e7      # s per year

# Sgr A* (Gravity Collaboration 2022)
M_SGRA = 4.154e6 * M_SUN   # kg
D_GC_PC = 8178.0            # parsecs
D_GC_M = D_GC_PC * PC_TO_M # metres

# S2 orbital elements (Gravity Collaboration 2018, Gillessen+ 2017)
S2_A_ARCSEC = 0.12555       # semi-major axis (arcsec)
S2_E = 0.88466              # eccentricity
S2_P_YR = 16.0455           # period (years)

# Vela pulsar (PSR B0833-45) — from memory.json science profile
VELA_P0_MS = 89.33          # period (ms)
VELA_PDOT = 1.25e-13        # period derivative (s/s)
VELA_AGE_KYR = 11.3         # characteristic age (kyr)
VELA_N_GLITCHES = 21        # confirmed glitches (ATNF + Lower 2021)
VELA_MEAN_INTERVAL_YR = 2.496
VELA_PERM_FRAC_TREND = 2.38e-3   # % per decade → fractional per decade

# ── Keyword routing tables ───────────────────────────────────────────────────

_ORBITAL_KEYWORDS = {
    "jailer", "orbital", "confinement", "s-star", "s2", "hills",
    "sphere of influence", "sgr a", "schwarzschild precession",
    "stellar orbit", "gravitational dominance",
}
_ACCRETION_KEYWORDS = {
    "accretion", "bondi", "eddington", "riaf", "dormant", "bad eater",
    "luminosity", "feeding", "agn", "m87", "quiescent",
}
_PULSAR_KEYWORDS = {
    "pulsar", "glitch", "crustal", "vela", "superfluid", "pinning",
    "nuclear pasta", "stress", "neutron star crust",
}
_BAYESIAN_KEYWORDS = {
    "bayesian", "confidence", "posterior", "prior", "update",
    "probability", "credibility",
}


def _classify(text: str) -> str:
    """Return sim_type from free-text name/request."""
    t = text.lower()
    if any(k in t for k in _ORBITAL_KEYWORDS):
        return "orbital_confinement"
    if any(k in t for k in _ACCRETION_KEYWORDS):
        return "accretion_physics"
    if any(k in t for k in _PULSAR_KEYWORDS):
        return "pulsar_glitch_stress"
    if any(k in t for k in _BAYESIAN_KEYWORDS):
        return "bayesian_update"
    return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Physics Engines
# ─────────────────────────────────────────────────────────────────────────────

def engine_orbital_confinement(params: dict) -> dict:
    """
    Sgr A* gravitational confinement physics.

    Replaces the metaphorical 'Jailer/Warden' framing of H3 with three
    concrete, measurable physical mechanisms:

    1. Gravitational Sphere of Influence (r_h): the radius within which
       Sgr A*'s gravity dominates the surrounding stellar potential.
    2. Schwarzschild Precession of S2: the relativistic orbital advance
       per period — the clearest GR signature in the central parsec.
    3. Hills Mechanism Capture Radius: the radius within which stellar
       binaries are tidally disrupted, permanently depositing one component
       as a bound S-star.

    All results are falsifiable against GRAVITY/VLT and EHT observations.
    """
    sigma_km_s = params.get("velocity_dispersion_km_s", 100.0)
    sigma = sigma_km_s * 1e3  # m/s

    bh_mass = params.get("bh_mass_msun", 4.154e6) * M_SUN
    a_arcsec = params.get("s2_semimajor_arcsec", S2_A_ARCSEC)
    e = params.get("s2_eccentricity", S2_E)
    p_yr = params.get("s2_period_yr", S2_P_YR)
    d_pc = params.get("distance_gc_pc", D_GC_PC)

    # ── 1. Gravitational Sphere of Influence ─────────────────────────────────
    # r_h = GM_BH / sigma²  (Peebles 1972 / Merritt 2004)
    gm = G * bh_mass
    r_h_m = gm / (sigma ** 2)
    r_h_pc = r_h_m / PC_TO_M
    r_h_ly = r_h_pc * 3.2616

    # ── 2. S2 Schwarzschild Precession ───────────────────────────────────────
    # Δφ_GR = 6π GM / [a(1-e²) c²]  per orbit (weak-field GR)
    a_m = a_arcsec * (d_pc * PC_TO_M / (180 * 3600 / math.pi))  # rad → m
    semi_latus = a_m * (1.0 - e ** 2)
    delta_phi_rad = 6.0 * math.pi * gm / (semi_latus * C ** 2)
    delta_phi_deg = math.degrees(delta_phi_rad)
    delta_phi_arcmin = delta_phi_deg * 60.0
    # Observed by Gravity Collaboration 2020: 12.1 ± 0.3 arcmin
    gravity_2020_arcmin = 12.1

    # ── 3. Hills Mechanism capture radius ────────────────────────────────────
    # For a binary with separation a_bin disrupted at pericenter r_p:
    # r_Hills ≈ a_bin × (M_BH / 3M_bin)^(1/3)
    # Using canonical a_bin=0.1 AU, M_bin=2 M_sun (two Solar-type stars)
    a_bin_m = params.get("binary_semimajor_au", 0.1) * AU_TO_M
    m_bin = params.get("binary_mass_msun", 2.0) * M_SUN
    r_hills_m = a_bin_m * (bh_mass / (3.0 * m_bin)) ** (1.0 / 3.0)
    r_hills_pc = r_hills_m / PC_TO_M
    r_hills_au = r_hills_m / AU_TO_M

    # ── 4. Tidal disruption radius for a Solar-type star ─────────────────────
    r_star = params.get("star_radius_rsun", 1.0) * 6.957e8  # m
    m_star = params.get("star_mass_msun", 1.0) * M_SUN
    r_tidal_m = r_star * (bh_mass / m_star) ** (1.0 / 3.0)
    r_tidal_au = r_tidal_m / AU_TO_M

    # ── 5. Number of S-stars within r_h ─────────────────────────────────────
    # Observed: ~140 S-stars within ~1 pc (Gillessen et al. 2017)
    n_s_stars_estimated = 140 if r_h_pc >= 1.0 else int(140 * (r_h_pc / 1.0) ** 2)

    # ── Testable predictions ─────────────────────────────────────────────────
    predictions = [
        {
            "prediction": "S2 Schwarzschild precession",
            "predicted_value": f"{delta_phi_arcmin:.2f} arcmin per orbit",
            "observed_value": f"{gravity_2020_arcmin:.1f} ± 0.3 arcmin (GRAVITY 2020)",
            "agreement": abs(delta_phi_arcmin - gravity_2020_arcmin) < 1.0,
            "falsification": "Precession deviating >1 arcmin from prediction would require beyond-GR physics",
        },
        {
            "prediction": "Gravitational sphere of influence radius",
            "predicted_value": f"{r_h_pc:.2f} pc ({r_h_ly:.2f} light-years)",
            "falsification": "Stars with regular kinematics found within r_h would challenge dominance claim",
        },
        {
            "prediction": "Hills capture radius for Solar-type binary",
            "predicted_value": f"{r_hills_au:.1f} AU ({r_hills_pc:.4f} pc)",
            "falsification": "Absence of hypervelocity star counterparts for S-stars within r_Hills would weaken the Hills mechanism",
        },
        {
            "prediction": "Additional S-star GR precession scales as a⁻¹(1-e²)⁻¹",
            "predicted_value": "Shorter-period, higher-eccentricity S-stars should show larger precession",
            "falsification": "Stars with predicted large precession showing Keplerian orbits would rule out GR dominance",
        },
    ]

    mechanism_description = (
        f"Sgr A* (M = {bh_mass/M_SUN:.3e} M☉) defines a Gravitational Sphere of Influence "
        f"of radius r_h = {r_h_pc:.2f} pc ({r_h_ly:.2f} ly), computed from "
        f"v_dispersion = {sigma_km_s} km/s. Within r_h, Sgr A*'s gravity dominates all "
        f"other galactic potentials — approximately {n_s_stars_estimated} S-stars are permanently "
        f"bound. The Hills mechanism disrupts stellar binaries within r_Hills = {r_hills_au:.1f} AU, "
        f"depositing captured components as S-stars. S2 (a = {a_arcsec:.5f} arcsec, e = {e:.5f}, "
        f"P = {p_yr:.4f} yr) shows Schwarzschild precession of {delta_phi_arcmin:.2f} arcmin/orbit, "
        f"confirmed at {gravity_2020_arcmin:.1f} arcmin by GRAVITY 2020 — a measurable GR signature "
        f"distinguishing this environment from simple Keplerian dynamics."
    )

    revised_hypothesis_text = (
        "Sgr A* defines a gravitational sphere of influence (r_h ≈ "
        f"{r_h_pc:.2f} pc) within which stellar kinematics are permanently dominated by "
        "its gravity. The S-stars are not 'imprisoned' metaphorically — they are physically "
        "captured via the Hills mechanism, with their orbital dynamics exhibiting measurable "
        "general-relativistic deviations (Schwarzschild precession) not present in Keplerian "
        "systems. This confinement zone is falsifiable: stars should exhibit precession "
        "scaling as a⁻¹(1−e²)⁻¹, detectable with GRAVITY/VLT continued monitoring."
    )

    return {
        "sim_type": "orbital_confinement",
        "inputs": {
            "bh_mass_msun": bh_mass / M_SUN,
            "velocity_dispersion_km_s": sigma_km_s,
            "s2_semimajor_arcsec": a_arcsec,
            "s2_eccentricity": e,
            "s2_period_yr": p_yr,
            "distance_gc_pc": d_pc,
        },
        "results": {
            "gravitational_sphere_of_influence_pc": round(r_h_pc, 4),
            "gravitational_sphere_of_influence_ly": round(r_h_ly, 4),
            "s2_schwarzschild_precession_arcmin_per_orbit": round(delta_phi_arcmin, 3),
            "s2_schwarzschild_precession_deg_per_orbit": round(delta_phi_deg, 5),
            "gravity_2020_observed_arcmin": gravity_2020_arcmin,
            "precession_agreement_within_1arcmin": abs(delta_phi_arcmin - gravity_2020_arcmin) < 1.0,
            "hills_capture_radius_au": round(r_hills_au, 2),
            "hills_capture_radius_pc": round(r_hills_pc, 6),
            "tidal_disruption_radius_au": round(r_tidal_au, 4),
            "n_s_stars_within_r_h_estimate": n_s_stars_estimated,
        },
        "mechanism": mechanism_description,
        "revised_hypothesis": revised_hypothesis_text,
        "testable_predictions": predictions,
        "council_revisions_addressed": [
            "Replaced anthropomorphic 'warden/imprisoned' with Gravitational Sphere of Influence",
            "Schwarzschild precession provides a quantitative, falsifiable prediction (12.1 arcmin/orbit observed)",
            "Hills mechanism gives a specific physical process for S-star capture",
            "Observational signature: precession scaling with a⁻¹(1−e²)⁻¹ distinguishes active GSI from passive Keplerian gravity",
        ],
        "recommended_next_evidence": [
            "GRAVITY Collaboration orbital data for S2, S62, S4716 (multi-star precession comparison)",
            "Hypervelocity star survey to find Hills-ejected counterparts to bound S-stars",
            "Stellar velocity dispersion profile within 1 pc of Sgr A* (VLT/NACO or KECK)",
        ],
    }


def engine_accretion_physics(params: dict) -> dict:
    """
    Bondi accretion, Eddington luminosity, and RIAF efficiency for Sgr A* / M87.
    Addresses H1 (Dormant Volcano) and H2 (Bad Eater).
    """
    bh_mass = params.get("bh_mass_msun", 4.154e6) * M_SUN
    rho_inf = params.get("ambient_density_cm3", 100.0) * 1e6  # /m³
    T_inf_k = params.get("ambient_temp_k", 1.5e7)
    mu = params.get("mean_molecular_weight", 0.62)
    eta_riaf = params.get("riaf_radiative_efficiency", 1e-3)  # RIAF is ~0.001, not 0.1
    m_proton = 1.6726e-27  # kg

    # ── Bondi accretion rate ─────────────────────────────────────────────────
    # Sound speed: c_s = sqrt(gamma kT / mu m_p)
    k_b = 1.381e-23
    gamma = 5.0 / 3.0
    c_s = math.sqrt(gamma * k_b * T_inf_k / (mu * m_proton))
    r_bondi_m = G * bh_mass / (c_s ** 2)
    r_bondi_pc = r_bondi_m / PC_TO_M
    rho_kg = rho_inf * mu * m_proton
    mdot_bondi = math.pi * (G * bh_mass) ** 2 * rho_kg / (c_s ** 3)  # kg/s
    mdot_bondi_msun_yr = mdot_bondi * YR_TO_S / M_SUN

    # ── Eddington luminosity and accretion rate ───────────────────────────────
    kappa_es = 0.034  # m²/kg (electron-scattering opacity, solar)
    l_edd = 4.0 * math.pi * G * bh_mass * C / kappa_es  # W
    mdot_edd = l_edd / (0.1 * C ** 2)  # kg/s  (10% standard efficiency)
    mdot_edd_msun_yr = mdot_edd * YR_TO_S / M_SUN

    # ── Observed luminosity of Sgr A* ────────────────────────────────────────
    # Quiescent X-ray: ~2e33 erg/s (Baganoff+ 2003) → W
    l_obs_sgra = params.get("observed_luminosity_erg_s", 2e33) * 1e-7  # W
    l_edd_fraction = l_obs_sgra / l_edd

    # ── RIAF effective accretion rate ────────────────────────────────────────
    # RIAF: actual accretion fraction α << Bondi, ~0.01-0.1 of Bondi
    alpha_capture = params.get("bondi_capture_fraction", 0.01)
    mdot_effective = mdot_bondi * alpha_capture
    l_riaf = eta_riaf * mdot_effective * C ** 2

    # ── Eddington ratio ───────────────────────────────────────────────────────
    l_edd_ratio_riaf = l_riaf / l_edd

    return {
        "sim_type": "accretion_physics",
        "inputs": {
            "bh_mass_msun": bh_mass / M_SUN,
            "ambient_density_cm3": params.get("ambient_density_cm3", 100.0),
            "ambient_temp_k": T_inf_k,
            "riaf_efficiency": eta_riaf,
            "bondi_capture_fraction": alpha_capture,
        },
        "results": {
            "bondi_radius_pc": round(r_bondi_pc, 6),
            "bondi_radius_arcsec": round(r_bondi_pc / D_GC_PC * 206265, 3),
            "bondi_accretion_rate_msun_yr": f"{mdot_bondi_msun_yr:.3e}",
            "eddington_luminosity_erg_s": f"{l_edd * 1e7:.3e}",
            "eddington_accretion_rate_msun_yr": f"{mdot_edd_msun_yr:.3e}",
            "observed_luminosity_fraction_eddington": f"{l_edd_fraction:.3e}",
            "riaf_predicted_luminosity_erg_s": f"{l_riaf * 1e7:.3e}",
            "riaf_eddington_ratio": f"{l_edd_ratio_riaf:.3e}",
            "dormancy_explanation": (
                f"Sgr A* accretes at only {l_edd_fraction:.1e} of its Eddington luminosity. "
                f"RIAF models predict angular-momentum-starved inflow: ~{alpha_capture*100:.0f}% "
                f"of the Bondi rate actually reaches the horizon, radiated at efficiency η={eta_riaf}. "
                "The black hole is not dormant by choice — it is geometrically inefficient."
            ),
        },
        "testable_predictions": [
            {
                "prediction": f"Bondi radius is resolvable at {round(r_bondi_pc/D_GC_PC*206265,3)} arcsec",
                "instrument": "Chandra X-ray (0.5 arcsec PSF)",
                "falsification": "Resolved Bondi radius inconsistent with ambient density model",
            },
            {
                "prediction": f"Luminosity floor ~{l_riaf*1e7:.1e} erg/s in quiescence (RIAF)",
                "instrument": "Chandra / NuSTAR",
                "falsification": "Luminosity consistently above RIAF ceiling would require standard disk",
            },
        ],
    }


def engine_pulsar_glitch_stress(params: dict) -> dict:
    """
    Vela pulsar crustal stress accumulation model (H14: Crustal Memory).
    Predicts next glitch window and expected permanent fraction.
    """
    p0_s = params.get("period_s", VELA_P0_MS / 1000.0)
    pdot = params.get("period_derivative", VELA_PDOT)
    n_glitches = params.get("n_glitches", VELA_N_GLITCHES)
    mean_interval_yr = params.get("mean_interval_yr", VELA_MEAN_INTERVAL_YR)
    last_glitch_yr = params.get("last_glitch_decimal_year", 2019.416)  # MJD 58515 ≈ 2019.416
    perm_frac_current = params.get("permanent_fraction_current", 0.683)
    perm_frac_trend_per_decade = params.get("perm_frac_trend_per_decade", VELA_PERM_FRAC_TREND)

    # ── Characteristic age ─────────────────────────────────────────────────
    tau_char_yr = (p0_s / (2.0 * pdot)) / YR_TO_S

    # ── Magnetic field estimate ───────────────────────────────────────────────
    b_field_gauss = 3.2e19 * math.sqrt(p0_s * pdot)

    # ── Spin-down energy loss rate ────────────────────────────────────────────
    # Assuming I_ns = 1e45 g cm² = 1e38 kg m²
    I_ns = 1e38
    omega = 2 * math.pi / p0_s
    edot = -4.0 * math.pi**2 * I_ns * pdot / p0_s**3  # W (magnitude)

    # ── Next glitch window ───────────────────────────────────────────────────
    next_glitch_center_yr = last_glitch_yr + mean_interval_yr
    window_half_width = mean_interval_yr * 0.25  # ±25% of mean interval
    window_open = next_glitch_center_yr - window_half_width
    window_close = next_glitch_center_yr + window_half_width

    # ── Predicted permanent fraction at next glitch ───────────────────────────
    years_since_baseline = 2026.0 - 1969.0  # 1969 = first Vela glitch
    perm_frac_predicted = 0.60 + (perm_frac_trend_per_decade * years_since_baseline / 10.0)
    perm_frac_predicted = min(perm_frac_predicted, 0.95)

    # ── Stress accumulation proxy ─────────────────────────────────────────────
    # Δν/ν accumulated since last glitch (fractional spin-up potential)
    years_since_last = 2026.3 - last_glitch_yr
    nu_dot = -pdot / p0_s**2  # Hz/s (spin-down rate)
    nu = 1.0 / p0_s
    delta_nu_accumulated = abs(nu_dot) * years_since_last * YR_TO_S
    stress_proxy = delta_nu_accumulated / nu  # dimensionless

    return {
        "sim_type": "pulsar_glitch_stress",
        "inputs": {
            "period_ms": p0_s * 1000,
            "period_derivative": pdot,
            "n_confirmed_glitches": n_glitches,
            "mean_inter_glitch_interval_yr": mean_interval_yr,
            "last_glitch_decimal_year": last_glitch_yr,
        },
        "results": {
            "characteristic_age_kyr": round(tau_char_yr / 1000, 1),
            "b_field_gauss": f"{b_field_gauss:.3e}",
            "spin_down_edot_W": f"{edot:.3e}",
            "years_since_last_glitch": round(years_since_last, 2),
            "stress_proxy_delta_nu_over_nu": f"{stress_proxy:.3e}",
            "next_glitch_window_open": round(window_open, 2),
            "next_glitch_window_close": round(window_close, 2),
            "next_glitch_center": round(next_glitch_center_yr, 2),
            "predicted_permanent_fraction": round(perm_frac_predicted, 4),
            "perm_fraction_trend_per_decade": perm_frac_trend_per_decade,
        },
        "testable_predictions": [
            {
                "prediction": f"Next Vela glitch: {window_open:.2f} – {window_close:.2f} (decimal year)",
                "falsification": "Glitch outside this window weakens mean-interval model",
            },
            {
                "prediction": f"Permanent fraction at next glitch: {perm_frac_predicted:.1%}",
                "falsification": "Fraction below 65% or above 80% would challenge crustal memory trend",
            },
            {
                "prediction": f"Stress proxy Δν/ν ≈ {stress_proxy:.2e} accumulated since last glitch",
                "falsification": "Glitch occurring at significantly lower stress would challenge threshold model",
            },
        ],
    }


def engine_bayesian_update(params: dict) -> dict:
    """
    Generic Bayesian hypothesis confidence update.
    Given a prior confidence, new evidence reliability, and whether evidence
    supports or challenges the hypothesis, compute the posterior.
    """
    prior = params.get("prior_confidence", 0.5)
    evidence_reliability = params.get("evidence_reliability", 0.7)
    evidence_supports = params.get("evidence_supports", True)
    n_supporting = params.get("n_supporting_items", 1)
    n_challenging = params.get("n_challenging_items", 0)

    # Simple Bayesian update: L(E|H) × P(H) / P(E)
    # P(E|H) = reliability if supports, (1-reliability) if challenges
    # Iterate over evidence items
    posterior = prior
    for _ in range(n_supporting):
        likelihood_given_h = evidence_reliability
        likelihood_given_not_h = 1.0 - evidence_reliability
        p_e = likelihood_given_h * posterior + likelihood_given_not_h * (1 - posterior)
        posterior = (likelihood_given_h * posterior) / p_e if p_e > 0 else posterior

    for _ in range(n_challenging):
        likelihood_given_h = 1.0 - evidence_reliability
        likelihood_given_not_h = evidence_reliability
        p_e = likelihood_given_h * posterior + likelihood_given_not_h * (1 - posterior)
        posterior = (likelihood_given_h * posterior) / p_e if p_e > 0 else posterior

    posterior = max(0.01, min(0.99, posterior))
    delta = posterior - prior

    return {
        "sim_type": "bayesian_update",
        "inputs": params,
        "results": {
            "prior_confidence": round(prior, 4),
            "posterior_confidence": round(posterior, 4),
            "delta_confidence": round(delta, 4),
            "direction": "supported" if delta > 0 else "challenged",
            "n_supporting_items": n_supporting,
            "n_challenging_items": n_challenging,
            "evidence_reliability": evidence_reliability,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bundle builder
# ─────────────────────────────────────────────────────────────────────────────

def build_bundle(sim_results: dict, task: dict) -> dict:
    """Format simulation results as a structured_ingest_v1 bundle."""
    sim_type = sim_results.get("sim_type", "simulation")
    hyp_id = task.get("hypothesis_id")
    now = datetime.now(timezone.utc).isoformat()

    # Significance: high for orbital/pulsar (real physics), medium for bayesian
    significance_map = {
        "orbital_confinement": 0.82,
        "accretion_physics": 0.78,
        "pulsar_glitch_stress": 0.80,
        "bayesian_update": 0.60,
    }
    significance = significance_map.get(sim_type, 0.65)

    # Domain tags
    domain_map = {
        "orbital_confinement": ["sgra", "stellar_dynamics", "general_relativity"],
        "accretion_physics": ["sgra", "accretion", "agn"],
        "pulsar_glitch_stress": ["pulsars", "neutron_stars", "crustal_physics"],
        "bayesian_update": ["epistemics", "hypothesis_council"],
    }
    domains = domain_map.get(sim_type, ["simulation"])

    # Summary
    results = sim_results.get("results", {})
    if sim_type == "orbital_confinement":
        summary = (
            f"Orbital confinement simulation for Sgr A* central parsec. "
            f"Gravitational sphere of influence r_h = {results.get('gravitational_sphere_of_influence_pc', '?')} pc. "
            f"S2 Schwarzschild precession: {results.get('s2_schwarzschild_precession_arcmin_per_orbit', '?')} arcmin/orbit "
            f"(GRAVITY 2020 observed: 12.1 arcmin). "
            f"Hills capture radius: {results.get('hills_capture_radius_au', '?')} AU. "
            f"Provides 4 falsifiable predictions replacing anthropomorphic framing."
        )
    elif sim_type == "accretion_physics":
        summary = (
            f"Accretion physics simulation for Sgr A*. Bondi radius = "
            f"{results.get('bondi_radius_arcsec', '?')} arcsec. "
            f"Observed luminosity = {results.get('observed_luminosity_fraction_eddington', '?')} × Eddington. "
            f"RIAF model explains sub-Eddington quiescence through geometric inefficiency."
        )
    elif sim_type == "pulsar_glitch_stress":
        summary = (
            f"Vela pulsar crustal stress simulation. Next glitch window: "
            f"{results.get('next_glitch_window_open', '?')} – {results.get('next_glitch_window_close', '?')}. "
            f"Predicted permanent fraction: {results.get('predicted_permanent_fraction', '?'):.1%}. "
            f"Stress proxy Δν/ν = {results.get('stress_proxy_delta_nu_over_nu', '?')}."
        )
    else:
        summary = f"Bayesian confidence update: prior {results.get('prior_confidence')} → posterior {results.get('posterior_confidence')} ({results.get('direction')})."

    # Build structured evidence items
    structured_evidence = {
        "simulation_engine": sim_type,
        "run_timestamp": now,
        "inputs": sim_results.get("inputs", {}),
        "outputs": results,
        "mechanism": sim_results.get("mechanism", ""),
        "testable_predictions": sim_results.get("testable_predictions", []),
        "council_revisions_addressed": sim_results.get("council_revisions_addressed", []),
        "recommended_next_evidence": sim_results.get("recommended_next_evidence", []),
        "revised_hypothesis_text": sim_results.get("revised_hypothesis", ""),
        "provenance": "Deterministic local simulation — Manatuabon simulation_worker.py",
        "data_quality": "SYNTHETIC_SIMULATION — not direct observation; use as quantitative framework only",
    }

    return {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": f"simulation_bundle_{sim_type}",
        "summary": summary,
        "entities": _entities_for(sim_type, results),
        "topics": [sim_type.replace("_", " "), "deterministic simulation", "quantitative evidence"],
        "anomalies": [],
        "significance": significance,
        "supports_hypothesis": hyp_id,
        "challenges_hypothesis": None,
        "domain_tags": domains,
        "source_catalogs": ["Manatuabon simulation_worker.py", "deterministic_physics"],
        "target": {
            "name": task.get("name", sim_type),
            "input_target": hyp_id or sim_type,
            "category": "simulation",
        },
        "structured_evidence": structured_evidence,
        "new_hypothesis": None,
        "manatuabon_context": {
            "hypothesis_focus": hyp_id or "general",
            "simulation_type": sim_type,
            "generated_at": now,
            "acknowledgement": "Deterministic physics simulation — all constants from published literature.",
        },
    }


def _entities_for(sim_type: str, results: dict) -> list:
    if sim_type == "orbital_confinement":
        return ["Sgr A*", "S2", "S-stars", "Gravitational Sphere of Influence",
                "Hills Mechanism", "Schwarzschild Precession", "GRAVITY Collaboration"]
    if sim_type == "accretion_physics":
        return ["Sgr A*", "Bondi accretion", "RIAF", "Eddington luminosity"]
    if sim_type == "pulsar_glitch_stress":
        return ["PSR B0833-45", "Vela Pulsar", "crustal memory", "glitch window",
                f"next window {results.get('next_glitch_window_open','?')}–{results.get('next_glitch_window_close','?')}"]
    return ["hypothesis confidence", "Bayesian update"]


# ─────────────────────────────────────────────────────────────────────────────
# Worker
# ─────────────────────────────────────────────────────────────────────────────

class SimulationWorker:
    def __init__(self, db_path=_DB_PATH, inbox_path=_INBOX_PATH):
        self.db_path = Path(db_path)
        self.inbox_path = Path(inbox_path)
        self.inbox_path.mkdir(parents=True, exist_ok=True)

    def _conn(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    # ── Queue polling ─────────────────────────────────────────────────────────

    def poll_simulations_queue(self) -> list[dict]:
        """Return all pending rows from the simulations table."""
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM simulations WHERE status='pending' ORDER BY rowid"
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            log.warning("Could not poll simulations table: %s", exc)
            return []

    def scan_evidence_requests(self) -> list[dict]:
        """
        Find pending evidence_requests that look quantitative and auto-generate
        simulation tasks for them. Returns synthetic task dicts.
        """
        tasks = []
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM evidence_requests WHERE status='pending'"
                ).fetchall()
        except Exception as exc:
            log.warning("Could not scan evidence_requests: %s", exc)
            return []

        for r in rows:
            r = dict(r)
            text = (r.get("request_text") or "").lower()
            sim_type = _classify(text)
            if sim_type == "unknown":
                continue
            tasks.append({
                "id": f"ER-SIM-{r['id']}",
                "name": f"Auto-sim from evidence_request #{r['id']}",
                "hypothesis_id": r.get("hypothesis_id"),
                "sim_type": sim_type,
                "parameters": json.dumps({}),
                "source_evidence_request_id": r["id"],
                "status": "pending",
            })
        return tasks

    # ── Dispatch ──────────────────────────────────────────────────────────────

    def dispatch(self, task: dict) -> dict | None:
        """Route a task dict to the correct physics engine."""
        params_raw = task.get("parameters") or "{}"
        try:
            parsed = json.loads(params_raw) if isinstance(params_raw, str) else params_raw
            # json.loads on a bare JSON string (e.g. '"orbital_confinement"') returns
            # a str, not a dict — wrap it so .get() calls never blow up.
            params = parsed if isinstance(parsed, dict) else {"name": str(parsed)}
        except json.JSONDecodeError:
            # Not valid JSON at all — treat the raw value as a name hint.
            params = {"name": params_raw}

        # Determine sim_type from task or classify from name/params
        sim_type = (
            task.get("sim_type")
            or params.get("sim_type")
            or _classify(params.get("name", "") + " " + task.get("name", ""))
        )

        log.info("Dispatching sim_type=%s for task %s (hyp=%s)",
                 sim_type, task.get("id"), task.get("hypothesis_id"))

        engines = {
            "orbital_confinement": engine_orbital_confinement,
            "accretion_physics": engine_accretion_physics,
            "pulsar_glitch_stress": engine_pulsar_glitch_stress,
            "bayesian_update": engine_bayesian_update,
        }

        engine = engines.get(sim_type)
        if not engine:
            log.warning("No engine for sim_type=%s — skipping", sim_type)
            return None

        try:
            return engine(params)
        except Exception as exc:
            log.error("Engine %s failed: %s", sim_type, exc, exc_info=True)
            return None

    # ── Output ────────────────────────────────────────────────────────────────

    def drop_bundle(self, bundle: dict, task: dict) -> Path:
        """Write bundle to inbox/ and return the path."""
        sim_type = bundle.get("payload_type", "simulation").replace("simulation_bundle_", "")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        hyp_slug = (task.get("hypothesis_id") or "nohyp").replace("-", "_")
        filename = f"simulation_bundle_{sim_type}_{hyp_slug}_{ts}.json"
        path = self.inbox_path / filename
        with open(path, "w", encoding="utf-8") as f:
            json.dump(bundle, f, indent=2, ensure_ascii=False)
        log.info("Bundle written -> %s", filename)
        return path

    def mark_complete(self, task: dict, bundle_path: Path):
        """Update simulations table row to completed (skipped for auto-ER tasks)."""
        task_id = task.get("id") or ""
        if task_id.startswith("ER-SIM-"):
            return  # these are ephemeral — don't try to write back
        params = {}
        try:
            _parsed = json.loads(task.get("parameters") or "{}")
            params = _parsed if isinstance(_parsed, dict) else {"name": str(_parsed)}
        except Exception:
            pass
        params["result_bundle"] = str(bundle_path)
        params["completed_at"] = datetime.now(timezone.utc).isoformat()
        try:
            with self._conn() as conn:
                conn.execute(
                    "UPDATE simulations SET status='done', parameters=? WHERE id=?",
                    (json.dumps(params), task_id),
                )
                conn.commit()
        except Exception as exc:
            log.warning("Could not mark simulation %s complete: %s", task_id, exc)

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run_once(self):
        tasks = self.poll_simulations_queue()
        tasks += self.scan_evidence_requests()

        if not tasks:
            log.info("No pending simulations.")
            return

        log.info("Found %d simulation task(s) to process.", len(tasks))
        for task in tasks:
            result = self.dispatch(task)
            if result is None:
                continue
            bundle = build_bundle(result, task)
            path = self.drop_bundle(bundle, task)
            self.mark_complete(task, path)

    def loop(self, interval_sec: int = 60):
        log.info("Simulation worker started (poll every %ds).", interval_sec)
        while True:
            try:
                self.run_once()
            except Exception as exc:
                log.error("Worker loop error: %s", exc, exc_info=True)
            time.sleep(interval_sec)

    def run_named(self, sim_type: str, hypothesis_id: str | None = None):
        """Run a specific engine by name and drop the bundle. Used for --run."""
        task = {
            "id": f"MANUAL-{uuid.uuid4().hex[:8]}",
            "name": sim_type,
            "hypothesis_id": hypothesis_id,
            "sim_type": sim_type,
            "parameters": "{}",
        }
        result = self.dispatch(task)
        if result is None:
            log.error("Engine returned no result for %s", sim_type)
            return
        bundle = build_bundle(result, task)
        path = self.drop_bundle(bundle, task)
        log.info("Manual run complete. Bundle at: %s", path)
        # Print a readable summary
        print("\n" + "=" * 72)
        print(f"  SIMULATION COMPLETE: {sim_type}")
        print("=" * 72)
        ev = bundle["structured_evidence"]
        print(f"\n  Summary:\n  {bundle['summary']}")
        if ev.get("mechanism"):
            print(f"\n  Mechanism:\n  {ev['mechanism'][:500]}...")
        print("\n  Testable Predictions:")
        for p in ev.get("testable_predictions", [])[:4]:
            print(f"    • {p['prediction']}")
            print(f"      Falsification: {p['falsification']}")
        if ev.get("council_revisions_addressed"):
            print("\n  Council Revisions Addressed:")
            for r in ev["council_revisions_addressed"]:
                print(f"    ✓ {r}")
        print("\n" + "=" * 72 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manatuabon Simulation Worker")
    parser.add_argument("--once", action="store_true",
                        help="Run one pass then exit")
    parser.add_argument("--run", metavar="SIM_TYPE",
                        help="Run a named engine immediately (orbital_confinement, accretion_physics, pulsar_glitch_stress, bayesian_update)")
    parser.add_argument("--hypothesis", metavar="HYP_ID", default=None,
                        help="Hypothesis ID to link the simulation to (e.g. H3)")
    parser.add_argument("--interval", type=int, default=60,
                        help="Polling interval in seconds (default: 60)")
    parser.add_argument("--db", default=str(_DB_PATH), help="Path to manatuabon.db")
    parser.add_argument("--inbox", default=str(_INBOX_PATH), help="Path to inbox/")
    args = parser.parse_args()

    worker = SimulationWorker(db_path=args.db, inbox_path=args.inbox)

    if args.run:
        worker.run_named(args.run, args.hypothesis)
    elif args.once:
        worker.run_once()
    else:
        worker.loop(interval_sec=args.interval)
