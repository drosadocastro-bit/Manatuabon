"""Galactic extinction E(B-V) lookup and per-band dereddening.

Provides two lookup backends:
  1. dustmaps SFD98 — accurate per-sightline reddening when map files are installed.
  2. Analytical csc(|b|) fallback — rough Galactic-latitude-based estimate when map
     data is not available.  Clearly labeled as approximate.

The fallback is conservative: it is adequate for flagging whether extinction is
significant at a given sightline, but not for publication-grade photometric
correction.  When dustmaps is unavailable, an ``extinction_method`` field in every
output row records ``"analytical_csc_b"`` so downstream consumers can audit the
source of the correction.

Dereddening coefficients are from Schlafly & Finkbeiner 2011 (Table 6, R_V = 3.1).
"""

from __future__ import annotations

import math

# ---------------------------------------------------------------------------
# Schlafly & Finkbeiner 2011 (Table 6) extinction coefficients A_lambda / E(B-V)
# Keyed by common band identifiers used across SDSS and Pan-STARRS.
# ---------------------------------------------------------------------------
SF11_COEFFICIENTS: dict[str, float] = {
    # SDSS bands
    "sdss_u": 4.239,
    "sdss_g": 3.303,
    "sdss_r": 2.285,
    "sdss_i": 1.698,
    "sdss_z": 1.263,
    # Pan-STARRS bands
    "ps_g": 3.172,
    "ps_r": 2.271,
    "ps_i": 1.682,
    "ps_z": 1.322,
    "ps_y": 1.087,
    # Generic broadband aliases
    "G": 2.740,   # approximate Gaia G
    "BP": 3.374,  # approximate Gaia BP
    "RP": 2.035,  # approximate Gaia RP
}


# ---------------------------------------------------------------------------
# Coordinate conversion: ICRS (RA, Dec) → Galactic (l, b)
# Uses astropy when available; pure-math fallback otherwise.
# ---------------------------------------------------------------------------

_USE_ASTROPY: bool | None = None


def _try_astropy_galactic(ra_deg: float, dec_deg: float) -> tuple[float, float]:
    """Return (l_deg, b_deg) using astropy."""
    from astropy.coordinates import SkyCoord
    import astropy.units as u

    coord = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame="icrs")
    galactic = coord.galactic
    return float(galactic.l.deg), float(galactic.b.deg)


def _pure_math_galactic(ra_deg: float, dec_deg: float) -> tuple[float, float]:
    """Approximate ICRS-to-Galactic using the J2000 rotation pole."""
    ra = math.radians(ra_deg)
    dec = math.radians(dec_deg)
    # North Galactic Pole in J2000 ICRS
    ra_ngp = math.radians(192.85948)
    dec_ngp = math.radians(27.12825)
    l_ncp = math.radians(122.93192)

    sin_b = (
        math.sin(dec_ngp) * math.sin(dec)
        + math.cos(dec_ngp) * math.cos(dec) * math.cos(ra - ra_ngp)
    )
    b = math.asin(max(-1.0, min(1.0, sin_b)))
    y = math.cos(dec) * math.sin(ra - ra_ngp)
    x = math.cos(dec_ngp) * math.sin(dec) - math.sin(dec_ngp) * math.cos(dec) * math.cos(ra - ra_ngp)
    l = l_ncp - math.atan2(y, x)
    l_deg = math.degrees(l) % 360.0
    b_deg = math.degrees(b)
    return l_deg, b_deg


def galactic_coords(ra_deg: float, dec_deg: float) -> tuple[float, float]:
    """Return Galactic (l, b) in degrees for a given ICRS (RA, Dec)."""
    global _USE_ASTROPY
    if _USE_ASTROPY is None:
        try:
            _try_astropy_galactic(0.0, 0.0)
            _USE_ASTROPY = True
        except Exception:
            _USE_ASTROPY = False
    if _USE_ASTROPY:
        return _try_astropy_galactic(ra_deg, dec_deg)
    return _pure_math_galactic(ra_deg, dec_deg)


# ---------------------------------------------------------------------------
# E(B-V) lookup
# ---------------------------------------------------------------------------

_DUSTMAPS_AVAILABLE: bool | None = None
_SFD_QUERIER = None


def _ensure_sfd():
    """Try to create an SFD querier.  Returns True if usable."""
    global _DUSTMAPS_AVAILABLE, _SFD_QUERIER
    if _DUSTMAPS_AVAILABLE is not None:
        return _DUSTMAPS_AVAILABLE
    try:
        from dustmaps.sfd import SFDQuery
        _SFD_QUERIER = SFDQuery()
        _DUSTMAPS_AVAILABLE = True
    except Exception:
        _DUSTMAPS_AVAILABLE = False
    return _DUSTMAPS_AVAILABLE


def _sfd_ebv(ra_deg: float, dec_deg: float) -> float:
    """Query the SFD98 dust map for line-of-sight E(B-V)."""
    from astropy.coordinates import SkyCoord
    import astropy.units as u

    coord = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame="icrs")
    return float(_SFD_QUERIER(coord))


def _analytical_ebv(ra_deg: float, dec_deg: float) -> float:
    """Rough csc(|b|) estimate: E(B-V) ≈ 0.03 * csc(|b|), capped at 5.0 mag.

    This is a gross simplification adequate only for flagging whether extinction
    matters at a given sightline.  At |b| < 5° values become unreliable.
    """
    _, b_deg = galactic_coords(ra_deg, dec_deg)
    abs_b = max(abs(b_deg), 2.0)  # avoid singularity at b=0
    ebv = 0.03 / math.sin(math.radians(abs_b))
    return round(min(ebv, 5.0), 6)


def galactic_ebv(ra_deg: float, dec_deg: float) -> tuple[float, str]:
    """Return (E(B-V), method) for a given ICRS sightline.

    ``method`` is ``"sfd"`` when dustmaps data is available, or
    ``"analytical_csc_b"`` when using the rough Galactic-latitude model.
    """
    if _ensure_sfd():
        return round(_sfd_ebv(ra_deg, dec_deg), 6), "sfd"
    return _analytical_ebv(ra_deg, dec_deg), "analytical_csc_b"


# ---------------------------------------------------------------------------
# Dereddening helpers
# ---------------------------------------------------------------------------

def deredden_mag(observed_mag: float, ebv: float, band: str) -> float | None:
    """Return the extinction-corrected magnitude: m_0 = m_obs - R_λ × E(B-V).

    Returns None if the band is not in the coefficient table.
    """
    coeff = SF11_COEFFICIENTS.get(band)
    if coeff is None:
        return None
    return round(observed_mag - coeff * ebv, 6)


def extinction_a_band(ebv: float, band: str) -> float | None:
    """Return A_λ = R_λ × E(B-V) for a given band.  None if band unknown."""
    coeff = SF11_COEFFICIENTS.get(band)
    if coeff is None:
        return None
    return round(coeff * ebv, 6)


def dereddened_color(mag_left: float, mag_right: float, ebv: float, band_left: str, band_right: str) -> float | None:
    """Return extinction-corrected color index (mag_left - mag_right).

    This is equivalent to computing both dereddened magnitudes and differencing,
    but avoids intermediate rounding.
    """
    coeff_left = SF11_COEFFICIENTS.get(band_left)
    coeff_right = SF11_COEFFICIENTS.get(band_right)
    if coeff_left is None or coeff_right is None:
        return None
    corrected_left = mag_left - coeff_left * ebv
    corrected_right = mag_right - coeff_right * ebv
    return round(corrected_left - corrected_right, 6)
