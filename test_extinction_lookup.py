"""Tests for extinction_lookup.py — coordinate conversion, E(B-V), and dereddening."""

import math
import unittest

from extinction_lookup import (
    SF11_COEFFICIENTS,
    dereddened_color,
    deredden_mag,
    extinction_a_band,
    galactic_coords,
    galactic_ebv,
)


class TestSF11Coefficients(unittest.TestCase):
    """Coefficient table should contain all standard bands."""

    EXPECTED_BANDS = [
        "sdss_u", "sdss_g", "sdss_r", "sdss_i", "sdss_z",
        "ps_g", "ps_r", "ps_i", "ps_z", "ps_y",
        "G", "BP", "RP",
    ]

    def test_all_bands_present(self):
        for band in self.EXPECTED_BANDS:
            self.assertIn(band, SF11_COEFFICIENTS, f"Missing coefficient for {band}")

    def test_all_coefficients_positive(self):
        for band, coeff in SF11_COEFFICIENTS.items():
            self.assertGreater(coeff, 0.0, f"Non-positive coefficient for {band}")

    def test_sdss_r_value(self):
        """SF11 Table 6: R_r = 2.285 for SDSS r-band."""
        self.assertAlmostEqual(SF11_COEFFICIENTS["sdss_r"], 2.285, places=3)


class TestGalacticCoords(unittest.TestCase):
    """Verify galactic coordinate transform against known landmarks."""

    def test_galactic_center(self):
        """RA=266.4°, Dec=-28.9° should be near Galactic center (l≈0, b≈0)."""
        l_deg, b_deg = galactic_coords(266.4, -28.9)
        self.assertAlmostEqual(b_deg, 0.0, delta=2.0)

    def test_galactic_pole(self):
        """North Galactic Pole is near RA=192.86, Dec=27.13 → b≈90."""
        l_deg, b_deg = galactic_coords(192.86, 27.13)
        self.assertAlmostEqual(b_deg, 90.0, delta=1.0)

    def test_output_range(self):
        l_deg, b_deg = galactic_coords(120.0, 45.0)
        self.assertGreaterEqual(l_deg, 0.0)
        self.assertLess(l_deg, 360.0)
        self.assertGreaterEqual(b_deg, -90.0)
        self.assertLessEqual(b_deg, 90.0)


class TestGalacticEBV(unittest.TestCase):
    """E(B-V) lookup returns (float, str) and respects high-latitude expectations."""

    def test_returns_tuple(self):
        ebv, method = galactic_ebv(180.0, 80.0)
        self.assertIsInstance(ebv, float)
        self.assertIn(method, ("sfd", "analytical_csc_b"))

    def test_high_latitude_low_extinction(self):
        """At high Galactic latitude, extinction should be modest."""
        ebv, _ = galactic_ebv(192.86, 27.13)  # near NGP
        self.assertLess(ebv, 0.15)

    def test_low_latitude_higher_extinction(self):
        """Near the Galactic plane, analytical model should give higher E(B-V)."""
        ebv_plane, _ = galactic_ebv(266.4, -28.9)  # near GC
        ebv_pole, _ = galactic_ebv(192.86, 27.13)  # near NGP
        self.assertGreater(ebv_plane, ebv_pole)

    def test_positive_ebv(self):
        ebv, _ = galactic_ebv(0.0, 0.0)
        self.assertGreaterEqual(ebv, 0.0)


class TestDereddenMag(unittest.TestCase):
    """Magnitude dereddening: m_0 = m_obs - R_λ × E(B-V)."""

    def test_basic_correction(self):
        result = deredden_mag(18.0, 0.1, "sdss_r")
        expected = 18.0 - SF11_COEFFICIENTS["sdss_r"] * 0.1
        self.assertAlmostEqual(result, expected, places=5)

    def test_zero_ebv_no_change(self):
        result = deredden_mag(18.0, 0.0, "ps_g")
        self.assertAlmostEqual(result, 18.0, places=5)

    def test_unknown_band_returns_none(self):
        self.assertIsNone(deredden_mag(18.0, 0.1, "invalid_band"))


class TestExtinctionABand(unittest.TestCase):
    def test_known_band(self):
        a = extinction_a_band(0.5, "ps_g")
        self.assertAlmostEqual(a, SF11_COEFFICIENTS["ps_g"] * 0.5, places=5)

    def test_unknown_band(self):
        self.assertIsNone(extinction_a_band(0.5, "bad_band"))


class TestDereddenedColor(unittest.TestCase):
    """Dereddened color = (m_left - R_left*E) - (m_right - R_right*E)."""

    def test_basic_color_correction(self):
        g_mag, r_mag, ebv = 18.5, 17.8, 0.2
        raw_color = g_mag - r_mag
        dered = dereddened_color(g_mag, r_mag, ebv, "ps_g", "ps_r")
        # Dereddened color should differ from raw by (R_g - R_r)*E(B-V)
        expected_shift = (SF11_COEFFICIENTS["ps_g"] - SF11_COEFFICIENTS["ps_r"]) * ebv
        self.assertAlmostEqual(dered, raw_color - expected_shift, places=5)

    def test_zero_ebv_equals_raw(self):
        dered = dereddened_color(18.5, 17.8, 0.0, "ps_g", "ps_r")
        self.assertAlmostEqual(dered, 18.5 - 17.8, places=5)

    def test_unknown_band_returns_none(self):
        self.assertIsNone(dereddened_color(18.5, 17.8, 0.1, "bad", "ps_r"))
        self.assertIsNone(dereddened_color(18.5, 17.8, 0.1, "ps_g", "bad"))


if __name__ == "__main__":
    unittest.main()
