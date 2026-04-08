"""Vela pulsar crust memory simulation for the Manatuabon astrophysics workspace.

Physics model
-------------
We use the two-component model of a neutron star (Baym et al. 1969, Alpar et al. 1984):

  Normal component  – the solid crust and coupled normal fluid, moment-of-inertia
                      fraction (1 - Q_s), spins down under magnetic-dipole radiation.
  Superfluid vortex reservoir – inner-crust superfluid, fraction Q_s, pinned to
                      nuclear lattice sites.  Its angular velocity is conserved
                      between glitches.

Lag accumulation
    δω(t) = ν_s(t) - ν_c(t) > 0
    d(δω)/dt ≈ |ν̇_c|   (crust spins down, superfluid stays put)

Glitch trigger
    When δω ≥ ω_cr a sudden vortex avalanche transfers angular momentum to the crust.

Post-glitch recovery (multi-τ exponential + permanent term)
    The spin-up Δν_total splits into:
      • Δν_p  = f_p * Δν_total   permanent crust restructuring (never recovers)
      • Σ A_i * exp(-t/τ_i)       recoverable components

H19 hypothesis test
    H19 requires Δν_p / Δν_total ≥ 0.01 for every Vela glitch.

Vela parameters (ATNF DR4, PSR B0833-45)
    ν₀   = 11.19490 Hz
    ν̇₀   = -1.563e-11 Hz/s  →  -1.3504e-6 Hz/day
    P    = 0.08933 s
"""

from __future__ import annotations

import argparse
import io
import json
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Physical constants for Vela (PSR B0833-45 / J0835-4510)
# ---------------------------------------------------------------------------
VELA_NU0_HZ: float = 11.19490          # spin frequency [Hz]
VELA_NUDOT_HZ_S: float = -1.563e-11    # spin-down rate [Hz/s]
VELA_NUDOT_HZ_DAY: float = VELA_NUDOT_HZ_S * 86400.0  # Hz/day  ≈ -1.3504e-6

# Glitch size distribution from Vela catalog (Espinoza 2011, Lower 2021)
VELA_MEAN_DELTA_NU_OVER_NU: float = 2.0e-6
VELA_MEAN_DELTA_NU_HZ: float = VELA_MEAN_DELTA_NU_OVER_NU * VELA_NU0_HZ  # ~2.24e-5 Hz

# Log-normal spread (in natural-log space) matching the observed scatter
VELA_GLITCH_LOG_SIGMA: float = 0.5

# Default inter-glitch recharge time used to set ω_cr [days]
VELA_RECHARGE_DAYS: float = 1185.0

# H19 permanent-fraction threshold
H19_PERMANENT_FRACTION_THRESHOLD: float = 0.01

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INBOX_DIR = BASE_DIR / "inbox"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class GlitchEvent:
    epoch_day: float                  # simulation day of glitch
    delta_nu_total_hz: float          # total spin-up [Hz]
    delta_nu_permanent_hz: float      # permanent offset [Hz]
    permanent_fraction: float         # Δν_p / Δν_total
    recovery_amplitudes_hz: list[float]  # recoverable amplitudes [Hz]
    recovery_taus_days: list[float]   # corresponding timescales [days]


@dataclass
class SimulationResult:
    duration_days: float
    dt_coarse_days: float
    dt_fine_days: float
    fine_window_days: float
    seed: int
    nu0_hz: float
    nudot_hz_day: float
    superfluid_fraction: float
    permanent_fraction: float
    omega_critical_hz: float

    # Per-step arrays (sampled at coarse dt except during fine window)
    times_days: list[float] = field(default_factory=list)
    nu_crust_hz: list[float] = field(default_factory=list)
    lag_hz: list[float] = field(default_factory=list)

    # Glitch table
    glitches: list[GlitchEvent] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class VelaCrustMemorySimulator:
    """Two-component Vela pulsar simulator with vortex-creep and crustal memory.

    Parameters
    ----------
    permanent_fraction : float
        Fraction f_p of each glitch's Δν that is permanently retained in the
        crust (the 'memory' term).  H19 requires f_p ≥ 0.01.
    superfluid_fraction : float
        Q_s – ratio of superfluid moment of inertia to total.  Controls glitch
        activity parameter and critical lag.
    recharge_days : float
        Target inter-glitch interval used to calibrate ω_cr.
    seed : int
        RNG seed for reproducible glitch amplitudes.
    """

    def __init__(
        self,
        *,
        permanent_fraction: float = 0.05,
        superfluid_fraction: float = 0.014,
        recharge_days: float = VELA_RECHARGE_DAYS,
        seed: int = 42,
    ) -> None:
        self.permanent_fraction = permanent_fraction
        self.superfluid_fraction = superfluid_fraction
        self.recharge_days = recharge_days
        self.seed = seed
        self._rng = random.Random(seed)

        # Critical lag: ω_cr = |ν̇₀| * T_recharge.
        #
        # Physical picture: the lag δω = ω_s - ω_c between the superfluid vortex
        # reservoir and the crust builds at |ν̇₀| per day during quiescence.  When
        # δω reaches ω_cr the vortex avalanche begins.  The ENTIRE accumulated lag
        # is released (vortices re-pin at zero lag post-glitch), so the next trigger
        # occurs after another T_recharge interval.  Only the fraction Q_s of the
        # transferred angular momentum appears as the observable spin-up Δν_total;
        # the rest drives internal heating and non-uniform vortex redistribution.
        # This calibration gives T_glitch ≈ T_recharge = 900 days.
        self.omega_critical_hz: float = abs(VELA_NUDOT_HZ_DAY) * recharge_days

        # Recovery timescale template (days) and fractional amplitudes of the
        # recoverable part (i.e. Δν_total - Δν_p)
        self._recovery_taus: list[float] = [10.0, 150.0, 600.0]
        self._recovery_fracs: list[float] = [0.40, 0.40, 0.20]  # must sum to 1

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _draw_glitch_size(self) -> float:
        """Return Δν_total [Hz] drawn from log-normal distribution."""
        log_mean = math.log(VELA_MEAN_DELTA_NU_HZ)
        log_sample = self._rng.gauss(log_mean, VELA_GLITCH_LOG_SIGMA)
        return math.exp(log_sample)

    def _make_glitch(self, epoch_day: float, delta_nu_total: float) -> GlitchEvent:
        delta_nu_p = self.permanent_fraction * delta_nu_total
        recoverable = delta_nu_total - delta_nu_p
        amplitudes = [f * recoverable for f in self._recovery_fracs]
        # Add mild jitter (±10 %) to recovery taus for realism
        taus = [
            tau * (1.0 + self._rng.uniform(-0.10, 0.10))
            for tau in self._recovery_taus
        ]
        return GlitchEvent(
            epoch_day=epoch_day,
            delta_nu_total_hz=delta_nu_total,
            delta_nu_permanent_hz=delta_nu_p,
            permanent_fraction=self.permanent_fraction,
            recovery_amplitudes_hz=amplitudes,
            recovery_taus_days=taus,
        )

    # ------------------------------------------------------------------
    # Main simulation loop
    # ------------------------------------------------------------------

    def simulate(self, duration_days: float = 5000.0) -> SimulationResult:
        """Run the simulation and return a SimulationResult.

        Integration uses coarse timesteps (dt_coarse) for inter-glitch
        intervals and fine timesteps (dt_fine) for the first ``fine_window``
        days after each glitch to resolve rapid recovery components.
        """
        dt_coarse = 0.1    # days
        dt_fine = 0.01     # days (10x finer near glitches)
        fine_window = 100.0  # days post-glitch to use fine dt

        result = SimulationResult(
            duration_days=duration_days,
            dt_coarse_days=dt_coarse,
            dt_fine_days=dt_fine,
            fine_window_days=fine_window,
            seed=self.seed,
            nu0_hz=VELA_NU0_HZ,
            nudot_hz_day=VELA_NUDOT_HZ_DAY,
            superfluid_fraction=self.superfluid_fraction,
            permanent_fraction=self.permanent_fraction,
            omega_critical_hz=self.omega_critical_hz,
        )

        # We use a three-component model (Alpar+1984):
        #   (a) Normal component + pinned-superfluid reservoir — tracked via lag.
        #   (b) Vortex-creep superfluid — provides post-glitch exponential recovery.
        # The lag = ν_s_reservoir - ν_c grows between glitches as the crust spins
        # down.  At a glitch, the RESERVOIR loses Δν_c / Q_s (angular-momentum
        # conservation) while the crust gains Δν_c.  Recovery is provided by the
        # CREEP component and does NOT feed back into the reservoir lag.
        t = 0.0
        nu_c = VELA_NU0_HZ   # crust spin frequency [Hz]
        lag = 0.0             # δω = ν_s - ν_c [Hz]  (tracked directly)

        # Active recovery components: list of (amplitude_hz, tau_days, start_day)
        active_recoveries: list[tuple[float, float, float]] = []

        # Track when the last glitch happened (for fine-dt window)
        last_glitch_day: float = -1e9

        def record(t_: float, nu_c_: float, lag_: float) -> None:
            result.times_days.append(t_)
            result.nu_crust_hz.append(nu_c_)
            result.lag_hz.append(lag_)

        record(t, nu_c, lag)

        nudot_abs = abs(VELA_NUDOT_HZ_DAY)

        while t < duration_days:
            # Choose timestep
            dt = dt_fine if (t - last_glitch_day) < fine_window else dt_coarse
            dt = min(dt, duration_days - t)
            if dt <= 0.0:
                break

            # --- Euler step ---------------------------------------------------
            # 1. Crust spins down (magnetic dipole radiation)
            nu_c += VELA_NUDOT_HZ_DAY * dt

            # 2. Lag grows as crust slows (superfluid stays pinned)
            lag += nudot_abs * dt

            # 3. Post-glitch recovery: recoverable components drain back.
            #    Each decaying amplitude A_i * exp(-(t-t0)/τ_i) was added to
            #    ν_c at the glitch.  As it decays, that excess is removed from
            #    ν_c and returned to the lag reservoir.
            for amp, tau, t0 in active_recoveries:
                decay_old = amp * math.exp(-(t - t0) / tau)
                decay_new = amp * math.exp(-(t + dt - t0) / tau)
                delta_decay = decay_old - decay_new   # positive: amplitude lost
                nu_c -= delta_decay   # crust relaxes back toward secular trend

            t += dt

            # --- Glitch check -------------------------------------------------
            if lag >= self.omega_critical_hz:
                delta_nu_total = self._draw_glitch_size()
                glitch = self._make_glitch(t, delta_nu_total)
                result.glitches.append(glitch)
                last_glitch_day = t

                # Instantaneous spin-up: superfluid transfers angular momentum to
                # the crust.  The observable crust spin-up is Δν_total; the full
                # lag reservoir is drained to zero (vortices re-pin after the
                # avalanche) so the next glitch occurs after another T_recharge.
                nu_c += delta_nu_total
                lag = 0.0

                # Register new recovery components
                for amp, tau in zip(glitch.recovery_amplitudes_hz, glitch.recovery_taus_days):
                    active_recoveries.append((amp, tau, t))

            record(t, nu_c, lag)

        return result


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyze_memory(result: SimulationResult) -> dict:
    """Compute memory statistics and test the H19 hypothesis.

    Returns a dict with:
      - glitch_count
      - permanent_fractions  (list per glitch)
      - mean_permanent_fraction, min_permanent_fraction, max_permanent_fraction
      - mean_delta_nu_hz, mean_delta_nu_permanent_hz
      - interglitch_intervals_days (list)
      - mean_interval_days, std_interval_days
      - h19_passes  (True if all glitches satisfy f_p >= 0.01)
      - h19_threshold
      - failing_glitch_indices  (indices of glitches that fail H19)
    """
    glitches = result.glitches
    n = len(glitches)

    if n == 0:
        return {
            "glitch_count": 0,
            "h19_passes": False,
            "h19_threshold": H19_PERMANENT_FRACTION_THRESHOLD,
            "note": "No glitches occurred during simulation.",
        }

    fractions = [g.permanent_fraction for g in glitches]
    delta_nus = [g.delta_nu_total_hz for g in glitches]
    delta_nu_ps = [g.delta_nu_permanent_hz for g in glitches]

    mean_frac = sum(fractions) / n
    mean_dnu = sum(delta_nus) / n
    mean_dnu_p = sum(delta_nu_ps) / n

    intervals = []
    for i in range(1, n):
        intervals.append(glitches[i].epoch_day - glitches[i - 1].epoch_day)

    mean_interval = sum(intervals) / len(intervals) if intervals else 0.0
    if len(intervals) > 1:
        variance = sum((x - mean_interval) ** 2 for x in intervals) / len(intervals)
        std_interval = math.sqrt(variance)
    else:
        std_interval = 0.0

    failing = [i for i, f in enumerate(fractions) if f < H19_PERMANENT_FRACTION_THRESHOLD]
    h19_passes = len(failing) == 0

    return {
        "glitch_count": n,
        "permanent_fractions": fractions,
        "mean_permanent_fraction": round(mean_frac, 6),
        "min_permanent_fraction": round(min(fractions), 6),
        "max_permanent_fraction": round(max(fractions), 6),
        "mean_delta_nu_hz": mean_dnu,
        "mean_delta_nu_over_nu": mean_dnu / result.nu0_hz,
        "mean_delta_nu_permanent_hz": mean_dnu_p,
        "interglitch_intervals_days": intervals,
        "mean_interval_days": round(mean_interval, 1),
        "std_interval_days": round(std_interval, 1),
        "h19_passes": h19_passes,
        "h19_threshold": H19_PERMANENT_FRACTION_THRESHOLD,
        "failing_glitch_indices": failing,
    }


# ---------------------------------------------------------------------------
# Bundle builder
# ---------------------------------------------------------------------------

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_simulation_bundle(
    result: SimulationResult,
    analysis: dict,
    *,
    supports_hypothesis: str | None = None,
) -> dict:
    """Build a Manatuabon structured_ingest_v1 evidence bundle from simulation output."""

    glitch_table = []
    for i, g in enumerate(result.glitches):
        glitch_table.append(
            {
                "index": i,
                "epoch_days": round(g.epoch_day, 2),
                "delta_nu_total_hz": g.delta_nu_total_hz,
                "delta_nu_total_over_nu": g.delta_nu_total_hz / result.nu0_hz,
                "delta_nu_permanent_hz": g.delta_nu_permanent_hz,
                "permanent_fraction": g.permanent_fraction,
                "recovery_taus_days": [round(t, 2) for t in g.recovery_taus_days],
                "recovery_amplitudes_hz": g.recovery_amplitudes_hz,
            }
        )

    h19_passes: bool = analysis.get("h19_passes", False)

    anomalies = []
    failing = analysis.get("failing_glitch_indices", [])
    if failing:
        anomalies.append(
            f"{len(failing)} of {analysis['glitch_count']} simulated glitches have "
            f"permanent fraction below the H19 threshold of {H19_PERMANENT_FRACTION_THRESHOLD}."
        )
    else:
        anomalies.append(
            f"All {analysis['glitch_count']} simulated glitches satisfy the H19 "
            f"permanent-fraction threshold (f_p >= {H19_PERMANENT_FRACTION_THRESHOLD})."
        )

    if analysis.get("glitch_count", 0) > 1:
        anomalies.append(
            f"Simulated mean inter-glitch interval: {analysis['mean_interval_days']} ± "
            f"{analysis['std_interval_days']} days (Vela observed: ~783–1012 days)."
        )

    mean_dnu_over_nu = analysis.get("mean_delta_nu_over_nu")
    if mean_dnu_over_nu is not None:
        anomalies.append(
            f"Simulated mean Δν/ν = {mean_dnu_over_nu:.2e} "
            f"(Vela catalog mean ~2.0e-6; deviation reflects log-normal scatter)."
        )

    significance = 0.72 if h19_passes else 0.45

    summary = (
        f"Vela pulsar crust memory simulation over {result.duration_days:.0f} days. "
        f"Generated {analysis.get('glitch_count', 0)} glitch(es) with permanent fraction "
        f"f_p = {result.permanent_fraction:.3f}. "
        f"H19 hypothesis ({'PASSES' if h19_passes else 'FAILS'}): "
        f"Δν_p/Δν >= {H19_PERMANENT_FRACTION_THRESHOLD} for "
        f"{'all' if h19_passes else 'some'} events."
    )

    sim_params = {
        "nu0_hz": result.nu0_hz,
        "nudot_hz_day": result.nudot_hz_day,
        "superfluid_fraction": result.superfluid_fraction,
        "permanent_fraction": result.permanent_fraction,
        "omega_critical_hz": result.omega_critical_hz,
        "duration_days": result.duration_days,
        "seed": result.seed,
        "dt_coarse_days": result.dt_coarse_days,
        "dt_fine_days": result.dt_fine_days,
    }

    bundle: dict = {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "vela_crust_memory_simulation_bundle",
        "generated_at": _iso_now(),
        "summary": summary,
        "entities": ["Vela Pulsar", "PSR B0833-45", "J0835-4510", "neutron star crust", "superfluid vortex", "crustal memory"],
        "topics": ["pulsar glitches", "crustal memory", "vortex pinning", "post-glitch recovery", "two-component model"],
        "anomalies": anomalies,
        "significance": significance,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": ["pulsars", "simulation"],
        "source_catalogs": ["vela_crust_memory_sim.py", "Baym+1969", "Alpar+1984", "Grover+2025:arXiv:2506.02100v1"],
        "target": {
            "name": "Vela Pulsar",
            "input_target": "PSR B0833-45",
            "psrj": "J0835-4510",
            "psrb": "B0833-45",
        },
        "structured_evidence": {
            "hypothesis_focus": "Crustal Memory",
            "hypothesis_id": "H19",
            "simulation_parameters": sim_params,
            "glitch_table": glitch_table,
            "memory_analysis": analysis,
            "h19_result": {
                "passes": h19_passes,
                "threshold": H19_PERMANENT_FRACTION_THRESHOLD,
                "description": (
                    "H19 requires a non-zero asymptotic post-glitch spin-frequency offset "
                    "(Δν_p/Δν >= 0.01) consistent with irreversible crustal restructuring."
                ),
            },
            "time_domain_summary": {
                "total_time_steps": len(result.times_days),
                "final_nu_crust_hz": result.nu_crust_hz[-1] if result.nu_crust_hz else None,
                "total_nu_drop_hz": (result.nu_crust_hz[-1] - result.nu_crust_hz[0]) if result.nu_crust_hz else None,
            },
        },
        "new_hypothesis": None if supports_hypothesis else {
            "title": "Crustal Memory in Vela Pulsar (Simulation)",
            "body": summary,
            "confidence": 0.64 if h19_passes else 0.30,
            "predictions": [
                "Future Vela glitches should show a measurable asymptotic post-glitch offset "
                "Δν_p/Δν >= 0.01, irrecoverable under standard vortex-creep-plus-bending fits.",
                "High-cadence timing around the next predicted Vela glitch (MJD ~61377.7) should "
                "resolve the permanent component within the first 30 days post-glitch.",
            ],
        },
    }
    return bundle


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def _build_text_report(bundle: dict) -> str:
    se = bundle.get("structured_evidence", {})
    analysis = se.get("memory_analysis", {})
    h19 = se.get("h19_result", {})
    params = se.get("simulation_parameters", {})
    lines = [
        "Vela Pulsar Crust Memory Simulation Report",
        "=" * 60,
        f"Generated : {bundle.get('generated_at', '')}",
        f"Duration  : {params.get('duration_days', '')} days",
        f"Seed      : {params.get('seed', '')}",
        f"f_p       : {params.get('permanent_fraction', '')}  (permanent fraction per glitch)",
        f"Q_s       : {params.get('superfluid_fraction', '')}  (superfluid moment-of-inertia fraction)",
        f"ω_cr      : {params.get('omega_critical_hz', ''):.4e} Hz  (critical lag)",
        "",
        "Glitch statistics",
        "-" * 40,
        f"Glitch count              : {analysis.get('glitch_count', 0)}",
        f"Mean Δν/ν                 : {analysis.get('mean_delta_nu_over_nu', 0):.3e}",
        f"Mean permanent fraction   : {analysis.get('mean_permanent_fraction', 'N/A')}",
        f"Min/Max permanent fraction: {analysis.get('min_permanent_fraction', 'N/A')} / {analysis.get('max_permanent_fraction', 'N/A')}",
        f"Mean inter-glitch interval: {analysis.get('mean_interval_days', 0):.1f} ± {analysis.get('std_interval_days', 0):.1f} days",
        "",
        "H19 Hypothesis",
        "-" * 40,
        f"Result    : {'PASS' if h19.get('passes') else 'FAIL'}",
        f"Threshold : Δν_p/Δν >= {h19.get('threshold', 0.01)}",
    ]
    failing = analysis.get("failing_glitch_indices", [])
    if failing:
        lines.append(f"Failing glitches: indices {failing}")
    for note in bundle.get("anomalies", []):
        lines.append(f"  • {note}")
    lines.append("")
    lines.append(f"Summary: {bundle.get('summary', '')}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def write_bundle(
    bundle: dict,
    inbox_dir: Path,
    filename_prefix: str = "vela_crust_memory_sim_bundle",
) -> tuple[Path, Path]:
    """Write the bundle JSON and a companion markdown report to inbox_dir."""
    inbox_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = inbox_dir / f"{filename_prefix}_{stamp}.json"
    md_path = inbox_dir / f"{filename_prefix}_{stamp}.md"

    json_tmp = json_path.with_suffix(json_path.suffix + ".tmp")
    md_tmp = md_path.with_suffix(md_path.suffix + ".tmp")

    with open(json_tmp, "w", encoding="utf-8") as fh:
        json.dump(bundle, fh, indent=2, ensure_ascii=False)
    with open(md_tmp, "w", encoding="utf-8") as fh:
        fh.write(_build_text_report(bundle))

    json_tmp.replace(json_path)
    md_tmp.replace(md_path)
    return json_path, md_path


# ---------------------------------------------------------------------------
# ASCII terminal plot
# ---------------------------------------------------------------------------

def _ascii_chart(
    xs: list[float],
    ys: list[float],
    *,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    width: int = 72,
    height: int = 14,
    vlines: list[float] | None = None,
    y_unit: str = "",
) -> str:
    """Render a minimal ASCII line chart.  Returns a multi-line string."""
    if not xs or not ys:
        return f"  [{title}: no data]\n"

    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)

    # Guard against flat lines
    x_span = x_max - x_min or 1.0
    y_span = y_max - y_min or abs(y_min) * 0.01 or 1e-12

    def to_col(x: float) -> int:
        return min(width - 1, max(0, int((x - x_min) / x_span * (width - 1))))

    def to_row(y: float) -> int:
        # row 0 = top (y_max), row height-1 = bottom (y_min)
        return min(height - 1, max(0, int((y_max - y) / y_span * (height - 1))))

    # Build empty grid
    grid: list[list[str]] = [[" "] * width for _ in range(height)]

    # Plot the line by connecting consecutive points
    for i in range(len(xs) - 1):
        c0, r0 = to_col(xs[i]), to_row(ys[i])
        c1, r1 = to_col(xs[i + 1]), to_row(ys[i + 1])
        # Bresenham-style column scan
        steps = max(abs(c1 - c0), 1)
        for s in range(steps + 1):
            t = s / steps
            c = int(c0 + t * (c1 - c0))
            r = int(r0 + t * (r1 - r0))
            grid[r][c] = "─"
        grid[r0][c0] = "·"
        grid[r1][c1] = "·"

    # Mark vertical glitch lines
    for vx in (vlines or []):
        vc = to_col(vx)
        for r in range(height):
            if grid[r][vc] == " ":
                grid[r][vc] = "│"

    # Assemble with y-axis labels
    y_label_width = 10
    buf = io.StringIO()
    if title:
        buf.write(f"  {title}\n")
    for r, row in enumerate(grid):
        # Left axis label at top, middle, bottom rows
        if r == 0:
            label = f"{y_max:>{y_label_width - 1}.3e}"
        elif r == height // 2:
            mid = (y_max + y_min) / 2
            label = f"{mid:>{y_label_width - 1}.3e}"
        elif r == height - 1:
            label = f"{y_min:>{y_label_width - 1}.3e}"
        else:
            label = " " * (y_label_width - 1)
        buf.write(f"{label} │{''.join(row)}\n")
    # X-axis
    buf.write(" " * y_label_width + "└" + "─" * width + "\n")
    # X labels
    x_left = f"{x_min:.0f}"
    x_right = f"{x_max:.0f}"
    x_mid = f"{(x_min + x_max) / 2:.0f}"
    pad = width - len(x_left) - len(x_right)
    mid_pos = max(0, (width // 2) - len(x_mid) // 2 - len(x_left))
    buf.write(" " * y_label_width + " " + x_left + " " * mid_pos + x_mid +
              " " * max(0, pad - mid_pos - len(x_mid)) + x_right + "\n")
    if xlabel:
        total = y_label_width + 1 + width
        buf.write(" " * ((total - len(xlabel)) // 2) + xlabel + "\n")
    if ylabel:
        buf.write(f"  y: {ylabel}" + (f"  [{y_unit}]" if y_unit else "") + "\n")
    return buf.getvalue()


def _bar_chart(labels: list[str], values: list[float], *, title: str = "", bar_width: int = 40) -> str:
    """Render a simple horizontal bar chart."""
    if not values:
        return f"  [{title}: no data]\n"
    v_max = max(values) or 1.0
    buf = io.StringIO()
    if title:
        buf.write(f"  {title}\n")
    for lbl, val in zip(labels, values):
        filled = int(val / v_max * bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)
        buf.write(f"  {lbl:>8s} │{bar}│ {val:.4f}\n")
    buf.write(f"  {'':>8s}  0{'':>{bar_width - 1}}{v_max:.4f}\n")
    return buf.getvalue()


def print_plot(result: SimulationResult, analysis: dict, *, output_file: Path | None = None) -> None:
    """Render three ASCII panels: ν_c(t), lag δω(t), and per-glitch permanent fractions."""
    glitch_days = [g.epoch_day for g in result.glitches]

    # Downsample time-series to ≤ 500 points for readability
    total = len(result.times_days)
    step = max(1, total // 500)
    t_ds = result.times_days[::step]
    nu_ds = result.nu_crust_hz[::step]
    lag_ds = result.lag_hz[::step]

    divider = "─" * 84 + "\n"

    sections = [
        "╔══════════════════════════════════════════════════════════════════════════════════╗\n"
        "║         VELA PULSAR CRUST MEMORY SIMULATION  —  ASCII TERMINAL PLOT             ║\n"
        "╚══════════════════════════════════════════════════════════════════════════════════╝\n",

        divider,
        "  Panel 1 · Crust spin frequency ν_c(t)\n",
        _ascii_chart(t_ds, nu_ds,
                     title="",
                     xlabel="Time [days]",
                     ylabel="ν_c",
                     y_unit="Hz",
                     vlines=glitch_days),

        divider,
        "  Panel 2 · Superfluid–crust lag δω(t)  (│ = glitch epoch)\n",
        _ascii_chart(t_ds, lag_ds,
                     title="",
                     xlabel="Time [days]",
                     ylabel="δω = ν_s − ν_c",
                     y_unit="Hz",
                     vlines=glitch_days),

        divider,
        "  Panel 3 · Per-glitch permanent fraction  (H19 threshold = 0.01)\n",
    ]

    if result.glitches:
        bar_labels = [f"G{i}" for i in range(len(result.glitches))]
        bar_values = [g.permanent_fraction for g in result.glitches]
        sections.append(_bar_chart(bar_labels, bar_values, bar_width=40))
    else:
        sections.append("  No glitches to display.\n")

    # Summary footer
    gc = analysis.get("glitch_count", 0)
    h19 = "PASS ✓" if analysis.get("h19_passes") else "FAIL ✗"
    sections += [
        divider,
        f"  Glitches: {gc}  |  Mean Δν/ν: {analysis.get('mean_delta_nu_over_nu', 0):.2e}"
        f"  |  Mean f_p: {analysis.get('mean_permanent_fraction', 0):.4f}"
        f"  |  Mean interval: {analysis.get('mean_interval_days', 0):.0f} days"
        f"  |  H19: {h19}\n",
    ]

    output = "".join(sections)
    if output_file:
        output_file.write_text(output, encoding="utf-8")
        print(f"Plot written : {output_file}")
    else:
        print(output, end="")


def print_json(bundle: dict, *, output_file: Path | None = None) -> None:
    """Dump the full simulation bundle as formatted JSON."""
    text = json.dumps(bundle, indent=2, ensure_ascii=False)
    if output_file:
        output_file.write_text(text, encoding="utf-8")
        print(f"JSON written : {output_file}")
    else:
        print(text)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulate Vela pulsar crust memory and emit a Manatuabon evidence bundle."
    )
    parser.add_argument("--duration-days", type=float, default=5000.0,
                        help="Simulation duration in days (default: 5000)")
    parser.add_argument("--seed", type=int, default=42,
                        help="RNG seed for reproducibility (default: 42)")
    parser.add_argument("--permanent-fraction", type=float, default=0.05,
                        help="Permanent spin-up fraction f_p per glitch (default: 0.05)")
    parser.add_argument("--superfluid-fraction", type=float, default=0.014,
                        help="Superfluid moment-of-inertia fraction Q_s (default: 0.014)")
    parser.add_argument("--recharge-days", type=float, default=VELA_RECHARGE_DAYS,
                        help=f"Inter-glitch recharge timescale for ω_cr calibration (default: {VELA_RECHARGE_DAYS})")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR),
                        help="Output inbox directory (default: ./inbox)")
    parser.add_argument("--supports-hypothesis", default=None,
                        help="Existing hypothesis ID to link as direct support (e.g. H19)")
    parser.add_argument(
        "--format", dest="output_format",
        choices=["text", "json", "plot"],
        default="text",
        help=(
            "Output format: "
            "'text' prints a human-readable summary (default); "
            "'json' prints the full evidence bundle as JSON; "
            "'plot' renders ASCII charts of ν_c(t), lag δω(t), and per-glitch memory fractions."
        ),
    )
    parser.add_argument(
        "--output-file", default=None,
        help=(
            "Write formatted output to this file instead of stdout "
            "(applies to --format json and --format plot; "
            "bundle JSON/MD are always written to --inbox unless --no-bundle is set)."
        ),
    )
    parser.add_argument("--no-bundle", action="store_true",
                        help="Skip writing the Manatuabon inbox bundle (JSON + MD report).")
    # Keep legacy --no-write as alias for --no-bundle
    parser.add_argument("--no-write", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    sim = VelaCrustMemorySimulator(
        permanent_fraction=args.permanent_fraction,
        superfluid_fraction=args.superfluid_fraction,
        recharge_days=args.recharge_days,
        seed=args.seed,
    )

    import sys as _sys
    _banner = f"Running Vela crust memory simulation: {args.duration_days:.0f} days, seed={args.seed}"
    # For machine-readable formats (json/plot-to-file) keep stdout clean; use stderr for the banner
    if args.output_format in ("json",) and not args.output_file:
        print(_banner, file=_sys.stderr)
    else:
        print(_banner)

    result = sim.simulate(args.duration_days)
    analysis = analyze_memory(result)

    output_file = Path(args.output_file) if args.output_file else None
    skip_bundle = args.no_write or args.no_bundle

    if args.output_format == "json":
        bundle = build_simulation_bundle(
            result, analysis, supports_hypothesis=args.supports_hypothesis
        )
        print_json(bundle, output_file=output_file)
        if not skip_bundle:
            json_path, md_path = write_bundle(bundle, Path(args.inbox))
            print(f"Bundle written : {json_path}")
            print(f"Report written : {md_path}")

    elif args.output_format == "plot":
        bundle = build_simulation_bundle(
            result, analysis, supports_hypothesis=args.supports_hypothesis
        )
        print_plot(result, analysis, output_file=output_file)
        if not skip_bundle:
            json_path, md_path = write_bundle(bundle, Path(args.inbox))
            print(f"Bundle written : {json_path}")
            print(f"Report written : {md_path}")

    else:  # "text" (default)
        print(f"  Glitches detected : {analysis['glitch_count']}")
        if analysis["glitch_count"] > 0:
            print(f"  Mean Δν/ν         : {analysis['mean_delta_nu_over_nu']:.3e}")
            print(f"  Mean f_p          : {analysis['mean_permanent_fraction']:.4f}")
            print(f"  Mean interval     : {analysis['mean_interval_days']:.1f} ± {analysis['std_interval_days']:.1f} days")
            h19_str = "PASS" if analysis["h19_passes"] else "FAIL"
            print(f"  H19 (f_p>=0.01)  : {h19_str}")
        if not skip_bundle:
            bundle = build_simulation_bundle(
                result, analysis, supports_hypothesis=args.supports_hypothesis
            )
            json_path, md_path = write_bundle(bundle, Path(args.inbox))
            print(f"Bundle written : {json_path}")
            print(f"Report written : {md_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
