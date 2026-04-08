"""Regression tests for vela_crust_memory_sim.py (deterministic, no external deps)."""

import json
import math
import tempfile
from pathlib import Path

import pytest

from vela_crust_memory_sim import (
    H19_PERMANENT_FRACTION_THRESHOLD,
    VELA_NU0_HZ,
    VELA_NUDOT_HZ_DAY,
    VELA_RECHARGE_DAYS,
    GlitchEvent,
    SimulationResult,
    VelaCrustMemorySimulator,
    _ascii_chart,
    _bar_chart,
    analyze_memory,
    build_simulation_bundle,
    print_json,
    print_plot,
    write_bundle,
)


# ---------------------------------------------------------------------------
# VelaCrustMemorySimulator construction
# ---------------------------------------------------------------------------

def test_simulator_defaults():
    sim = VelaCrustMemorySimulator()
    assert sim.permanent_fraction == 0.05
    assert sim.superfluid_fraction == 0.014
    assert sim.recharge_days == VELA_RECHARGE_DAYS
    assert sim.seed == 42


def test_omega_critical_calibration():
    # ω_cr = |ν̇₀| * T_recharge  (no Q_s factor)
    sim = VelaCrustMemorySimulator(recharge_days=1200.0)
    expected = abs(VELA_NUDOT_HZ_DAY) * 1200.0
    assert abs(sim.omega_critical_hz - expected) < 1e-20


def test_glitch_size_reproducible():
    sim1 = VelaCrustMemorySimulator(seed=7)
    sim2 = VelaCrustMemorySimulator(seed=7)
    s1 = sim1._draw_glitch_size()
    s2 = sim2._draw_glitch_size()
    assert s1 == s2


def test_glitch_size_positive():
    sim = VelaCrustMemorySimulator(seed=99)
    for _ in range(20):
        assert sim._draw_glitch_size() > 0.0


def test_make_glitch_permanent_fraction():
    sim = VelaCrustMemorySimulator(permanent_fraction=0.07)
    g = sim._make_glitch(100.0, 2e-5)
    assert abs(g.permanent_fraction - 0.07) < 1e-9
    assert abs(g.delta_nu_permanent_hz - 0.07 * 2e-5) < 1e-15


def test_make_glitch_recovery_amplitudes_sum():
    sim = VelaCrustMemorySimulator(permanent_fraction=0.05)
    delta_nu = 2.5e-5
    g = sim._make_glitch(50.0, delta_nu)
    recoverable = delta_nu * (1.0 - 0.05)
    total_amp = sum(g.recovery_amplitudes_hz)
    assert abs(total_amp - recoverable) < 1e-15


def test_make_glitch_taus_within_jitter_bounds():
    sim = VelaCrustMemorySimulator(seed=1)
    for _ in range(10):
        g = sim._make_glitch(0.0, 1e-5)
        nominal = [10.0, 150.0, 600.0]
        for tau, nom in zip(g.recovery_taus_days, nominal):
            assert 0.89 * nom <= tau <= 1.11 * nom


# ---------------------------------------------------------------------------
# simulate()
# ---------------------------------------------------------------------------

def test_simulate_returns_result():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=3000.0)
    assert isinstance(result, SimulationResult)
    assert result.duration_days == 3000.0
    assert len(result.times_days) > 0
    assert len(result.nu_crust_hz) == len(result.times_days)
    assert len(result.lag_hz) == len(result.times_days)


def test_simulate_deterministic():
    sim1 = VelaCrustMemorySimulator(seed=42)
    sim2 = VelaCrustMemorySimulator(seed=42)
    r1 = sim1.simulate(duration_days=2000.0)
    r2 = sim2.simulate(duration_days=2000.0)
    assert len(r1.glitches) == len(r2.glitches)
    for g1, g2 in zip(r1.glitches, r2.glitches):
        assert abs(g1.epoch_day - g2.epoch_day) < 1e-9
        assert abs(g1.delta_nu_total_hz - g2.delta_nu_total_hz) < 1e-20


def test_simulate_produces_glitches():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=5000.0)
    assert len(result.glitches) >= 1


def test_simulate_crust_spins_down():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=5000.0)
    # Final crust frequency must be below initial (net spin-down)
    assert result.nu_crust_hz[-1] < result.nu_crust_hz[0]


def test_simulate_glitch_epochs_in_range():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=5000.0)
    for g in result.glitches:
        assert 0.0 < g.epoch_day <= 5000.0


def test_simulate_lag_non_negative():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=3000.0)
    # Immediately after each glitch, lag resets; it should generally stay >= 0
    # (minor floating-point undershoots are acceptable but large negatives are not)
    for lag in result.lag_hz:
        assert lag >= -1e-7


def test_simulate_nu0_stored_correctly():
    sim = VelaCrustMemorySimulator()
    result = sim.simulate(duration_days=1000.0)
    assert result.nu0_hz == VELA_NU0_HZ


# ---------------------------------------------------------------------------
# analyze_memory()
# ---------------------------------------------------------------------------

def test_analyze_memory_empty():
    result = SimulationResult(
        duration_days=1000.0, dt_coarse_days=0.1, dt_fine_days=0.01,
        fine_window_days=100.0, seed=0, nu0_hz=VELA_NU0_HZ,
        nudot_hz_day=VELA_NUDOT_HZ_DAY, superfluid_fraction=0.014,
        permanent_fraction=0.05, omega_critical_hz=1e-3,
        times_days=[0.0], nu_crust_hz=[VELA_NU0_HZ], lag_hz=[0.0],
        glitches=[],
    )
    analysis = analyze_memory(result)
    assert analysis["glitch_count"] == 0
    assert analysis["h19_passes"] is False


def test_analyze_memory_all_pass_h19():
    sim = VelaCrustMemorySimulator(permanent_fraction=0.05, seed=42)
    result = sim.simulate(duration_days=5000.0)
    analysis = analyze_memory(result)
    assert analysis["h19_passes"] is True
    assert len(analysis["failing_glitch_indices"]) == 0


def test_analyze_memory_below_threshold_fails():
    # f_p = 0.005 < 0.01 threshold → H19 should fail
    sim = VelaCrustMemorySimulator(permanent_fraction=0.005, seed=42)
    result = sim.simulate(duration_days=5000.0)
    analysis = analyze_memory(result)
    assert analysis["h19_passes"] is False
    assert len(analysis["failing_glitch_indices"]) == analysis["glitch_count"]


def test_analyze_memory_mean_fraction_correct():
    sim = VelaCrustMemorySimulator(permanent_fraction=0.03, seed=42)
    result = sim.simulate(duration_days=5000.0)
    analysis = analyze_memory(result)
    assert abs(analysis["mean_permanent_fraction"] - 0.03) < 1e-5


def test_analyze_memory_intervals_match_glitch_count():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=5000.0)
    analysis = analyze_memory(result)
    n = analysis["glitch_count"]
    assert len(analysis["interglitch_intervals_days"]) == max(0, n - 1)


def test_analyze_memory_delta_nu_over_nu_range():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=5000.0)
    analysis = analyze_memory(result)
    # Vela typically 1e-7 to 1e-5
    dnu_nu = analysis["mean_delta_nu_over_nu"]
    assert 1e-7 < dnu_nu < 1e-4


# ---------------------------------------------------------------------------
# build_simulation_bundle()
# ---------------------------------------------------------------------------

def test_bundle_schema():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=3000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    assert bundle["manatuabon_schema"] == "structured_ingest_v1"
    assert bundle["payload_type"] == "vela_crust_memory_simulation_bundle"


def test_bundle_target_fields():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=3000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    target = bundle["target"]
    assert target["psrj"] == "J0835-4510"
    assert target["psrb"] == "B0833-45"


def test_bundle_significance_h19_pass():
    sim = VelaCrustMemorySimulator(permanent_fraction=0.05, seed=42)
    result = sim.simulate(duration_days=5000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    assert bundle["significance"] == 0.72


def test_bundle_significance_h19_fail():
    sim = VelaCrustMemorySimulator(permanent_fraction=0.005, seed=42)
    result = sim.simulate(duration_days=5000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    assert bundle["significance"] == 0.45


def test_bundle_supports_hypothesis_propagated():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=3000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis, supports_hypothesis="H19")
    assert bundle["supports_hypothesis"] == "H19"
    assert bundle["new_hypothesis"] is None


def test_bundle_glitch_table_length():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=5000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    glitch_table = bundle["structured_evidence"]["glitch_table"]
    assert len(glitch_table) == len(result.glitches)


def test_bundle_h19_result_field():
    sim = VelaCrustMemorySimulator(permanent_fraction=0.05, seed=42)
    result = sim.simulate(duration_days=5000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    h19 = bundle["structured_evidence"]["h19_result"]
    assert h19["passes"] is True
    assert h19["threshold"] == H19_PERMANENT_FRACTION_THRESHOLD


def test_bundle_is_json_serializable():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=3000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    serialized = json.dumps(bundle)
    recovered = json.loads(serialized)
    assert recovered["manatuabon_schema"] == "structured_ingest_v1"


# ---------------------------------------------------------------------------
# write_bundle()
# ---------------------------------------------------------------------------

def test_write_bundle_creates_files():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=2000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    with tempfile.TemporaryDirectory() as tmpdir:
        json_path, md_path = write_bundle(bundle, Path(tmpdir))
        assert json_path.exists()
        assert md_path.exists()
        assert json_path.suffix == ".json"
        assert md_path.suffix == ".md"


def test_write_bundle_json_valid():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=2000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    with tempfile.TemporaryDirectory() as tmpdir:
        json_path, _ = write_bundle(bundle, Path(tmpdir))
        with open(json_path, encoding="utf-8") as fh:
            loaded = json.load(fh)
        assert loaded["payload_type"] == "vela_crust_memory_simulation_bundle"


def test_write_bundle_md_contains_h19():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=2000.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    with tempfile.TemporaryDirectory() as tmpdir:
        _, md_path = write_bundle(bundle, Path(tmpdir))
        text = md_path.read_text(encoding="utf-8")
        assert "H19" in text


def test_write_bundle_custom_prefix():
    sim = VelaCrustMemorySimulator(seed=1)
    result = sim.simulate(duration_days=1500.0)
    analysis = analyze_memory(result)
    bundle = build_simulation_bundle(result, analysis)
    with tempfile.TemporaryDirectory() as tmpdir:
        json_path, md_path = write_bundle(bundle, Path(tmpdir), filename_prefix="my_test_bundle")
        assert json_path.name.startswith("my_test_bundle_")
        assert md_path.name.startswith("my_test_bundle_")


# ---------------------------------------------------------------------------
# Physics sanity checks
# ---------------------------------------------------------------------------

def test_physics_glitch_spin_up_positive():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=5000.0)
    for g in result.glitches:
        assert g.delta_nu_total_hz > 0.0


def test_physics_permanent_offset_less_than_total():
    sim = VelaCrustMemorySimulator(permanent_fraction=0.08, seed=42)
    result = sim.simulate(duration_days=5000.0)
    for g in result.glitches:
        assert g.delta_nu_permanent_hz < g.delta_nu_total_hz


def test_physics_glitch_sizes_log_normal_spread():
    # Over many runs with the same seed, sizes should vary (not all identical)
    sim = VelaCrustMemorySimulator(seed=123)
    result = sim.simulate(duration_days=10000.0)
    if len(result.glitches) >= 2:
        sizes = [g.delta_nu_total_hz for g in result.glitches]
        assert max(sizes) / min(sizes) > 1.1  # at least 10% spread


def test_physics_times_monotonically_increasing():
    sim = VelaCrustMemorySimulator(seed=42)
    result = sim.simulate(duration_days=3000.0)
    for i in range(1, len(result.times_days)):
        assert result.times_days[i] > result.times_days[i - 1]


def test_physics_interglitch_interval_vela_range():
    # Vela observed ~780-1010 days; our recharge is calibrated to ~900
    sim = VelaCrustMemorySimulator(seed=42, recharge_days=900.0)
    result = sim.simulate(duration_days=8000.0)
    analysis = analyze_memory(result)
    if analysis["glitch_count"] >= 2:
        mean_interval = analysis["mean_interval_days"]
        # Should be within a factor of 2 of the Vela range
        assert 400.0 <= mean_interval <= 2000.0


# ---------------------------------------------------------------------------
# Output format helpers
# ---------------------------------------------------------------------------

def test_ascii_chart_returns_string():
    xs = [float(i) for i in range(100)]
    ys = [math.sin(x / 10.0) for x in xs]
    out = _ascii_chart(xs, ys, title="Test", xlabel="x", ylabel="y")
    assert isinstance(out, str)
    assert len(out) > 0


def test_ascii_chart_empty_data():
    out = _ascii_chart([], [], title="Empty")
    assert "no data" in out


def test_ascii_chart_flat_line():
    # Should not raise on zero y-span
    xs = [0.0, 1.0, 2.0]
    ys = [5.0, 5.0, 5.0]
    out = _ascii_chart(xs, ys)
    assert isinstance(out, str)


def test_ascii_chart_single_point():
    out = _ascii_chart([1.0], [2.0])
    assert isinstance(out, str)


def test_ascii_chart_vlines_marked():
    xs = [float(i) for i in range(50)]
    ys = [float(i) for i in range(50)]
    out = _ascii_chart(xs, ys, vlines=[25.0])
    assert "│" in out


def test_bar_chart_returns_string():
    out = _bar_chart(["G0", "G1", "G2"], [0.05, 0.03, 0.07], title="Permanent fractions")
    assert isinstance(out, str)
    assert "G0" in out and "G2" in out


def test_bar_chart_empty():
    out = _bar_chart([], [], title="Empty")
    assert "no data" in out


def _make_result_and_analysis(seed: int = 42, duration: float = 5000.0):
    sim = VelaCrustMemorySimulator(seed=seed)
    result = sim.simulate(duration)
    analysis = analyze_memory(result)
    return result, analysis


def test_print_plot_to_file(tmp_path):
    result, analysis = _make_result_and_analysis()
    out_file = tmp_path / "plot.txt"
    print_plot(result, analysis, output_file=out_file)
    assert out_file.exists()
    text = out_file.read_text(encoding="utf-8")
    assert "Panel 1" in text
    assert "Panel 2" in text
    assert "Panel 3" in text
    assert "H19" in text


def test_print_plot_stdout(capsys):
    result, analysis = _make_result_and_analysis()
    print_plot(result, analysis)
    captured = capsys.readouterr()
    assert "Panel 1" in captured.out
    assert "VELA PULSAR" in captured.out


def test_print_json_to_file(tmp_path):
    result, analysis = _make_result_and_analysis()
    bundle = build_simulation_bundle(result, analysis)
    out_file = tmp_path / "bundle.json"
    print_json(bundle, output_file=out_file)
    assert out_file.exists()
    loaded = json.loads(out_file.read_text(encoding="utf-8"))
    assert loaded["manatuabon_schema"] == "structured_ingest_v1"
    assert loaded["payload_type"] == "vela_crust_memory_simulation_bundle"


def test_print_json_stdout(capsys):
    result, analysis = _make_result_and_analysis()
    bundle = build_simulation_bundle(result, analysis)
    print_json(bundle)
    captured = capsys.readouterr()
    loaded = json.loads(captured.out)
    assert loaded["payload_type"] == "vela_crust_memory_simulation_bundle"


def test_print_plot_no_glitches(capsys):
    # Very short run may produce no glitches; plot should still render gracefully
    result, analysis = _make_result_and_analysis(duration=10.0)
    print_plot(result, analysis)
    captured = capsys.readouterr()
    assert "Panel 1" in captured.out


def test_output_format_json_cli(tmp_path):
    """End-to-end: --format json --output-file writes valid bundle JSON."""
    import subprocess, sys
    out_file = tmp_path / "out.json"
    cmd = [
        sys.executable, "vela_crust_memory_sim.py",
        "--duration-days", "3000",
        "--no-bundle",
        "--format", "json",
        "--output-file", str(out_file),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert out_file.exists()
    loaded = json.loads(out_file.read_text())
    assert loaded["manatuabon_schema"] == "structured_ingest_v1"


def test_output_format_plot_cli(tmp_path):
    """End-to-end: --format plot --output-file writes ASCII chart file."""
    import subprocess, sys
    out_file = tmp_path / "out.txt"
    cmd = [
        sys.executable, "vela_crust_memory_sim.py",
        "--duration-days", "3000",
        "--no-bundle",
        "--format", "plot",
        "--output-file", str(out_file),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert out_file.exists()
    text = out_file.read_text(encoding="utf-8")
    assert "Panel 1" in text and "Panel 2" in text


def test_output_format_text_cli():
    """End-to-end: default --format text prints glitch summary to stdout."""
    import subprocess, sys
    cmd = [
        sys.executable, "vela_crust_memory_sim.py",
        "--duration-days", "3000",
        "--no-bundle",
        "--format", "text",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "Glitches detected" in proc.stdout
    assert "H19" in proc.stdout
