"""Build structured Manatuabon evidence bundles from Vela recovery papers."""

from __future__ import annotations

import argparse
from pathlib import Path

from pulsar_glitch_importer import (
    DEFAULT_DB_PATH,
    DEFAULT_INBOX_DIR,
    ensure_canonical_hypothesis,
    resolve_canonical_rule,
    write_bundle,
)


VELA_RECOVERY_PAPER_2506_02100V1 = {
    "paper_id": "arXiv:2506.02100v1",
    "title": "Post-glitch Recovery and the Neutron Star Structure: The Vela Pulsar",
    "url": "https://arxiv.org/abs/2506.02100v1",
    "authors": [
        "Himanshu Grover",
        "Erbil Gugercinoglu",
        "Bhal Chandra Joshi",
        "M. A. Krishnakumar",
        "Shantanu Desai",
        "P. Arumugam",
        "Debades Bandyopadhyay",
    ],
    "observation_span": "September 2016 to January 2025",
    "cadence": {
        "ort_days": [1, 14],
        "ugmrt_days": [15, 30],
    },
    "glitches": [
        {
            "label": "G1",
            "epoch_mjd": 57734.4,
            "delta_nu_over_nu_e9": 1433.2,
            "delta_nudot_over_nudot_e3": 5.595,
            "exp_tau_days": [21.52, 298.93],
            "nonlinear_tau_days": 61.0,
            "offset_time_days": 774.54,
            "predicted_interglitch_days_68": [787, 1083],
            "observed_interglitch_days": 783,
            "residual_period_days": [314.1],
        },
        {
            "label": "G2",
            "epoch_mjd": 58517.0,
            "delta_nu_over_nu_e9": 2471.0,
            "delta_nudot_over_nudot_e3": 6.0,
            "exp_tau_days": [7.35, 59.50, 616.34],
            "nonlinear_tau_days": 22.34,
            "offset_time_days": 921.52,
            "predicted_interglitch_days_68": [752, 1033],
            "observed_interglitch_days": 902,
            "residual_period_days": [],
        },
        {
            "label": "G3",
            "epoch_mjd": 59417.6,
            "delta_nu_over_nu_e9": 1235.0,
            "delta_nudot_over_nudot_e3": 8.0,
            "exp_tau_days": [1.62, 13.83, 228.20],
            "nonlinear_tau_days": 212.74,
            "offset_time_days": 469.11,
            "predicted_interglitch_days_68": [855, 1123],
            "observed_interglitch_days": 1012,
            "residual_period_days": [344.0, 153.0],
        },
        {
            "label": "G4",
            "epoch_mjd": 60429.9,
            "delta_nu_over_nu_e9": 2396.0,
            "delta_nudot_over_nudot_e3": 23.0,
            "exp_tau_days": [2.38, 12.31, 169.00],
            "nonlinear_tau_days": 254.98,
            "predicted_next_glitch_mjd_68": [61249, 61506],
            "predicted_next_glitch_mjd_median": 61377.7,
            "residual_period_days": [],
        },
    ],
    "key_findings": [
        "Bayesian model comparison favored hybrid exponential plus linear recovery models for all four Vela glitches.",
        "Measured recovery timescales span about 1.62 to 616.34 days across the monitored glitches.",
        "Residual spin-down oscillations remain after subtracting the preferred vortex-creep fit, including significant periods near 314, 344, and 153 days.",
        "The paper reports the next Vela glitch is likely around MJD 61377.7 with a 68 percent credible interval from MJD 61249 to 61506.",
        "The authors interpret the data within vortex creep plus vortex bending, so this paper is comparative evidence for H19 rather than standalone proof of crustal memory.",
    ],
}


def build_recovery_paper_bundle(
    paper: dict,
    *,
    target: str = "PSR B0833-45",
    hypothesis_focus: str = "Crustal Memory",
    supports_hypothesis: str | None = None,
) -> dict:
    target_name = "Vela Pulsar"
    summary = (
        f"Structured evidence bundle for {target_name} from {paper['paper_id']} covering four post-glitch recoveries, "
        "day-scale recovery times, and residual oscillation periods after standard vortex-creep fits."
    )
    anomalies = [
        "Residual spin-down oscillations remain after subtracting the preferred vortex-creep recovery model.",
        "Standard vortex creep plus vortex bending explains much of the data, so crustal-memory claims must outperform that baseline rather than merely restate it.",
        "Recovery timescales range from 1.62 to 616.34 days, providing direct time-domain quantities for falsifiability.",
    ]
    predictions = [
        "A revised crustal-memory hypothesis should beat or augment vortex-creep-plus-bending fits on post-glitch residuals over 200 to 700 day windows.",
        "If crustal memory is real, a persistent asymptotic post-glitch offset should remain measurable in microhertz after standard recovery components are removed.",
        "Upcoming Vela monitoring around the paper's predicted next-glitch window near MJD 61377.7 should test whether the same residual structure recurs.",
    ]
    bundle = {
        "manatuabon_schema": "structured_ingest_v1",
        "payload_type": "pulsar_recovery_paper_bundle",
        "summary": summary,
        "entities": [target_name, "pulsar", "glitch recovery", "vortex creep", "vortex bending", "crustal memory"],
        "topics": ["pulsar glitches", "post-glitch recovery", "timing residuals", "vortex creep", "crustal memory"],
        "anomalies": anomalies,
        "significance": 0.84,
        "supports_hypothesis": supports_hypothesis,
        "challenges_hypothesis": None,
        "domain_tags": ["pulsars"],
        "source_catalogs": [paper["paper_id"], paper["url"]],
        "target": {
            "name": target_name,
            "input_target": target,
            "psrj": "J0835-4510",
            "psrb": "B0833-45",
        },
        "structured_evidence": {
            "hypothesis_focus": hypothesis_focus,
            "paper": {
                "paper_id": paper["paper_id"],
                "title": paper["title"],
                "url": paper["url"],
                "authors": paper["authors"],
                "observation_span": paper["observation_span"],
                "cadence": paper["cadence"],
            },
            "glitch_recoveries": paper["glitches"],
            "time_domain_measurements": {
                "recovery_tau_days": sorted({tau for glitch in paper["glitches"] for tau in glitch.get("exp_tau_days", [])}),
                "nonlinear_tau_days": [glitch["nonlinear_tau_days"] for glitch in paper["glitches"] if glitch.get("nonlinear_tau_days") is not None],
                "residual_period_days": [period for glitch in paper["glitches"] for period in glitch.get("residual_period_days", [])],
            },
            "key_findings": paper["key_findings"],
            "comparative_baseline": "vortex_creep_plus_bending",
            "recommended_predictions": predictions,
        },
        "new_hypothesis": None if supports_hypothesis else {
            "title": f"Crustal Memory in {target_name}",
            "body": " ".join([
                summary,
                "The literature adds explicit day-scale recovery and residual-period measurements that can be used to test whether a crustal-memory term improves on the standard vortex-creep-plus-bending baseline.",
            ]),
            "confidence": 0.69,
            "predictions": predictions,
        },
    }
    return bundle


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a structured Vela recovery-paper evidence bundle for Manatuabon.")
    parser.add_argument("--target", default="PSR B0833-45", help="Target pulsar label")
    parser.add_argument("--hypothesis-focus", default="Crustal Memory", help="Hypothesis focus label stored in the bundle")
    parser.add_argument("--supports-hypothesis", default=None, help="Existing hypothesis ID to link as direct support")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Output directory for the generated bundle")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite database used to resolve canonical hypothesis threads")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    supports_hypothesis = args.supports_hypothesis
    canonical_rule = resolve_canonical_rule(args.target, args.hypothesis_focus)
    if not supports_hypothesis and canonical_rule:
        supports_hypothesis = ensure_canonical_hypothesis(Path(args.db), canonical_rule)

    bundle = build_recovery_paper_bundle(
        VELA_RECOVERY_PAPER_2506_02100V1,
        target=args.target,
        hypothesis_focus=args.hypothesis_focus,
        supports_hypothesis=supports_hypothesis,
    )
    json_path, md_path = write_bundle(
        bundle,
        Path(args.inbox),
        args.target,
        filename_prefix="pulsar_recovery_paper_bundle",
    )
    print(f"Structured bundle written: {json_path}")
    print(f"Companion report written: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())