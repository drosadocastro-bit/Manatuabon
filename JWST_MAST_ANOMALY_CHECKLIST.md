# JWST MAST Anomaly Retrieval Checklist

Purpose: collect the minimum evidence package needed to review a held JWST infrared-anomaly hypothesis in Manatuabon without mixing in unrelated datasets.

## Scope

Use this checklist for anomaly claims tied to JWST NIRCam or NIRSpec observations.

Do not treat LIGO, radio, or other repo-wide data as supporting evidence unless the hypothesis explicitly claims a cross-observatory counterpart and the timestamps, sky position, and physical mechanism are all linked.

## Rule Zero

For any claimed anomaly, fetch the observation-local evidence in this order:

1. Association files and observation metadata.
2. Stage 0 raw inputs.
3. Stage 1 detector-level outputs.
4. Stage 2 calibrated single-exposure outputs.
5. Stage 3 combined or extracted science products.
6. Matched comparison observations with the same instrument configuration.
7. Calibration context and known-issue references.

If the feature disappears before Stage 2, treat it as a detector or pipeline artifact candidate until proven otherwise.

## Capture This Metadata For Every File

- Program, observation, visit, exposure, detector, and target identifiers.
- Instrument configuration: detector, filter, grating, slit, subarray, readout pattern.
- Time metadata: `MJD-BEG`, `MJD-MID`, `MJD-END`.
- Exposure structure: `NINTS`, `NGROUPS`, `GROUPGAP`.
- Pipeline provenance: `CAL_VER`, `CRDS_CTX`.
- Reference-file headers used by calibration steps, such as `R_*` keywords.
- Step-completion flags that matter to the suspected artifact path.
- If segmented: `EXSEGNUM`, `EXSEGTOT`, `INTSTART`, `INTEND`.

## Common Fetch Order

### 1. Fetch association files first

- Download the relevant Stage 2 or Stage 3 association JSON when MAST provides it.
- For NIRSpec, keep associated background and MSA imprint exposures if the association references them.
- For Stage 3, note whether the product is an observation association (`oNNN`) or candidate association (`c1NNN`).

### 2. Fetch Stage 0 raw inputs

- `*uncal.fits`
- If the observation is segmented, fetch every `segNNN` file, not just the first segment.

Why: this is the only place to determine whether the claimed structure already exists in the raw ramps or appears later in processing.

### 3. Fetch Stage 1 detector products

- `*rate.fits`
- `*rateints.fits` for any multi-integration case and especially all TSO cases

Inspect:

- `SCI`
- `ERR`, `VAR_POISSON`, `VAR_RNOISE`
- `PIXELDQ`, `GROUPDQ` in Stage 1 products
- `INT_TIMES` when present

Why: detector artifacts, cosmic-ray behavior, hot pixels, saturation, and segment-boundary issues usually show up here.

### 4. Fetch Stage 2 calibrated products

- `*cal.fits` for standard imaging or spectroscopy outputs
- `*calints.fits` for time-series or per-integration spectroscopic review
- `*bsub.fits` if background-subtracted products exist and the anomaly might be background-sensitive

Inspect:

- `SCI`
- `ERR`
- `DQ`
- WCS assignment and photometric calibration headers

Why: this stage shows whether the feature survives standard calibration and coordinate assignment.

### 5. Fetch Stage 3 science products

Pick products by mode rather than downloading everything blindly.

#### NIRCam imaging

- `*i2d.fits`

Inspect:

- `SCI`
- `ERR` or propagated uncertainty products
- `WHT`
- `CON`

Why: confirms whether the feature persists after resampling and combination, and whether it is supported by multiple inputs.

#### NIRCam grism time-series spectroscopy

- `*rateints.fits`
- `*calints.fits`
- `*x1dints.fits` if present for the extraction path in use
- `*crfints.fits`
- `*wtlt.ecsv`

Why: the anomaly may be temporal, extraction-specific, or concentrated in a subset of integrations.

#### NIRCam time-series imaging

- `*rateints.fits`
- `*crfints.fits`
- `*phot.ecsv`

Why: time-variable anomalies must be checked at the integration level and then in the extracted photometric light curve.

#### NIRSpec fixed-slit or MSA spectroscopy

- `*cal.fits` or `*calints.fits`
- `*s2d.fits` when a resampled 2-D spectral product exists
- `*x1d.fits`
- `*x1dints.fits` for per-integration or TSO review

If MSA data are involved, keep background and imprint-related exposures from the association.

Why: a real spectral anomaly should be traceable from calibrated 2-D data into the extracted 1-D spectrum, not only in one representation.

#### NIRSpec IFU spectroscopy

- `*cal.fits`
- `*s3d.fits`
- `*x1d.fits` when extraction products are available and relevant

Why: IFU anomalies must be checked both in the cube and in any extracted spectrum.

#### NIRSpec bright object time series

- `*rateints.fits`
- `*calints.fits`
- `*x1dints.fits`
- `*crfints.fits`
- `*wtlt.ecsv`

Why: the white-light curve and per-integration spectra are the minimum package for judging whether the anomaly is temporal, spectrally localized, or a processing artifact.

### 6. Fetch matched comparison observations

For every candidate anomaly, fetch at least one comparison dataset with:

- the same instrument mode
- the same detector and similar subarray
- the same filter or grating setup
- similar readout pattern and integration structure
- similar brightness regime if the source is instrumental or saturation-sensitive

Priority order:

1. Same proposal or same visit configuration.
2. Same mode from a nearby date.
3. Same mode from a different target with similar acquisition settings.

Why: without a matched control, most anomaly narratives stay Tier C.

### 7. Fetch calibration context

- Record `CAL_VER` from every reviewed file.
- Record `CRDS_CTX` from every reviewed file.
- Preserve the specific `R_*` reference-file keywords used for key corrections.
- Check JWST calibration-status and known-issues pages for the instrument and mode.

Why: if the feature matches a known issue or disappears under a newer CRDS or pipeline build, it should not be treated as unexplained evidence.

## Fast Triage Logic

Use this progression when reviewing the files:

1. Does the feature already exist in `uncal` ramps?
2. Does it persist in `rate` or `rateints` after detector corrections?
3. Is it flagged by `PIXELDQ`, `GROUPDQ`, or later `DQ`?
4. Does the `ERR` budget make the apparent feature statistically weak?
5. Does it survive into `cal` or `calints`?
6. Does it remain after Stage 3 combination or extraction?
7. Does it recur across integrations, segments, or matched comparison observations?
8. Is there a known calibration issue that explains it better?

## What Counts As Stronger Evidence In Manatuabon

Evidence can move from speculative to reviewable when most of the following are true:

- The feature is visible in raw or near-raw products, not only in a final mosaic.
- It is not dominated by bad-pixel, saturation, or cosmic-ray flags.
- It survives from Stage 1 into Stage 2.
- For spectroscopy, it appears in both the calibrated 2-D product and the extracted spectrum.
- For time-series data, it repeats coherently across integrations instead of appearing in one segment only.
- It is not reproduced in matched control observations with the same configuration.
- The pipeline version and CRDS context are recorded and do not point to a known issue.

## Minimum Evidence Bundle To Attach To A Held Hypothesis

When you ingest the evidence back into Manatuabon, attach at least:

1. One `uncal` file or segment reference.
2. One `rate` or `rateints` file reference.
3. One `cal` or `calints` file reference.
4. One Stage 3 product appropriate to the mode: `i2d`, `x1d`, `s2d`, `s3d`, `phot.ecsv`, or `wtlt.ecsv`.
5. The file-level provenance summary with `CAL_VER`, `CRDS_CTX`, and relevant `R_*` keywords.
6. One matched comparison observation or an explicit note that no valid control has been retrieved yet.

Without that bundle, the council should usually remain at `held` or `needs_revision`.