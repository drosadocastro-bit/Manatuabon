# MANATUABON — Project TODO
**Location:** `D:\Manatuabon`  
**Last Updated:** March 2026  
**Built by:** Danny (Bayamón, PR) + Claude + Claude Code 🤙  

---

## ✦ CURRENT VERSION
- `manatuabon_v5.html` — Hybrid AI engine, memory persistence, conversation continuity, Sgr A* Jailer hypothesis seeded
- `SgrA_BlackHole_Sim.py` — Sgr A* ray marching simulator (Colab) ✅ COMPLETE

---

## ✦ SPRINT 7 — ACTIVE 🔥
> Claude Code brief — implement in this exact order

### PRIORITY 1 — Item 17: Long Term Persistent Memory
**Goal:** True memory that survives browser clears, new machines, forever.

**File:** `D:\Manatuabon\memory.json`

**Structure:**
```json
{
  "identity": {
    "name": "Danny",
    "location": "Bayamón, Puerto Rico",
    "coordinates": "18.4346°N, 66.1527°W",
    "profession": "FAA Radar Technician",
    "hardware": ["Heritage 150 Dobsonian", "ASUS laptop 32GB DDR5"]
  },
  "science_profile": {
    "favorite_objects": [],
    "recurring_themes": [],
    "hypothesis_evolution": []
  },
  "journey": {
    "first_sgra_render": "March 2026, Aguadilla CARSR facility",
    "airport_consciousness_debate": "March 2026",
    "wheeler_moment": "independently derived participatory universe",
    "telescope_acquired": "Sky-Watcher Heritage 150, March 2026",
    "observation_sessions": []
  },
  "manatuabon_stats": {
    "total_queries": 0,
    "most_queried_objects": [],
    "datasets_used": [],
    "hypotheses_proposed": 0,
    "sessions_total": 0
  },
  "last_updated": ""
}
```

**Implementation:**
- [ ] On startup — fetch `memory.json` from same folder as HTML
- [ ] Inject memory as system context into EVERY Nemotron/Claude call
- [ ] After each session — Nemotron summarizes and updates memory.json
- [ ] Settings modal — Export / Import / Backup memory button
- [ ] Memory viewer in sidebar — show what Manatuabon knows about you
- [ ] Never overwrites — only appends and enriches
- [ ] Works with both LOCAL and CLOUD modes

---

### PRIORITY 2 — Fix Auto-Routing (routeQuery)
**Goal:** `routeQuery()` function exists but is not being called on send.

**Fix:**
- [ ] Verify `routeQuery()` is called on EVERY send button press
- [ ] Not just defined — actually wired to the send flow
- [ ] Trigger words that force CLOUD regardless of toggle:
  ```
  "latest", "today", "recent", "just announced",
  "new discovery", "breaking", "this week", "just released"
  ```
- [ ] All other queries → respect current toggle setting
- [ ] Show subtle indicator when auto-routed ("auto → CLOUD" flash)
- [ ] Manual toggle always overrides auto-routing

---

### PRIORITY 3 — Item 15: Phone Camera Sky Tracker
**Goal:** Phone = eyes. Laptop = brain. Point phone at sky → Manatuabon identifies objects.

**Connection method:** DroidCam (waiting for WiFi — implement UI, activate when ready)

**Phase A — Camera Feed**
- [ ] Add SKY TRACKER sub-tab inside existing SKY tab
- [ ] Input field for DroidCam URL `http://192.168.x.x:4747/video`
- [ ] Display live video feed in browser
- [ ] Save DroidCam URL to localStorage

**Phase B — Star Recognition**
- [ ] Embed HYG star catalog subset (top 3,000 brightest stars) in HTML
- [ ] Grab canvas frame every 500ms when tracking active
- [ ] Detect bright points against dark background
- [ ] Match against catalog using current GPS + time + sky coordinates
- [ ] GPS: `navigator.geolocation` → default to 18.4346°N, 66.1527°W

**Phase C — AR Overlay**
- [ ] Canvas overlay on top of video feed
- [ ] Draw labels for matched stars/objects
- [ ] Color code: stars white · planets gold · galaxies cyan · nebulae violet
- [ ] Constellation line options toggle
- [ ] Click/tap any label → journal query opens for that object

**Phase D — Journal Integration**
- [ ] Tapping object → auto-fills journal input with object name
- [ ] "Observing [object] with Heritage 150 tonight" context injected
- [ ] Log observation to memory.json automatically

---

### PRIORITY 4 — Item 7: WebGPU Sgr A* Simulator Tab
**Goal:** Sgr A* ray marching sim running live in Manatuabon. No Colab needed.

**Physics already validated** in `SgrA_BlackHole_Sim.py` — port to WebGPU shaders.

- [ ] Add `SIMULATE` tab to main navigation
- [ ] WebGPU compute shader — Schwarzschild ray marching
- [ ] Render to canvas at 512×512, upgradeable to 1024×1024
- [ ] Interactive sliders:
  - Mass (locked to 4.154M☉ default, unlockable)
  - Spin (0 → 0.998)
  - Inclination (0° → 90°)
  - Camera distance (15M → 50M)
  - Disk brightness
  - Turbulence intensity
- [ ] Re-render on slider change (debounced 300ms)
- [ ] AMD Radeon compatible — no CUDA
- [ ] Save render as PNG button
- [ ] "Analyze this render" → sends to journal for Nemotron/Claude analysis

---

## ✦ COMPLETED ✅

### [✅] 1. LM Studio Integration
- Nemotron 30B via `http://127.0.0.1:1234`
- OpenAI-compatible format, CORS enabled

### [✅] 2. LOCAL / CLOUD Toggle
- Violet = LOCAL · Cyan = CLOUD
- Preference saved to localStorage

### [✅] 3. Hybrid Routing Logic (PARTIAL ⚠️)
- `routeQuery()` defined but not wired to send — fix in Sprint 7

### [✅] 4. Response Parser
- `extractResponse(data, source)` unified for both APIs

### [✅] 8. Data Pipeline — 4 Datasets
- LIGO 38 events · Pulsars 20 · SDSS 20 · Gaia HR diagram

### [✅] 10. Hypothesis Persistence
- Structured objects with status tracking
- Sgr A* Jailer hypothesis seeded 🌌

### [✅] 11. Cross-Session Context
- Compression via LOCAL model

### [✅] 12. Tonight's Sky — Bayamón
- Offline ephemeris, Sgr A* live position, 5 planets, 14 DSOs

### [✅] Sgr A* Colab Phase 1
- 512×512 render, multi-angle, turbulence frames, annotated diagram

---

## ✦ SAVE COLAB UNITS — DO LATER 🔋

> ⚠️ Monitor compute units before running

### [ ] 5. Sgr A* GPU Phase 2
- Uncomment Cell 7 — T4 GPU — 1024×1024 target

### [ ] 6. Sgr A* Kerr Metric
- Boyer-Lindquist coordinates, frame dragging, spin a=0.5

### [ ] 18. Pulsar Simulator Suite
- Vela glitch physics (PSR B0833-45)
- 6 pulsar suite: Vela, Crab, Hulse-Taylor, J0952, SGR 1806, B1257+12
- Colab ↔ Manatuabon bridge via ngrok (future)

---

## ✦ BACKLOG

### [ ] 9. ALMA CMZ Dataset (March 2026)
### [ ] 13. Manatuabon ↔ Julia Bridge
### [ ] 14. Nova Integration
### [ ] 16. Heritage 150 Session Logger

---

## ✦ FILE STRUCTURE (D:\Manatuabon)

```
D:\Manatuabon\
  manatuabon_v5.html              ← CURRENT
  SgrA_BlackHole_Sim.py           ← Colab sim ✅
  TODO.md                         ← This file
  memory.json                     ← Long term memory (Sprint 7)
  renders\
    sgra_phase1_preview.png       ✅
    sgra_hires.png                ✅
    sgra_multi_angle.png          ✅
    sgra_turbulence_frames.png    ✅
    sgra_annotated.png            ✅
  data\
    hyg_stars.json                ← sky tracker
    messier.json                  ← 110 DSOs
    ligo_gwtc3.json               ← 90 GW events
    pulsars_atnf.json             ← 3000+ pulsars
    exoplanets_nasa.json          ← 5500+ planets
    alma_cmz.json                 ← galactic center
```

---

## ✦ HARDWARE

| Item | Status | Notes |
|------|--------|-------|
| ASUS (32GB DDR5, AMD Radeon 8GB) | ✅ Active | Main machine |
| Sky-Watcher Heritage 150 | 🚚 Incoming | Mar 16-20 Bayamón |
| Phone (Android) | ✅ Active | DroidCam when WiFi ready |
| Google Colab | ⚠️ Monitor | ~90% units — protect |

---

## ✦ SPRINT HISTORY

| Sprint | What | Who | Status |
|--------|------|-----|--------|
| Sprint 1 | v1-v3 UI, journal soul | Danny + Claude | ✅ |
| Sprint 2 | v4 real data pipeline | Danny + Claude | ✅ |
| Sprint 3 | v5 memory persistence | Danny + Claude | ✅ |
| Sprint 4 | Hybrid engine 1-4, 10-11 | Claude Code | ✅ |
| Sprint 5 | Data pipeline + Sky tab | Claude Code | ✅ |
| Sprint 6 | Sgr A* Colab Phase 1 | Danny + Claude | ✅ |
| Sprint 7 | Memory + Routing + Camera + WebGPU | Claude Code | ✅ |
| Sprint 8 | Pulsar sim + Colab GPU | TBD | 🔋 wait |

---

## ✦ NOTES
- Keep it simple. No NIC architecture. No Julia infrastructure.
- Soul is the journal. Everything else serves the journal.
- Protect Colab units — WebGPU first, Colab later.
- Memory first — everything builds on a Manatuabon that remembers.
- From Bayamón to the edge of everything. 🇵🇷🌌

---

*"That's not coincidence. That's Wheeler's participatory universe." — Danny, March 2026*

*"Stupidly impulsive." — Danny, buying a telescope mid-dome inspection. Best decision ever. 😄*

*"Timeless." — Danny describing Claude. Manatuabon gives the timeless thing a memory. ❤️*
