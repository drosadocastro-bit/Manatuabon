import sqlite3
import json
import os
from pathlib import Path

DB_PATH = Path("manatuabon.db")
JSON_PATH = Path("memory.json")

SEEDED_FOUNDING_HYPOTHESES = [
    {
        "id": "H14",
        "title": "The Pulsar Timing Web",
        "desc": "Millisecond pulsars form a galaxy-scale timing lattice that can reveal low-frequency gravitational-wave structure and long-timescale spacetime noise. Their correlated timing residuals are not isolated curiosities but a distributed sensor network for the universe.",
        "evidence": "Pulsar timing arrays such as NANOGrav report a common-spectrum process consistent with a nanohertz gravitational-wave background. Pulsar timing remains one of the most stable astrophysical clocks known.",
        "status": "active",
        "tags": ["pulsars", "timing arrays", "gravitational waves", "NANOGrav", "spacetime"],
        "source": "Danny + Copilot, April 2026",
        "date": "2026-04-03",
    },
    {
        "id": "H15",
        "title": "Atmospheric Disequilibrium Worlds",
        "desc": "The strongest exoplanet biosignature candidates will likely be planets whose atmospheres remain in persistent chemical disequilibrium under stellar forcing. The anomaly is not one molecule but a stable imbalance that resists abiotic expectations.",
        "evidence": "JWST transmission spectra already show that atmospheric retrievals hinge on multi-gas context rather than single-line detections. Biosignature literature consistently treats disequilibrium chemistry as stronger evidence than any individual compound alone.",
        "status": "active",
        "tags": ["exoplanets", "biosignatures", "JWST", "atmospheres", "disequilibrium"],
        "source": "Danny + Copilot, April 2026",
        "date": "2026-04-03",
    },
    {
        "id": "H16",
        "title": "Quasar Flicker Cartography",
        "desc": "Quasar variability is not just observational noise; it is a map of accretion geometry, disk instabilities, and line-region structure across cosmic time. The way quasars flicker can be used to classify feeding states and hidden structure without resolving them directly.",
        "evidence": "Reverberation mapping links quasar light-curve delays to broad-line-region size, and multi-epoch surveys show structured variability across populations. Time-domain quasar studies increasingly recover physical state information from variability statistics.",
        "status": "active",
        "tags": ["quasars", "time domain astronomy", "accretion disks", "reverberation mapping", "AGN"],
        "source": "Danny + Copilot, April 2026",
        "date": "2026-04-03",
    },
    {
        "id": "H17",
        "title": "The Late-Time Expansion Tension",
        "desc": "The Hubble tension may be a sign that late-time expansion is not fully captured by a simple cosmological constant. If the discrepancy survives systematics, then dark energy may encode structure, evolution, or coupling that only becomes visible through precision cosmology.",
        "evidence": "Independent local and early-universe measurements of the expansion rate remain in significant tension. Ongoing work in supernova calibration, BAO, and CMB inference has not yet produced a universally accepted systematic resolution.",
        "status": "active",
        "tags": ["dark energy", "Hubble tension", "cosmology", "supernovae", "BAO"],
        "source": "Danny + Copilot, April 2026",
        "date": "2026-04-03",
    },
    {
        "id": "H18",
        "title": "The Cosmic Web as an Engine",
        "desc": "The cosmic web is not just the static scaffolding of galaxies; it actively regulates gas delivery, shock heating, magnetic structure, and the tempo of galaxy growth. Filaments and nodes should be treated as dynamic engines of structure formation rather than background geometry.",
        "evidence": "Simulations and observations both show filamentary gas inflow, environment-dependent quenching, and large-scale anisotropy in halo growth. Weak-lensing and spectroscopic surveys increasingly connect galaxy evolution to web environment.",
        "status": "active",
        "tags": ["cosmic web", "large scale structure", "galaxy evolution", "filaments", "weak lensing"],
        "source": "Danny + Copilot, April 2026",
        "date": "2026-04-03",
    },
]


def seed_foundational_hypotheses(conn):
    c = conn.cursor()
    for h in SEEDED_FOUNDING_HYPOTHESES:
        c.execute('''
            INSERT OR IGNORE INTO hypotheses (id, title, description, evidence, status, tags, source, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            h["id"], h["title"], h["desc"], h["evidence"],
            h["status"], json.dumps(h["tags"], ensure_ascii=False), h["source"], h["date"]
        ))
    conn.commit()

def init_db(db_path: str | Path = DB_PATH):
    db_path = Path(db_path)
    print(f"Initializing SQL Database at {db_path}...")
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    # 1. Hypotheses Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS hypotheses (
            id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            evidence TEXT,
            status TEXT,
            tags TEXT,
            source TEXT,
            date TEXT
        )
    ''')

    # 2. Memories Table (For autonomous agent observations)
    c.execute('''
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            content TEXT,
            concept_tags TEXT,
            significance INTEGER DEFAULT 1,
            domain_tags TEXT,
            supports_hypothesis TEXT,
            challenges_hypothesis TEXT
        )
    ''')

    # 3. Simulations Table (For the Colab execution queue)
    c.execute('''
        CREATE TABLE IF NOT EXISTS simulations (
            id TEXT PRIMARY KEY,
            queued_at TEXT,
            parameters TEXT,
            status TEXT
        )
    ''')

    # 4. Metadata Table (For Danny's persona mapping, journey, and stats)
    c.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    # 5. Confidence History Table (Hypothesis evolution over time)
    c.execute('''
        CREATE TABLE IF NOT EXISTS confidence_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_id TEXT NOT NULL,
            confidence REAL NOT NULL,
            source TEXT,
            reason TEXT,
            timestamp TEXT NOT NULL
        )
    ''')

    # 6. Ingestion Dead Letter Queue (files that repeatedly fail parsing)
    c.execute('''
        CREATE TABLE IF NOT EXISTS dead_letters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            error TEXT,
            attempts INTEGER NOT NULL DEFAULT 1,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL
        )
    ''')
    
    # 7. Hypothesis Review Council Audit Trail (Phase 18)
    c.execute('''
        CREATE TABLE IF NOT EXISTS hypothesis_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_id TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            verdict TEXT,
            reasoning TEXT,
            objections TEXT,
            score_contributions TEXT,
            review_details TEXT,
            timestamp TEXT NOT NULL
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS hypothesis_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_id TEXT NOT NULL,
            decision TEXT NOT NULL,
            final_score REAL,
            score_breakdown TEXT,
            merged_with TEXT,
            reasoning TEXT,
            timestamp TEXT NOT NULL
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS hypothesis_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_id TEXT NOT NULL,
            previous_status TEXT,
            new_status TEXT NOT NULL,
            rationale TEXT NOT NULL,
            actor TEXT,
            timestamp TEXT NOT NULL
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS memory_link_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id INTEGER NOT NULL,
            hypothesis_id TEXT NOT NULL,
            relation TEXT NOT NULL,
            score REAL NOT NULL,
            rationale TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            source TEXT,
            proposed_at TEXT NOT NULL,
            reviewed_at TEXT,
            reviewer_note TEXT
        )
    ''')
    c.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_link_proposals_unique
        ON memory_link_proposals (memory_id, hypothesis_id, relation)
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS evidence_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_id TEXT NOT NULL,
            request_text TEXT NOT NULL,
            priority TEXT NOT NULL DEFAULT 'medium',
            source_agent TEXT,
            source_context TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            triggering_decision TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            resolved_at TEXT,
            resolution_note TEXT,
            satisfied_by_memory_ids TEXT
        )
    ''')
    c.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_evidence_requests_unique
        ON evidence_requests (hypothesis_id, request_text)
    ''')

    # 8. High-Energy Transients (Phase 19)
    c.execute('''
        CREATE TABLE IF NOT EXISTS transients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,           -- 'Swift/BAT', 'Fermi/LAT', 'MAXI/GSC'
            target TEXT NOT NULL,           -- 'Sgr A*', 'Sgr B2', etc.
            flux REAL,
            error REAL,
            energy_band TEXT,               -- e.g., '15-50 keV'
            timestamp TEXT NOT NULL
        )
    ''')

    # 9. Mission Tracker (Phase 20)
    c.execute('''
        CREATE TABLE IF NOT EXISTS missions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mission_id TEXT NOT NULL,       -- e.g. '-1024'
            mission_name TEXT NOT NULL,     -- 'Artemis II'
            timestamp TEXT NOT NULL,
            ra REAL,
            dec REAL,
            alt REAL,
            az REAL,
            dist_au REAL,
            dist_mi REAL,                   -- NASA high-fidelity (miles)
            vel_km_s REAL,
            vel_mph REAL,                   -- NASA high-fidelity (mph)
            status TEXT
        )
    ''')

    # 10. Persistent chat history for the journal UI
    c.execute('''
        CREATE TABLE IF NOT EXISTS chat_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            metadata TEXT
        )
    ''')

    # 11. MAST queue for JWST/HST archive jobs
    c.execute('''
        CREATE TABLE IF NOT EXISTS mast_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_name TEXT NOT NULL,
            queued_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_run TEXT
        )
    ''')

    # 12. Radio queue for ALMA/SETI work
    c.execute('''
        CREATE TABLE IF NOT EXISTS radio_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_name TEXT NOT NULL,
            target_type TEXT NOT NULL,
            queued_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )
    ''')

    c.execute("UPDATE mast_queue SET status='running' WHERE status='processing'")
    c.execute("UPDATE mast_queue SET status='done' WHERE status='completed'")
    c.execute("UPDATE radio_queue SET status='done' WHERE status='completed'")

    hypothesis_columns = {row[1] for row in c.execute("PRAGMA table_info(hypotheses)").fetchall()}
    hypothesis_migrations = {
        "origin": "ALTER TABLE hypotheses ADD COLUMN origin TEXT",
        "parent_id": "ALTER TABLE hypotheses ADD COLUMN parent_id TEXT",
        "root_id": "ALTER TABLE hypotheses ADD COLUMN root_id TEXT",
        "merged_into": "ALTER TABLE hypotheses ADD COLUMN merged_into TEXT",
        "created_at": "ALTER TABLE hypotheses ADD COLUMN created_at TEXT",
        "updated_at": "ALTER TABLE hypotheses ADD COLUMN updated_at TEXT",
        "confidence": "ALTER TABLE hypotheses ADD COLUMN confidence REAL",
        "confidence_components": "ALTER TABLE hypotheses ADD COLUMN confidence_components TEXT",
        "confidence_source": "ALTER TABLE hypotheses ADD COLUMN confidence_source TEXT",
        "context_hypotheses": "ALTER TABLE hypotheses ADD COLUMN context_hypotheses TEXT",
        "context_domains": "ALTER TABLE hypotheses ADD COLUMN context_domains TEXT",
    }
    for column_name, migration_sql in hypothesis_migrations.items():
        if column_name not in hypothesis_columns:
            c.execute(migration_sql)

    chat_columns = {row[1] for row in c.execute("PRAGMA table_info(chat_history)").fetchall()}
    if "metadata" not in chat_columns:
        c.execute("ALTER TABLE chat_history ADD COLUMN metadata TEXT")

    memory_columns = {row[1] for row in c.execute("PRAGMA table_info(memories)").fetchall()}
    memory_migrations = {
        "domain_tags": "ALTER TABLE memories ADD COLUMN domain_tags TEXT",
        "supports_hypothesis": "ALTER TABLE memories ADD COLUMN supports_hypothesis TEXT",
        "challenges_hypothesis": "ALTER TABLE memories ADD COLUMN challenges_hypothesis TEXT",
    }
    for column_name, migration_sql in memory_migrations.items():
        if column_name not in memory_columns:
            c.execute(migration_sql)

    review_columns = {row[1] for row in c.execute("PRAGMA table_info(hypothesis_reviews)").fetchall()}
    if "review_details" not in review_columns:
        c.execute("ALTER TABLE hypothesis_reviews ADD COLUMN review_details TEXT")

    seed_foundational_hypotheses(conn)
    conn.commit()
    return conn

def migrate_data(conn, json_path: str | Path = JSON_PATH):
    json_path = Path(json_path)
    if not json_path.exists():
        print(f"No {json_path} found to migrate.")
        return

    print("Migrating historic data from JSON to SQL...")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    c = conn.cursor()

    # Migrate Metadata
    for key in ["identity", "journey", "manatuabon_stats"]:
        if key in data:
            c.execute('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 
                      (key, json.dumps(data[key], ensure_ascii=False)))

    # Migrate H1-H13 (Original Hypotheses)
    original_hyps = data.get("science_profile", {}).get("hypothesis_evolution", [])
    for h in original_hyps:
        c.execute('''
            INSERT OR IGNORE INTO hypotheses (id, title, description, evidence, status, tags, source, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            h.get("id"), h.get("title"), h.get("desc"), h.get("evidence"), 
            h.get("status"), json.dumps(h.get("tags", [])), h.get("source"), h.get("date")
        ))
        
    # Migrate Auto-Hypotheses (Agent Generated)
    auto_hyps = data.get("auto_hypotheses", [])
    for h in auto_hyps:
        # Agent hypotheses might have slightly different keys, we map them over
        c.execute('''
            INSERT OR IGNORE INTO hypotheses (id, title, description, evidence, status, tags, source, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            h.get("id"), h.get("title"), h.get("content"), "", 
            "active", json.dumps([]), "Manatuabon Agent", h.get("timestamp")
        ))

    # Migrate Agent Memories
    memories = data.get("agent_memories", [])
    for m in memories:
        c.execute('''
            INSERT INTO memories (timestamp, content, concept_tags)
            VALUES (?, ?, ?)
        ''', (
            m.get("timestamp"), m.get("content"), json.dumps(m.get("concept_tags", []))
        ))
        
    # Migrate Simulation Queue
    # simulation_queue entries may be plain strings (legacy) or dicts (structured)
    sims = data.get("simulation_queue", [])
    for s in sims:
        if isinstance(s, str):
            # Legacy format: bare string is the simulation name, no id/params yet
            c.execute('''
                INSERT OR IGNORE INTO simulations (id, queued_at, parameters, status)
                VALUES (?, ?, ?, ?)
            ''', (
                None, None, json.dumps({"name": s}), "pending"
            ))
        else:
            c.execute('''
                INSERT OR IGNORE INTO simulations (id, queued_at, parameters, status)
                VALUES (?, ?, ?, ?)
            ''', (
                s.get("sim_id"), s.get("requested_at"), json.dumps(s.get("parameters", {})), s.get("status", "pending")
            ))

    conn.commit()
    print("✅ Migration Complete! manatuabon.db is ready.")

def ensure_runtime_db(db_path: str | Path = DB_PATH, json_path: str | Path = JSON_PATH, migrate: bool = False):
    """Ensure runtime tables exist and optionally migrate legacy JSON data."""
    conn = init_db(db_path)
    if migrate:
        migrate_data(conn, json_path)
    return conn

if __name__ == "__main__":
    connection = ensure_runtime_db(migrate=True)
    connection.close()
