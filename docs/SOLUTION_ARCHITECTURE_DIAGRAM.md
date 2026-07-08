# NY IEDR Data Platform - Solution Architecture Diagram

## 📊 High-Level Architecture

```mermaid
graph LR
    subgraph "Data Sources"
        U1["🏢 Utility 1<br/>Segment-level CSVs<br/>64,539 segments<br/>Wide DER format"]
        U2["🏢 Utility 2<br/>Feeder-level CSVs<br/>1,909 feeders<br/>Narrow DER format"]
    end

    subgraph "Landing Zone"
        V1["📦 Volume: landing/1/<br/>Circuits, DER CSV files"]
        V2["📦 Volume: landing/2/<br/>Circuits, DER CSV files"]
    end

    subgraph "Bronze Layer (Raw Storage)"
        B1["🥉 circuits_raw<br/>66,448 records<br/>All STRING columns<br/>Artifact clearing"]
        B2["🥉 der_installed_raw<br/>39,657 projects"]
        B3["🥉 der_planned_raw<br/>32,689 projects"]
        B4["📋 file_tracking<br/>Idempotency audit"]
    end

    subgraph "Utility Registry"
        REG["⚙️ N-Utility Registry<br/>utility_registry.py<br/>━━━━━━━━━━━━━━<br/>Config-driven onboarding<br/>3 transformers per utility"]
    end

    subgraph "Silver Layer (Standardized)"
        S1["🥈 circuits_standardized<br/>2,178 feeders<br/>MODE aggregation (utility1)<br/>Color standardization"]
        S2["🥈 der_installed_standardized<br/>39,657 projects<br/>Unpivot (utility1)<br/>210 unresolved feeders"]
        S3["🥈 der_planned_standardized<br/>32,689 projects<br/>919 unresolved feeders"]
        S4["📊 data_quality_metrics_silver<br/>Freshness monitoring<br/>Volume tracking<br/>Null key detection"]
    end

    subgraph "Gold Layer (Historical + API)"
        G1["🥇 circuits_current<br/>SCD Type 2<br/>KEY: feeder_id, utility_id<br/>SEQ: hca_refresh_date<br/>Liquid Clustered"]
        G2["🥇 der_installed_current<br/>SCD Type 2<br/>KEY: der_id, utility_id<br/>SEQ: ingestion_date"]
        G3["🥇 der_planned_current<br/>SCD Type 2<br/>KEY: der_id, utility_id<br/>SEQ: ingestion_date"]
        G4["📍 feeder_map_layer<br/>Denormalized map view<br/>Geometry + district clustering<br/>Zero-join rendering"]
        G5["📈 feeders_with_capacity<br/>API-optimized current view<br/>Available capacity > 0"]
        G6["📈 feeder_der_summary<br/>Pre-aggregated DER counts<br/>Per feeder summary"]
    end

    subgraph "Observability"
        OBS1["👁️ Freshness Monitoring<br/>days_since_refresh<br/>Alert: > 45 days"]
        OBS2["📊 Volume Baseline<br/>30-day rolling avg ± 2σ<br/>ANOMALY_LOW/HIGH flags"]
        OBS3["🚨 Data Quality Alerts<br/>Null keys, unresolved feeders<br/>Email + Slack + PagerDuty"]
    end

    subgraph "Consumption Layer"
        API["🌐 REST API<br/>Query Gold current views"]
        DASH["📊 BI Dashboards<br/>Map + metrics"]
        APPS["📱 Interactive Map App<br/>Feeder drill-down<br/>DER details"]
    end

    %% Data Flow
    U1 --> V1
    U2 --> V2
    V1 --> B1
    V1 --> B2
    V1 --> B3
    V2 --> B1
    V2 --> B2
    V2 --> B3
    
    B1 --> B4
    B2 --> B4
    B3 --> B4
    
    B1 --> REG
    B2 --> REG
    B3 --> REG
    
    REG --> S1
    REG --> S2
    REG --> S3
    
    S1 --> S4
    S2 --> S4
    S3 --> S4
    
    S1 --> G1
    S2 --> G2
    S3 --> G3
    
    G1 --> G4
    G2 --> G4
    G3 --> G4
    
    G1 --> G5
    G1 --> G6
    G2 --> G6
    G3 --> G6
    
    S4 --> OBS1
    S4 --> OBS2
    S4 --> OBS3
    
    G4 --> API
    G5 --> API
    G6 --> API
    
    G4 --> DASH
    G5 --> DASH
    G6 --> DASH
    
    G4 --> APPS
    G5 --> APPS
    G6 --> APPS

    %% Node Styling
    style U1 fill:#e1f5ff,color:#000
    style U2 fill:#e1f5ff,color:#000
    style B1 fill:#cd7f32,color:#fff
    style B2 fill:#cd7f32,color:#fff
    style B3 fill:#cd7f32,color:#fff
    style B4 fill:#cd7f32,color:#fff
    style REG fill:#ffd700,color:#000
    style S1 fill:#c0c0c0,color:#000
    style S2 fill:#c0c0c0,color:#000
    style S3 fill:#c0c0c0,color:#000
    style S4 fill:#c0c0c0,color:#000
    style G1 fill:#ffd700,color:#000
    style G2 fill:#ffd700,color:#000
    style G3 fill:#ffd700,color:#000
    style G4 fill:#ffd700,color:#000
    style G5 fill:#ffd700,color:#000
    style G6 fill:#ffd700,color:#000
    style OBS1 fill:#ff9999,color:#000
    style OBS2 fill:#ff9999,color:#000
    style OBS3 fill:#ff9999,color:#000
    style API fill:#99ff99,color:#000
    style DASH fill:#99ff99,color:#000
    style APPS fill:#99ff99,color:#000
```

---

## 🔄 Detailed Transformation Flow

```mermaid
flowchart LR
    subgraph "Bronze: Raw Ingestion"
        BR1["Auto Loader<br/>cloudFiles<br/>Schema evolution"]
        BR2["Artifact Clearing<br/>_c0, _file_path<br/>Case variant extraction"]
        BR3["File Tracking<br/>MD5 signature<br/>Idempotency"]
    end

    subgraph "Silver: Standardization"
        SR1["Registry Transformers<br/>Utility-specific logic<br/>Intermediate schema"]
        SR2["Canonical Mapping<br/>Type casting<br/>Date parsing<br/>Color standardization"]
        SR3["MODE Aggregation<br/>Segment → Feeder<br/>utility1 only"]
        SR4["DER Unpivot<br/>Wide → Narrow<br/>utility1 only"]
        SR5["Quality Checks<br/>@dlt.expect_or_drop<br/>Unresolved tracking"]
    end

    subgraph "Gold: History + API"
        GR1["Auto CDC<br/>SCD Type 2<br/>Track changes over time"]
        GR2["Liquid Clustering<br/>utility_id + feeder_id<br/>Query optimization"]
        GR3["Current Views<br/>__END_AT IS NULL<br/>API-optimized"]
        GR4["Map Layer<br/>Geometry + grid<br/>Denormalized"]
    end

    BR1 --> BR2
    BR2 --> BR3
    BR3 --> SR1
    SR1 --> SR2
    SR2 --> SR3
    SR3 --> SR4
    SR4 --> SR5
    SR5 --> GR1
    GR1 --> GR2
    GR2 --> GR3
    GR3 --> GR4

    style BR1 fill:#cd7f32,color:#fff
    style BR2 fill:#cd7f32,color:#fff
    style BR3 fill:#cd7f32,color:#fff
    style SR1 fill:#c0c0c0
    style SR2 fill:#c0c0c0
    style SR3 fill:#c0c0c0
    style SR4 fill:#c0c0c0
    style SR5 fill:#c0c0c0
    style GR1 fill:#ffd700
    style GR2 fill:#ffd700
    style GR3 fill:#ffd700
    style GR4 fill:#ffd700
```

---

## 🏗️ N-Utility Registry Pattern

```mermaid
graph TB
    subgraph "Utility Registry (utility_registry.py)"
        REG["UTILITY_REGISTRY = {<br/>  1: UtilityConfig(...),<br/>  2: UtilityConfig(...),<br/>  N: UtilityConfig(...)<br/>}"]
    end

    subgraph "Utility 1 Transformers"
        U1C["transform_utility1_circuits()<br/>Segment aggregation<br/>MODE capacity"]
        U1DI["transform_utility1_der_installed()<br/>Unpivot wide format<br/>14 tech columns → narrow"]
        U1DP["transform_utility1_der_planned()<br/>Unpivot wide format"]
    end

    subgraph "Utility 2 Transformers"
        U2C["transform_utility2_circuits()<br/>Feeder-level passthrough<br/>Direct mapping"]
        U2DI["transform_utility2_der_installed()<br/>Narrow format<br/>Direct mapping"]
        U2DP["transform_utility2_der_planned()<br/>Narrow format<br/>Direct mapping"]
    end

    subgraph "Silver Pipeline (02_silver_transformations.py)"
        LOOP["for utility_id, config in<br/>get_registered_utilities().items():<br/>━━━━━━━━━━━━━━━━<br/>  transform = config.circuits_transformer<br/>  intermediate = transform(bronze_df)<br/>  canonical = map_to_canonical(intermediate)"]
    end

    REG --> U1C
    REG --> U1DI
    REG --> U1DP
    REG --> U2C
    REG --> U2DI
    REG --> U2DP
    
    U1C --> LOOP
    U1DI --> LOOP
    U1DP --> LOOP
    U2C --> LOOP
    U2DI --> LOOP
    U2DP --> LOOP

    style REG fill:#ffd700,color:#000
    style LOOP fill:#c0c0c0
```

**Onboarding Utility 3:**
1. Write 3 transformer functions
2. Add to `UTILITY_REGISTRY`
3. Upload CSVs to `/Volumes/.../landing/3/`
4. **Zero pipeline code changes** ✅

---

## 📊 Data Quality & Observability Flow

```mermaid
graph TB
    subgraph "Data Quality Metrics Collection"
        DQ1["Silver Transformations<br/>Compute metrics:<br/>• Total records<br/>• Null keys<br/>• Unresolved feeders<br/>• Negative capacities"]
        DQ2["Freshness Calculation<br/>• last_refresh_date<br/>• days_since_refresh<br/>• Alert thresholds"]
    end

    subgraph "DQ Metrics Table"
        DQT["data_quality_metrics_silver<br/>━━━━━━━━━━━━━━━━━━<br/>Streaming table<br/>skipChangeCommits=true<br/>Append-only history"]
    end

    subgraph "Observability Queries"
        OBS1["volume_baseline_tracking.sql<br/>30-day rolling avg ± 2σ<br/>ANOMALY_LOW/HIGH detection"]
        OBS2["Freshness Monitoring<br/>CASE days_since_refresh<br/>  > 45: 🔴 STALE<br/>  > 30: ⚠️ AGING<br/>  ELSE: ✅ FRESH"]
        OBS3["Historical Trend Analysis<br/>LAG() window functions<br/>Day-over-day changes"]
    end

    subgraph "Alert Configuration"
        ALT1["🔴 Critical Alerts<br/>• Pipeline failure<br/>• null_key_count > 0<br/>• days_since_refresh > 45<br/>→ Email + Slack"]
        ALT2["🟠 High Alerts<br/>• Volume ANOMALY_LOW<br/>→ Slack"]
        ALT3["🟡 Medium Alerts<br/>• Volume ANOMALY_HIGH<br/>• unresolved > 1000<br/>→ Email"]
    end

    DQ1 --> DQT
    DQ2 --> DQT
    DQT --> OBS1
    DQT --> OBS2
    DQT --> OBS3
    OBS1 --> ALT1
    OBS1 --> ALT2
    OBS1 --> ALT3
    OBS2 --> ALT1
    OBS3 --> ALT2

    style DQT fill:#c0c0c0
    style ALT1 fill:#ff9999
    style ALT2 fill:#ffcc99
    style ALT3 fill:#ffff99
```

---

## 🗺️ Map Application Query Pattern

```mermaid
sequenceDiagram
    participant User
    participant MapUI
    participant API
    participant Gold

    Note over User,Gold: Initial Map Load
    User->>MapUI: Open IEDR Map
    MapUI->>API: GET /feeders?capacity>0
    API->>Gold: SELECT * FROM feeder_map_layer<br/>WHERE available_capacity_mw > 0
    Gold-->>API: 2,178 feeder geometries<br/>+ color_hex, DER counts
    API-->>MapUI: GeoJSON with properties
    MapUI-->>User: Render colored feeders

    Note over User,Gold: User Clicks Feeder
    User->>MapUI: Click feeder_id='utility1_1105354'
    MapUI->>API: GET /feeder/{id}/der
    API->>Gold: SELECT * FROM der_installed_current<br/>WHERE feeder_id='utility1_1105354'<br/>AND __END_AT IS NULL
    Gold-->>API: 45 installed DER projects
    API->>Gold: SELECT * FROM der_planned_current<br/>WHERE feeder_id='utility1_1105354'<br/>AND __END_AT IS NULL
    Gold-->>API: 12 planned DER projects
    API-->>MapUI: Two result sets
    MapUI-->>User: Show DER details table

    Note over User,Gold: Zero-Join Architecture
    rect rgb(200, 255, 200)
        Note over Gold: feeder_map_layer is denormalized<br/>All map data in ONE table<br/>No JOIN required ✅
    end
```

---

## 🔧 Technology Stack

```mermaid
graph LR
    subgraph "Ingestion"
        AL["Auto Loader<br/>cloudFiles<br/>Incremental"]
    end

    subgraph "Orchestration"
        SDP["Lakeflow Spark<br/>Declarative Pipelines<br/>(formerly DLT)"]
    end

    subgraph "Storage"
        DL["Delta Lake<br/>Liquid Clustering<br/>Time Travel"]
        UC["Unity Catalog<br/>Volumes + Tables<br/>Governance"]
    end

    subgraph "Compute"
        DBR["Databricks Runtime<br/>Serverless<br/>Photon enabled"]
    end

    subgraph "Testing"
        PT["pytest<br/>PySpark unit tests<br/>Schema validation"]
    end

    subgraph "Version Control"
        GH["GitHub<br/>ny-iedr-data-platform<br/>Feature branches"]
    end

    subgraph "Languages"
        PY["Python (PySpark)<br/>SQL<br/>Mermaid diagrams"]
    end

    AL --> SDP
    SDP --> DL
    DL --> UC
    UC --> DBR
    PT --> GH
    PY --> SDP

    style AL fill:#4da6ff
    style SDP fill:#ff9933
    style DL fill:#00cc66
    style UC fill:#9966ff
    style DBR fill:#ff6666
    style PT fill:#ffcc00
    style GH fill:#333,color:#fff
    style PY fill:#3399ff
```

---

## 📈 Current State (2026-07-08)

### ✅ **Production-Ready Status**

| Component | Status | Details |
|-----------|--------|----------|
| **Bronze Layer** | ✅ Complete | 66,448 circuits, 72,346 DER, artifact clearing |
| **Silver Layer** | ✅ Complete | MODE aggregation, unpivot, color standardization |
| **Gold Layer** | ✅ Complete | SCD Type 2, API views, map layer |
| **Observability** | ✅ Complete | Freshness monitoring, volume baseline, alerts |
| **Testing** | ✅ Validated | 2 successful runs (full refresh + incremental) |
| **Documentation** | ✅ Complete | ARCHITECTURE.md, JOB_ALERT_SETUP.md |
| **Code Quality** | ✅ Refactored | Silver: 450 → 306 lines (-144 lines) |

### 📊 **Data Volumes**
- **Circuits**: 2,178 feeders (269 utility1 + 1,909 utility2)
- **DER Installed**: 39,657 projects (14,120 utility1 + 25,537 utility2)
- **DER Planned**: 32,689 projects (1,733 utility1 + 30,956 utility2)
- **Total Tables**: 17 across 4 layers

### ⚠️ **Data Quality Findings**
- **Freshness**: Both utilities STALE (Oct 2022 data, 1,362-1,376 days old)
- **Unresolved Feeders**: 210 installed, 919 planned (need linkage improvement)
- **Null Keys**: 0 (all expectations passing) ✅

### 🚀 **Next Steps**
1. Deploy to `prod_iedr` catalog
2. Schedule pipeline (daily/weekly cadence)
3. Configure Databricks SQL Alerts
4. Set up monitoring dashboards
5. Production smoke tests

---

## 📚 File Locations

```
ny-iedr-data-platform/
├── pipelines/
│   ├── 01_bronze_ingestion.py          # Auto Loader, artifact clearing
│   ├── 02_silver_transformations.py    # MODE aggregation, registry loop
│   ├── 03_gold_scd2.py                 # SCD Type 2, API views
│   └── utils/
│       ├── helpers.py                  # Lineage, artifact helpers
│       ├── utility_registry.py         # N-utility config, transformers
│       └── schema_normalization.py     # Canonical mapping, colors
├── tests/
│   └── test_schema_normalization.py    # Unit tests (20+ cases)
├── docs/
│   ├── ARCHITECTURE.md                 # Detailed architecture
│   ├── SOLUTION_ARCHITECTURE_DIAGRAM.md # THIS FILE
│   ├── JOB_ALERT_SETUP.md             # Alert configuration guide
│   └── volume_baseline_tracking.sql    # Anomaly detection query
└── README.md                           # Project overview
```

---

**Legend:**
- 🥉 Bronze = Raw storage (STRING columns, no transformation)
- 🥈 Silver = Standardized (typed, aggregated, quality-checked)
- 🥇 Gold = Historical + API-ready (SCD2, liquid clustered)
- ⚙️ Registry = Configuration-driven utility onboarding
- 👁️ Observability = Monitoring, alerting, anomaly detection
- 🗺️ Map = Geospatial rendering layer
