# Azure Databricks Healthcare Lakehouse

**End-to-End Data Engineering | Azure Databricks | Delta Lake | PySpark | Unity Catalog | ADLS Gen2**

## Executive Overview

This project implements an end-to-end healthcare analytics Lakehouse on Microsoft Azure using Azure Databricks, Apache Spark, Delta Lake, and Azure Data Lake Storage Gen2.

The solution demonstrates the engineering lifecycle from file-based ingestion through incremental Bronze processing, Silver dimensional modeling, and Gold analytical aggregation, orchestrated using a dependency-aware Databricks Job.

The primary analytical use case is **hospital utilization, patient readmissions, and associated healthcare costs**.

The architecture prioritizes separation of responsibilities, incremental processing, data governance, reproducibility, and operational transparency.

**Project status:** Functional demonstration completed. All nine workflow tasks successfully executed. Additional controls would be required before production deployment.

## 1. Business Problem

Healthcare organizations need reliable analytical data to understand hospital utilization, patient encounters, readmission patterns, and financial outcomes.

Operational data frequently arrives across separate datasets with different structures and relationships. Without a governed integration layer, analytical teams must repeatedly reconstruct these relationships, increasing the risk of inconsistent reporting.

This project establishes a unified analytical foundation across four source domains:

- Patients
- Hospitals
- Diagnoses
- Hospital visits

The resulting Gold dataset supports analysis of encounter volume, 30-day readmissions, total visit costs, and average visit costs by hospital and diagnosis.

## 2. Solution Architecture

```text
          HEALTHCARE SOURCE DATA
        Patients | Hospitals | Diagnoses | Visits
                         |
                         v
                Azure Data Lake Storage
                       Gen2
                Staging CSV Files
                         |
                         v
               DATABRICKS BRONZE
              Auto Loader + Delta Lake
                         |
              +----------+----------+
              |          |          |
              v          v          v
         Diagnosis    Hospital    Patient
         Dimension    Dimension   Dimension
              |          |          |
              +----------+----------+
                         |
                Visits Bronze Data
                         |
                         v
                SILVER FACT VISIT
              Validated Joins + MERGE
                         |
                         v
                 GOLD ANALYTICS
               Hospital Disease KPIs
                         |
                         v
                 Databricks SQL
                 Analytical Queries
```

**Orchestration:** Databricks Jobs coordinates all nine tasks using explicit upstream dependencies. Independent ingestion tasks can execute concurrently, while downstream transformations wait for their required upstream tasks.

## 3. Technology Stack

| Component | Technology | Responsibility |
|---|---|---|
| Cloud platform | Microsoft Azure | Infrastructure and cloud services |
| Storage | ADLS Gen2 | Source file landing and staging |
| Processing | Azure Databricks | Distributed data transformation |
| Compute engine | Apache Spark / PySpark | Data processing and analytical transformations |
| Table format | Delta Lake | Transactional Lakehouse tables |
| Incremental file ingestion | Databricks Auto Loader | File discovery and ingestion |
| Governance | Unity Catalog | Catalog, schema, and storage access governance |
| Transformation | Structured Streaming, Delta MERGE | Incremental processing and upserts |
| Orchestration | Databricks Jobs | Task execution and dependency management |
| Version control | GitHub | Source code management |

## 4. Medallion Architecture

### Bronze — Source Ingestion

The Bronze layer ingests healthcare CSV files from ADLS Gen2 using Databricks Auto Loader.

Implemented capabilities include:

- File-based incremental discovery
- Schema tracking
- Structured Streaming checkpoints
- Delta table persistence
- Available-now processing for bounded ingestion runs

**Bronze tables**

| Table | Validated rows |
|---|---:|
| `bronze.diagnosis_raw` | 5 |
| `bronze.hospitals_raw` | 5 |
| `bronze.patients_raw` | 5 |
| `bronze.visits_raw` | 8 |
| **Total** | **23** |

These are small demonstration datasets. Their row counts establish functional correctness for the initial load, not scalability or performance at production volumes.

### Silver — Standardization and Dimensional Modeling

The Silver layer prepares integrated, analytically useful datasets.

Three dimension tables are constructed from their corresponding Bronze sources:

- `silver.dim_diagnosis`
- `silver.dim_hospital`
- `silver.dim_patient`

The fact table, `silver.fact_visit`, integrates visits with the three dimensions.

The notebooks implement business-key deduplication and Delta MERGE operations through Structured Streaming `foreachBatch` processing.

**Validated Silver tables**

| Table | Rows | Business key |
|---|---:|---|
| `silver.dim_diagnosis` | 5 | `diagnosis_code` |
| `silver.dim_hospital` | 5 | `hospital_id` |
| `silver.dim_patient` | 5 | `patient_id` |
| `silver.fact_visit` | 8 | `visit_id` |

The resulting dimensional model provides a reusable foundation for analytical queries.

**Privacy qualification:** The patient transformation creates a SHA-256-derived name field, but the source name columns remain present. This is not comprehensive de-identification or a validated PHI protection strategy.

### Gold — Business Analytics

The Gold layer materializes:

`healthcare_dev.gold.hospital_disease_kpi`

The transformation reads the Silver visit fact, uses patient-level window functions to examine preceding encounters, derives a 30-day readmission indicator, and aggregates results.

**Implemented measures**

| Measure | Definition |
|---|---|
| Total visits | Count of visit records |
| Total readmissions | Sum of qualifying readmission indicators |
| Readmission rate | Qualifying readmissions divided by visits |
| Total cost | Sum of encounter costs |
| Average cost | Mean encounter cost |

The output grain is **hospital and diagnosis**, not hospital alone.

The Gold table is rebuilt using an overwrite operation. It is therefore a materialized analytical summary rather than an incrementally maintained Gold table.

**Analytical limitation:** The readmission calculation is a simplified demonstration definition. Clinical readmission reporting would require validated encounter sequencing, exclusion rules, patient attribution, and a formally approved measure definition.

## 5. Workflow Orchestration

The Databricks Job is named:

`end_to_end_healthcare_project_job`

It contains nine tasks organized into three processing stages.

| Stage | Task | Dependency |
|---|---|---|
| Bronze | `diagnosis_raw_ingestion_task` | None |
| Bronze | `hospitals_raw_ingestion_task` | None |
| Bronze | `patients_ingestion_task` | None |
| Bronze | `visits_raw_ingestion_task` | None |
| Silver | `diagnosis_silver-task` | Diagnosis Bronze |
| Silver | `dim_hospital_ingestion` | Hospital Bronze |
| Silver | `dim_patient_ingestion` | Patient Bronze |
| Silver | `fact_visit_ingestion` | Three Silver dimensions and Visits Bronze |
| Gold | `resubmission_analysis` | Silver fact visit |

The final task name is retained as configured in the Databricks workspace; its implementation performs readmission analysis.

### Execution Validation

**Latest validated end-to-end run**

- Execution status: Succeeded
- Successful tasks: 9 of 9
- Total execution duration: 2 minutes, 10 seconds
- Execution mode: Manual
- Compute: Databricks Serverless

This result verifies that the configured task graph executed successfully against the demonstration environment.

It does not independently establish production SLAs, sustained throughput, recovery guarantees, or data reconciliation under failure conditions.

## 6. Governance and Storage Access

The implementation uses Unity Catalog to organize and govern Lakehouse assets.

**Catalog:** `healthcare_dev`

**Schemas:** `bronze`, `silver`, `gold`

ADLS Gen2 access is configured through a Unity Catalog storage credential and external location, using an Azure managed identity access connector.

This avoids embedding storage account access keys directly in the ingestion notebooks.

For production deployment, additional governance requirements would include role-based access reviews, patient-data classification, least-privilege controls, audit requirements, and appropriate retention policies.

## 7. Reliability and Incremental Processing

The implementation demonstrates several foundational reliability patterns.

**Checkpoint-based file ingestion:** Auto Loader tracks previously processed files, supporting subsequent runs without indiscriminately reprocessing the original input files.

**Streaming microbatch processing:** Silver transformations use `foreachBatch` to apply Delta operations.

**Business-key MERGE:** Silver tables use keyed upserts to support updates to existing business entities.

**Dependency-aware orchestration:** Downstream tasks do not begin until their configured upstream dependencies complete successfully.

These capabilities are valuable foundations, but they are not a complete production reliability framework.

In particular, source-side CDC, delete propagation, schema evolution policy, late-arriving data behavior, automated reconciliation, and recovery from partial failures require additional testing or implementation.

## 8. Repository Structure

```text
azure-databricks-healthcare-lakehouse/
|
|-- NB_Bronze_Diagnosis_AutoLoader.ipynb
|-- NB_Bronze_Diagnosis_Ingestion.ipynb
|-- NB_Bronze_Hospitals_AutoLoader.ipynb
|-- NB_Bronze_Patients_AutoLoader.ipynb
|-- NB_Bronze_Visits_AutoLoader.ipynb
|
|-- NB_Silver_Diagnosis_Merge.ipynb
|-- NB_Silver_Hospital_Merge.ipynb
|-- NB_Silver_Patient_Merge.ipynb
|-- NB_Silver_Fact_Visit_Merge.ipynb
|
|-- NB_Gold_Hospital_Disease_KPI.ipynb
|
|-- README.md
```

The repository contains 10 notebooks, including an additional diagnosis ingestion notebook outside the nine-task orchestrated workflow.

The Databricks Job definition, infrastructure provisioning, and environment configuration are not currently maintained as deployable artifacts in this repository.

## 9. Deployment and Operational Considerations

The demonstrated solution currently uses manually staged source files and a manually executed Databricks Job.

A production implementation would require explicit decisions around:

| Area | Recommended next consideration |
|---|---|
| Source integration | Azure SQL, REST API, or other supported connectors |
| Change capture | Source-supported CDC, change tracking, or controlled incremental extraction |
| Scheduling | Job schedule aligned to upstream data availability and freshness SLAs |
| Data quality | Schema, uniqueness, completeness, referential integrity, and reconciliation tests |
| Observability | Job failure alerts, dataset freshness, record counts, and execution audit history |
| Recovery | Checkpoint management, replay procedures, idempotency, and backfill testing |
| Security | Data classification, identity boundaries, access policies, and PHI safeguards |
| Deployment | Declarative job configuration and automated promotion across environments |
| Cost management | Compute sizing, workload scheduling, and storage lifecycle policies |

An ADLS-first ingestion architecture may be appropriate when durable raw extracts and replay are required. Direct managed ingestion should also be evaluated when source-supported connectors can reliably capture changes.

Neither approach should be selected solely because it is already familiar.

## 10. Engineering Decisions and Tradeoffs

### Why Auto Loader?

Auto Loader provides an incremental file-ingestion mechanism suited to the project's ADLS staging architecture. Checkpointing separates new-file discovery from repeated full-directory ingestion.

### Why Delta Lake?

Delta Lake supports transactional table operations and MERGE-based transformations, making it suitable for the implemented Silver integration pattern.

### Why separate dimensions from facts?

The dimensional model separates descriptive business entities from visit-level events, reducing repeated business logic and supporting consistent analytical joins.

### Why orchestrate dependencies explicitly?

The fact table depends on multiple dimensions and the visit source. Encoding those dependencies in the workflow prevents downstream tasks from starting before the required upstream tasks have succeeded.

### Why not claim production readiness?

A successful workflow execution demonstrates functional integration, not operational maturity. Production readiness requires measurable evidence of correctness, recoverability, security, performance, and ongoing supportability.

## 11. Current Status and Next Engineering Milestones

**Completed**

- ADLS Gen2 staging configuration
- Unity Catalog and managed-identity-backed storage access
- Bronze Auto Loader ingestion
- Silver dimensional transformations
- Silver fact integration
- Gold analytical aggregation
- Nine-task Databricks orchestration
- Successful end-to-end workflow execution
- GitHub notebook version control

**Next milestones**

1. Automate source extraction and define source change semantics.
2. Configure scheduled execution and failure notifications.
3. Validate incremental inserts, updates, deletes, and replay scenarios.
4. Introduce automated data-quality and reconciliation checks.
5. Version-control the Databricks Job definition and deployment configuration.
6. Establish operational metrics, cost visibility, and recovery procedures.

## 12. Project Context

This repository is an educational, hands-on implementation based on the Azure Databricks healthcare Lakehouse tutorial by [Anirvan De](https://github.com/anirvandecodes/Azure-Databricks-Real-World-Healthcare-End-to-End-Project).

The implementation has been configured and executed in a separate Azure environment, with project-specific storage, catalog, notebook, and workflow settings.

The goal is to demonstrate practical data engineering concepts and document the architectural considerations required to evolve a functional prototype into a supportable enterprise data platform.

---

**Engineering principle:** A successful pipeline run establishes that processing completed. A dependable data platform must also demonstrate that the correct data arrived, the transformations produced the intended business result, failures are recoverable, and downstream consumers receive trustworthy data when they need it.
