# Model Risk Management (MRM) Policy — VERSION_2

**Version:** 2.0 (Production)  
**Scope:** ML models for cases #20 (ALS), #22 (supervised classification), #21 (LLM classification)  
**Regulatory Driver:** None (educational project). The architecture and data processing rules are designed with best practices in mind (analogous to CBR 657-P, SR 11-7, Basel Committee) solely to foster proper engineering habits. Full compliance with regulatory requirements and formal risk management procedures are not implemented in the current infrastructure.  
**Status:** Draft Document

---

## 1. Purpose and Scope

This policy establishes requirements for the development, validation, deployment, monitoring, and decommissioning of ML models used in the analytical cases of Bootcamp 2026.

Cases subject to MRM:
| # | Name | Model Type | Purpose |
|---|------|------------|---------|
| #20 | Hero segmentation by pick and counter-pick | ALS (Alternating Least Squares) | Collaborative filtering, hero recommendation |
| #22 | Picks and counter-picks (ML) | Supervised classification (LogReg / CatBoost) | Predicting Radiant victory based on draft |
| #21 | Nickname classification (LLM) | LLM API (classification) | Classifying nicknames by type |

---

## 2. Model Lifecycle

### 2.1 Development

Requirements:
- The training dataset is documented and cleaned (DQ checks passed).
- The version of the training data is fixed (Iceberg snapshot or watermark).
- Hyperparameters are fixed in the configuration.
- No target variable leakage (leakage protection).

**Specific requirements by case:**
- **#20 (ALS):** separation of users/heroes, fixed rank, maxIter, regParam; exclusion of leakage through the target variable in features.
- **#22 (Supervised):** temporal train/test split by `start_time` (not random by rows); target encoding only within train (out-of-fold); no post-match features (KDA, GPM, final rank) in features for pre-draft prediction.
- **#21 (LLM):** fixing API model, version, temperature, prompt; validation of JSON response schema; API error rate not exceeding 5% (DQ-CASE21-04).

### 2.2 Validation

Mandatory checks before deployment:

| Check | Description | Pass Criteria |
|-------|-------------|---------------|
| **Stability** | Results are reproducible on the same data | 100% of control runs |
| **Quality** | Metrics on the test set meet thresholds | Precision@K, Recall@K, NDCG, ROC-AUC ≥ specified threshold |
| **No Leakage** | Target variable does not leak into features | Code and feature review |
| **Drift** | Feature distribution on inference is close to training | No statistically significant differences |
| **Coverage** | All key segments are covered by the sample | No "cold" objects with 0 observations (DQ-CASE20-02) |

### 2.3 Deployment

- The model is saved with its version and training parameters.
- The result contains the model version identifier.
- Rollback to the previous version is possible.
- Access to the model is restricted (in this educational project, RBAC is not implemented, but it is architecturally planned for production).

### 2.4 Monitoring

Daily monitoring:
- Prediction quality on new data.
- API error rate (for LLM).
- Feature and target variable drift.
- Execution time (SLA: model training ≤ 2 hours, recommendation generation ≤ 15 minutes).

### 2.5 Decommissioning

A model is decommissioned if:
- Quality drops below the threshold for 3 consecutive runs.
- The data source is changed and is incompatible with the model.
- A critical leakage or logic error is detected.

---

## 3. Model Cards

Each model must have a Model Card with the following fields:

| Field | Description |
|-------|-------------|
| `model_id` | Unique model identifier |
| `version` | Version (e.g., `v1.0`) |
| `case` | Case number (#20, #22, #21) |
| `algorithm` | Algorithm (ALS, Logistic Regression, CatBoost, LLM API) |
| `training_data` | Source, period, number of records |
| `hyperparameters` | Fixed hyperparameters |
| `metrics` | Quality metrics on the test set |
| `limitations` | Limitations and known issues |
| `owner` | Responsible developer |
| `created_at` | Training date |
| `retired_at` | Decommissioning date (if applicable) |

---

## 4. Roles and Responsibilities

| Role | Responsibility |
|------|----------------|
| **Data Scientist** | Model development, hyperparameter tuning, training |
| **ML Engineer** | Deployment, monitoring, infrastructure |
| **Data Engineer** | Data preparation, DQ checks, pipeline |
| **Model Owner** | Responsibility for the model throughout its lifecycle |
| **Risk Reviewer** | Independent review before deployment |

---

## 5. Connection to SLA

The MRM policy complements the SLA:
- ETL run success rate ≥ 95%.
- Mandatory DQ checks pass for 100% of published marts.
- Processing reproducibility for 100% of control reruns.
- Lineage availability for 100% of described marts.

---

## 6. Exceptions and Appeals

Any deviation from the policy requires:
- Documented justification.
- Approval from the Model Owner and Risk Reviewer.
- Logging in the exceptions register with a date and validity period.