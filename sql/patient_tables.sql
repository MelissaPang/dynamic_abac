-- Synthetic patient demographics and claims (no real PHI).
-- Delta tables are created by the DAB job (see resources/seed_patient_tables.job.yml).
-- Logical shape for reference; in UC use: {catalog}.demo_dynamic_abac.*
-- The seed job also creates identical Delta copies for ABAC demos:
--   patient_demographics_with_abac  (same columns as patient_demographics)
--   patient_claims_with_abac        (same columns as patient_claims)

CREATE TABLE patient_demographics (
    patient_id           TEXT PRIMARY KEY,
    first_name           TEXT NOT NULL,
    last_name            TEXT NOT NULL,
    date_of_birth        DATE NOT NULL,
    sex_at_birth         TEXT,
    race_ethnicity       TEXT,
    zip_code             TEXT,
    state                TEXT,
    phone                TEXT,
    insurance_member_id  TEXT
);

CREATE TABLE patient_claims (
    claim_id         TEXT PRIMARY KEY,
    patient_id       TEXT NOT NULL REFERENCES patient_demographics (patient_id),
    service_date     DATE NOT NULL,
    procedure_code   TEXT,
    diagnosis_code   TEXT,
    billed_amount    NUMERIC(12, 2),
    paid_amount      NUMERIC(12, 2),
    payer            TEXT,
    claim_status     TEXT
);
