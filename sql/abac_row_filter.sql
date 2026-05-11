-- Unity Catalog: patient ABAC / row-level visibility using UDF check_patient_access.
-- Replace melissap.demo_dynamic_abac with your {catalog}.{schema}.
--
-- Querying users need SELECT on staff_patient_crosswalk and EXECUTE on check_patient_access.
--
-- Example grants:
--   GRANT EXECUTE ON FUNCTION melissap.demo_dynamic_abac.check_patient_access TO `account users`;
--   GRANT SELECT ON TABLE melissap.demo_dynamic_abac.staff_patient_crosswalk TO `account users`;

CREATE OR REPLACE FUNCTION melissap.demo_dynamic_abac.check_patient_access(pid STRING)
RETURNS BOOLEAN
RETURN ((
  SELECT CASE
    WHEN EXISTS (
      SELECT 1
      FROM melissap.demo_dynamic_abac.staff_patient_crosswalk c
      WHERE c.staff_email = CURRENT_USER()
        AND c.patient_id = pid
        AND CAST(c.is_excluded AS INT) = 1
    ) THEN FALSE
    ELSE TRUE
  END
));

-- Path A — Inline row filter (works on any UC workspace with EXECUTE on the UDF).
-- Drop first if you are switching from Path B:
--   ALTER TABLE melissap.demo_dynamic_abac.patient_demographics_with_abac DROP ROW FILTER;
--   ALTER TABLE melissap.demo_dynamic_abac.patient_claims_with_abac DROP ROW FILTER;

ALTER TABLE melissap.demo_dynamic_abac.patient_demographics_with_abac
SET ROW FILTER melissap.demo_dynamic_abac.check_patient_access ON (patient_id);

ALTER TABLE melissap.demo_dynamic_abac.patient_claims_with_abac
SET ROW FILTER melissap.demo_dynamic_abac.check_patient_access ON (patient_id);

-- Path B — ABAC row filter policy (Databricks Runtime 16.4+ / serverless).
-- Policy ``has_tag*`` conditions only accept tag keys your account registered for policies
-- (often governed tags). If ``CREATE POLICY`` fails with ``Unknown tag policy key``, use
-- Path A or register tags under Catalog > Govern, then attach policy at schema scope.
--
-- Example (run Path A DROP ROW FILTER lines first; do not keep both Path A and B active):
--
-- SET TAG ON TABLE melissap.demo_dynamic_abac.patient_demographics_with_abac dynamic_abac_table = true;
-- SET TAG ON TABLE melissap.demo_dynamic_abac.patient_claims_with_abac dynamic_abac_table = true;
-- SET TAG ON COLUMN melissap.demo_dynamic_abac.patient_demographics_with_abac.patient_id dynamic_abac_table = true;
-- SET TAG ON COLUMN melissap.demo_dynamic_abac.patient_claims_with_abac.patient_id dynamic_abac_table = true;
--
-- CREATE OR REPLACE POLICY patient_crosswalk_abac
-- ON SCHEMA melissap.demo_dynamic_abac
-- COMMENT 'ABAC row filter via check_patient_access'
-- ROW FILTER melissap.demo_dynamic_abac.check_patient_access
-- TO `account users`
-- FOR TABLES
-- WHEN has_tag_value('dynamic_abac_table', 'true')
-- MATCH COLUMNS has_tag_value('dynamic_abac_table', 'true') AS pid
-- USING COLUMNS (pid);
