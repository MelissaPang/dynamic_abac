-- Unity Catalog row filter (ABAC): staff see patients from crosswalk unless is_excluded = 1
-- for their email. Replace melissap.demo_dynamic_abac with your {catalog}.{schema}.
--
-- Querying users need:
--   SELECT on staff_patient_crosswalk (referenced by the filter)
--   EXECUTE on check_patient_access
--   SELECT on the protected *_with_abac tables
--
-- Example grants (adjust grantee for your workspace):
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

ALTER TABLE melissap.demo_dynamic_abac.patient_demographics_with_abac
SET ROW FILTER melissap.demo_dynamic_abac.check_patient_access ON (patient_id);

ALTER TABLE melissap.demo_dynamic_abac.patient_claims_with_abac
SET ROW FILTER melissap.demo_dynamic_abac.check_patient_access ON (patient_id);
