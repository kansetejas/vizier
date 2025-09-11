{% macro transform_all_staging_tables() %}
    
    {#
    --------------------------------------------------------------------------------
    MODEL 1: TRANSFORMED PATIENTS
    --------------------------------------------------------------------------------
    #}
    {% set transformed_patients_sql %}
        DROP TABLE IF EXISTS {{ target.schema }}.transformed_patients;
        CREATE TABLE {{ target.schema }}.transformed_patients AS
        WITH source AS (
            SELECT * FROM {{ target.schema }}.staging_patients
        ),
        cleaned AS (
            SELECT
                id::VARCHAR AS patient_id,
                CASE
                    WHEN birthdate ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN to_date(birthdate, 'DD-MM-YYYY')
                    WHEN birthdate ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN to_date(birthdate, 'YYYY-MM-DD')
                    ELSE NULL
                END AS birth_date,
                CASE
                    WHEN deathdate ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN to_date(deathdate, 'DD-MM-YYYY')
                    WHEN deathdate ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN to_date(deathdate, 'YYYY-MM-DD')
                    ELSE NULL
                END AS death_date,
                ssn::VARCHAR,
                drivers::VARCHAR AS drivers_license,
                passport::VARCHAR,
                prefix::VARCHAR,
                first::VARCHAR AS first_name,
                middle::VARCHAR AS middle_name,
                last::VARCHAR AS last_name,
                marital::VARCHAR AS marital_status,
                race::VARCHAR,
                ethnicity::VARCHAR,
                gender::VARCHAR,
                birthplace::VARCHAR,
                address::VARCHAR,
                city::VARCHAR,
                state::VARCHAR,
                zip::VARCHAR,
                NULLIF(TRIM(healthcare_expenses), '')::NUMERIC AS healthcare_expenses,
                NULLIF(TRIM(healthcare_coverage), '')::NUMERIC AS healthcare_coverage
            FROM source
        ),
        age_calculation AS (
             SELECT
                *,
                CASE WHEN birth_date IS NOT NULL THEN EXTRACT(YEAR FROM age(CURRENT_DATE, birth_date)) ELSE NULL END as age_years,
                CASE WHEN death_date IS NOT NULL THEN 1 ELSE 0 END AS is_deceased
             FROM cleaned
        )
        SELECT * FROM age_calculation;
    {% endset %}
    {{ log("Creating transformed_patients table", info=True) }}
    {{ run_query(transformed_patients_sql) }}

    {#
    --------------------------------------------------------------------------------
    MODEL 2: TRANSFORMED ENCOUNTERS
    --------------------------------------------------------------------------------
    #}
    {% set transformed_encounters_sql %}
        DROP TABLE IF EXISTS {{ target.schema }}.transformed_encounters;
        CREATE TABLE {{ target.schema }}.transformed_encounters AS
        WITH source AS (
            SELECT * FROM {{ target.schema }}.staging_encounters
        ),
        cleaned AS (
            SELECT
                id::VARCHAR AS encounter_id,
                NULLIF(TRIM(start), '')::TIMESTAMP AS start_timestamp,
                NULLIF(TRIM(stop), '')::TIMESTAMP AS stop_timestamp,
                patient::VARCHAR AS patient_id,
                organization::VARCHAR AS organization_id,
                provider::VARCHAR AS provider_id,
                payer::VARCHAR AS payer_id,
                encounterclass::VARCHAR AS encounter_class,
                code::VARCHAR AS procedure_code,
                description::VARCHAR AS procedure_description,
                NULLIF(TRIM(base_encounter_cost), '')::NUMERIC AS base_encounter_cost,
                NULLIF(TRIM(total_claim_cost), '')::NUMERIC AS total_claim_cost,
                NULLIF(TRIM(payer_coverage), '')::NUMERIC AS payer_coverage,
                reasoncode::VARCHAR AS reason_code,
                reasondescription::VARCHAR AS reason_description
            FROM source
        )
        SELECT * FROM cleaned;
    {% endset %}
    {{ log("Creating transformed_encounters table", info=True) }}
    {{ run_query(transformed_encounters_sql) }}
    
    {#
    --------------------------------------------------------------------------------
    MODEL 3: TRANSFORMED OBSERVATIONS
    --------------------------------------------------------------------------------
    #}
    {% set transformed_observations_sql %}
        DROP TABLE IF EXISTS {{ target.schema }}.transformed_observations;
        CREATE TABLE {{ target.schema }}.transformed_observations AS
        WITH source AS (
            SELECT * FROM {{ target.schema }}.staging_observations
        ),
        cleaned AS (
            SELECT
                NULLIF(TRIM(date), '')::TIMESTAMP AS observation_timestamp,
                patient::VARCHAR AS patient_id,
                encounter::VARCHAR AS encounter_id,
                category::VARCHAR,
                code::VARCHAR AS observation_code,
                description::VARCHAR AS observation_description,
                value::VARCHAR AS observation_value,
                units::VARCHAR
            FROM source
        )
        SELECT * FROM cleaned;
    {% endset %}
    {{ log("Creating transformed_observations table", info=True) }}
    {{ run_query(transformed_observations_sql) }}
    
    {#
    --------------------------------------------------------------------------------
    MODEL 4: CLINICAL ANALYTICS MART
    --------------------------------------------------------------------------------
    #}
    {% set clinical_analytics_sql %}
        DROP TABLE IF EXISTS {{ target.schema }}.clinical_analytics_mart;
        CREATE TABLE {{ target.schema }}.clinical_analytics_mart AS

        -- 1. Readmission Risk Calculation
        WITH readmission_risk AS (
            SELECT
                patient_id,
                SUM(CASE WHEN reason_code IN ('I10', 'E11') OR lower(reason_description) LIKE ANY (array['%hypertensi%', '%diabet%']) THEN 1 ELSE 0 END) AS chronic_conditions_count,
                SUM(CASE WHEN reason_code IN ('J18', 'A41') OR lower(reason_description) LIKE ANY (array['%pneumoni%', '%sepsis%']) THEN 1 ELSE 0 END) AS acute_conditions_count
            FROM {{ target.schema }}.transformed_encounters
            GROUP BY patient_id
        ),

        -- 2. Length of Stay (LOS) Calculation
        los_stats AS (
            SELECT
                e.encounter_id,
                e.patient_id,
                e.procedure_code,
                -- *** THE FIX IS HERE: Include start_timestamp so we can order by it later ***
                e.start_timestamp,
                EXTRACT(EPOCH FROM (e.stop_timestamp - e.start_timestamp)) / 86400.0 AS los_days,
                AVG(EXTRACT(EPOCH FROM (e.stop_timestamp - e.start_timestamp)) / 86400.0) OVER (PARTITION BY e.procedure_code) AS avg_los_for_procedure,
                STDDEV(EXTRACT(EPOCH FROM (e.stop_timestamp - e.start_timestamp)) / 86400.0) OVER (PARTITION BY e.procedure_code) AS stddev_los_for_procedure
            FROM {{ target.schema }}.transformed_encounters e
            WHERE e.start_timestamp IS NOT NULL AND e.stop_timestamp IS NOT NULL AND e.stop_timestamp > e.start_timestamp
        ),

        -- 3. Quality Measure Denominator & Exclusions
        quality_measures AS (
            SELECT
                p.patient_id,
                p.age_years,
                CASE WHEN p.age_years >= 65 AND p.is_deceased = 0 THEN 1 ELSE 0 END AS is_in_denominator_65_plus,
                MAX(CASE WHEN o.observation_code IN ('Z66', 'Z51.5') OR lower(o.observation_description) LIKE '%hospice%' THEN 1 ELSE 0 END) AS is_hospice_excluded
            FROM {{ target.schema }}.transformed_patients p
            LEFT JOIN {{ target.schema }}.transformed_observations o ON p.patient_id = o.patient_id
            GROUP BY p.patient_id, p.age_years, p.is_deceased
        ),
        
        -- 4. Care Gap Identification
        care_gaps AS (
             SELECT
                patient_id,
                MAX(CASE WHEN observation_code IN ('73761001', '46680005') THEN observation_timestamp END) AS last_colonoscopy_date,
                MAX(CASE WHEN observation_code IN ('24606-6', '39156-5') THEN observation_timestamp END) AS last_mammogram_date
             FROM {{ target.schema }}.transformed_observations
             GROUP BY patient_id
        )

        -- Final Join: Combine all analytics into one table per patient
        SELECT
            p.patient_id,
            p.age_years,
            p.gender,
            p.marital_status,
            p.race,
            p.state,
            -- Readmission Score
            (COALESCE(rr.chronic_conditions_count, 0) * 2 + COALESCE(rr.acute_conditions_count, 0) * 3) AS readmission_risk_score,
            -- LOS outlier flag (for the patient's most recent encounter)
            (SELECT 
                CASE 
                    WHEN ls.los_days > ls.avg_los_for_procedure + (2 * ls.stddev_los_for_procedure) THEN 1
                    ELSE 0
                END
             FROM los_stats ls WHERE ls.patient_id = p.patient_id ORDER BY ls.start_timestamp DESC LIMIT 1) AS is_los_outlier_last_visit,
            -- Quality Flags
            qm.is_in_denominator_65_plus,
            qm.is_hospice_excluded,
            -- Care Gap Flags
            CASE WHEN cg.last_colonoscopy_date IS NULL OR cg.last_colonoscopy_date < (CURRENT_DATE - INTERVAL '10 years') THEN 1 ELSE 0 END AS has_colonoscopy_gap,
            CASE WHEN p.gender = 'F' AND (cg.last_mammogram_date IS NULL OR cg.last_mammogram_date < (CURRENT_DATE - INTERVAL '2 years')) THEN 1 ELSE 0 END AS has_mammogram_gap
        FROM {{ target.schema }}.transformed_patients p
        LEFT JOIN readmission_risk rr ON p.patient_id = rr.patient_id
        LEFT JOIN quality_measures qm ON p.patient_id = qm.patient_id
        LEFT JOIN care_gaps cg ON p.patient_id = cg.patient_id;

    {% endset %}
    {{ log("Building clinical_analytics_mart table", info=True) }}
    {{ run_query(clinical_analytics_sql) }}

    {#
    --------------------------------------------------------------------------------
    FINAL STEP: CREATE STRUCTURED TABLES
    --------------------------------------------------------------------------------
    #}
    {% for table_prefix in ['patients', 'encounters', 'observations'] %}
        {% set clone_sql %}
            DROP TABLE IF EXISTS {{ target.schema }}.structured_{{ table_prefix }};
            CREATE TABLE {{ target.schema }}.structured_{{ table_prefix }} AS
            SELECT * FROM {{ target.schema }}.transformed_{{ table_prefix }};
        {% endset %}
        {{ log("Creating structured_" ~ table_prefix, info=True) }}
        {{ run_query(clone_sql) }}
    {% endfor %}

{% endmacro %}
