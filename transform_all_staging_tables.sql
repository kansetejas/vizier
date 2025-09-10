{% macro transform_all_staging_tables() %}

    {# --------------- existing staging -> transformed loop (unchanged) --------------- #}
    {% set results = run_query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'staging_%'"
    ) %}

    {% if execute %}
        {% set staging_tables = results.columns[0].values() %}
    {% else %}
        {% set staging_tables = [] %}
    {% endif %}

    {% for staging_table in staging_tables %}
        {% set transformed_table = staging_table | replace("staging_", "transformed_") %}

        {{ log("Transforming " ~ staging_table ~ " -> " ~ transformed_table, info=True) }}

        {% set cols_query %}
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = '{{ staging_table }}'
        {% endset %}

        {% set cols = run_query(cols_query) %}
        {% if execute %}
            {% set cols = cols.rows %}
        {% else %}
            {% set cols = [] %}
        {% endif %}

        {% set col_transforms = [] %}

        {% for col in cols %}
            {% set colname = col[0] %}
            {% set dtype = col[1] | lower %}

            {% if dtype in ["character varying","text"] %}
                {% set transform = "nullif(trim(" ~ colname ~ "), '')::varchar as " ~ colname %}
            {% elif dtype == "boolean" %}
                {% set transform = "
                    case
                        when lower(" ~ colname ~ "::text) in ('true','t','1','yes','y') then true
                        when lower(" ~ colname ~ "::text) in ('false','f','0','no','n') then false
                        else null
                    end::boolean as " ~ colname
                %}
            {% elif dtype in ["integer","smallint","bigint","numeric","double precision"] %}
                {% set transform = "
                    nullif(regexp_replace(" ~ colname ~ "::text, ',', '', 'g'), '')::" ~ dtype ~ " as " ~ colname
                %}
            {% elif dtype in ["date"] %}
                {% set transform = "
                    case
                        when " ~ colname ~ " is null or trim(" ~ colname ~ "::text) = ''
                        then null
                        else " ~ colname ~ "::date
                    end as " ~ colname
                %}
            {% elif dtype in ["timestamp without time zone","timestamp with time zone"] %}
                {% set transform = "
                    case
                        when " ~ colname ~ " is null or trim(" ~ colname ~ "::text) = ''
                        then null
                        else to_timestamp(" ~ colname ~ "::text, 'YYYY-MM-DD HH24:MI:SS')
                    end::timestamp as " ~ colname
                %}
            {% else %}
                {% set transform = colname %}
            {% endif %}

            {% do col_transforms.append(transform) %}
        {% endfor %}

        {% set sql %}
            drop table if exists {{ target.schema }}.{{ transformed_table }};
            create table {{ target.schema }}.{{ transformed_table }} as
            select distinct
                {{ col_transforms | join(',\n                ') }}
            from {{ target.schema }}.{{ staging_table }};
        {% endset %}

        {{ run_query(sql) }}
    {% endfor %}


    {# --------------- clinical business logic (uses transformed_* lowercased column names) --------------- #}

    {{ log("Building clinical business logic tables (readmission, LOS, quality, care-gaps)", info=True) }}


    --------------------------------------------------------------------------------
    -- 1) Readmission risk (uses transformed_encounters: patient, reasoncode, reasondescription)
    -- NOTES: Replace the lists below with the actual codes in your environment.
    --------------------------------------------------------------------------------
    {% set readmission_sql %}
        drop table if exists {{ target.schema }}.clinical_readmission_risk;
        create table {{ target.schema }}.clinical_readmission_risk as

        with enc as (
            select
                patient,
                coalesce(nullif(reasoncode::text,''), '') as reasoncode_text,
                coalesce(lower(nullif(reasondescription::text,'')), '') as reasondescription
            from {{ target.schema }}.transformed_encounters
        ),

        -- You MUST tailor these lists to your code system (ICD/SNOMED). Using both code + description gives resilience.
        code_lists as (
            select
                patient,
                case when reasoncode_text in ('I10','E11') then 1
                     when reasoncode_text in ('185347001','162673000') then 1 -- example SNOMED numeric codes (placeholder)
                     when reasondescription like '%hypertensi%' then 1
                     when reasondescription like '%diabet%' then 1
                     else 0 end as chronic_flag,
                case when reasoncode_text in ('J18','A41') then 1
                     when reasondescription like '%pneumoni%' then 1
                     when reasondescription like '%sepsis%' then 1
                     else 0 end as acute_flag
            from enc
        ),

        agg as (
            select
                patient,
                sum(chronic_flag) as chronic_count,
                sum(acute_flag) as acute_count
            from code_lists
            group by patient
        )

        select
            patient as patient_id,
            chronic_count,
            acute_count,
            (chronic_count * 2 + acute_count * 3) as readmission_risk_score
        from agg;
    {% endset %}
    {{ run_query(readmission_sql) }}


    --------------------------------------------------------------------------------
    -- 2) Length-of-stay variance and outlier flag (transformed_encounters: start, stop, code, patient)
    --------------------------------------------------------------------------------
    {% set los_sql %}
        drop table if exists {{ target.schema }}.clinical_los_variance;
        create table {{ target.schema }}.clinical_los_variance as

        with enc as (
            select
                patient,
                code as procedure_code,
                -- parse timestamps (start/stop were cleaned earlier to timestamp if possible)
                (stop::timestamp - start::timestamp) as los_interval
            from {{ target.schema }}.transformed_encounters
            where start is not null and stop is not null
        ),

        stats as (
            select
                procedure_code,
                avg(extract(epoch from los_interval)/86400.0) as avg_los_days,
                stddev(extract(epoch from los_interval)/86400.0) as stddev_los_days
            from enc
            group by procedure_code
        ),

        with_los as (
            select
                e.patient,
                e.procedure_code,
                (extract(epoch from e.los_interval)/86400.0) as los_days
            from enc e
        )

        select
            w.patient as patient_id,
            w.procedure_code,
            w.los_days,
            s.avg_los_days,
            s.stddev_los_days,
            case
                when s.stddev_los_days is null then 0
                when abs(w.los_days - s.avg_los_days) > 2 * s.stddev_los_days then 1
                else 0
            end as los_outlier_flag
        from with_los w
        left join stats s
            on w.procedure_code = s.procedure_code;
    {% endset %}
    {{ run_query(los_sql) }}


    --------------------------------------------------------------------------------
    -- 3) Quality measure denominators & exclusions (transformed_patients: id, birthdate, deathdate;
    --    transformed_observations: patient_id, code)
    -- NOTE: birthdate sample is 'DD-MM-YYYY' so parse accordingly; adjust if your data differs.
    --------------------------------------------------------------------------------
    {% set quality_sql %}
        drop table if exists {{ target.schema }}.clinical_quality_measures;
        create table {{ target.schema }}.clinical_quality_measures as

        with p as (
            select
                id as patient_id,
                -- handle a few common formats; tweak if needed
                case
                    when birthdate ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then to_date(birthdate, 'DD-MM-YYYY')
                    when birthdate ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then birthdate::date
                    else null
                end as birth_date,
                case when coalesce(nullif(deathdate,''), '') = '' then null else
                    (case
                        when deathdate ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then to_date(deathdate, 'DD-MM-YYYY')
                        when deathdate ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then deathdate::date
                        else null
                    end)
                end as death_date
            from {{ target.schema }}.transformed_patients
        ),

        age_calc as (
            select
                patient_id,
                birth_date,
                death_date,
                case when birth_date is null then null else extract(year from age(current_date, birth_date)) end as age_years,
                case when death_date is not null then 1 else 0 end as is_deceased
            from p
        ),

        -- exclusions: example codes for hospice/palliative; adjust to your code system
        exclusions as (
            select
                patient,
                1 as hospice_exclusion
            from {{ target.schema }}.transformed_observations
            where code in ('Z66','Z51.5') or lower(description::text) like '%hospice%'
            group by patient
        )

        select
            a.patient_id,
            a.age_years,
            case when a.age_years >= 65 and a.is_deceased = 0 then 1 else 0 end as in_denominator,
            coalesce(e.hospice_exclusion, 0) as excluded_flag
        from age_calc a
        left join exclusions e on a.patient_id = e.patient;
    {% endset %}
    {{ run_query(quality_sql) }}


    --------------------------------------------------------------------------------
    -- 4) Care gap identification (transformed_observations: patient_id, code, date)
    --    Example: colonoscopy (placeholder code '73761001'), mammogram (placeholder '24606-6')
    --    Replace screening_code lists with the actual codes used in your Synthea export.
    --------------------------------------------------------------------------------
    {% set care_sql %}
        drop table if exists {{ target.schema }}.clinical_care_gaps;
        create table {{ target.schema }}.clinical_care_gaps as

        with o as (
            select
                patient_id,
                code,
                case
                    when date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' then (date::timestamp)::date
                    when date ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}' then to_date(date, 'DD-MM-YYYY')
                    else null
                end as obs_date,
                description
            from {{ target.schema }}.transformed_observations
        ),

        screening as (
            select
                patient_id,
                max(case when code in ('73761001','46680005') or lower(description) like '%colonoscopy%' then obs_date end) as last_colonoscopy,
                max(case when code in ('24606-6','39156-5') or lower(description) like '%mammogram%' then obs_date end) as last_mammogram
            from o
            group by patient_id
        )

        select
            patient_id,
            case when last_colonoscopy is null or last_colonoscopy < current_date - interval '10 years' then 1 else 0 end as colonoscopy_gap_flag,
            case when last_mammogram is null or last_mammogram < current_date - interval '2 years' then 1 else 0 end as mammogram_gap_flag,
            last_colonoscopy,
            last_mammogram
        from screening;
    {% endset %}
    {{ run_query(care_sql) }}


{% endmacro %}
