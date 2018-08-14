---------- USERS_STAGING_QUALITY TABLE
----
---- Staging Quality tables are cleaned and transform-ready source tables.
---- The data in Staging Quality tables are completely untransformed source data, with one exception: they
---- have additional attributes AUDIT_KEY, ROW_QUALITY_SCORE and AUDIT_QUALITY_SCORE.
----
---- AUDIT_KEY is the FKey to the audit that added (or last updated) the subject row.
----
---- ROW_QUALITY_SCORE represents the quality performance of the subject row. Options are:
----    - Passed: row is considered quality data
----    - Flagged: row has failed one or more quality screens, and should be considered suspect
----
---- AUDIT_QUALITY_SCORE represents the quality perfomance of the row in the context of the audit. Options are:
----    - passed: row is considered quality data in the context of the audit
----    - flagged: row is suspect in the context of the audit
----


---------- STATEMENTS [leave this section alone!]
---- Statements populate the python context with information about the subject audit.
{% if adapter.already_exists(this.schema, this.name) %}
    {%- call statement('target_audit', fetch_result=True) -%}
        SELECT
            audit_key,
            cdc_target,
            lowest_cdc,
            highest_cdc,
            target.data_type AS cdc_data_type,
            record_identifier.data_type AS record_identifier_data_type
        FROM
            {{this.database}}.{{this.schema | replace('STAGING_QUALITY','QUALITY')}}.audit

        JOIN
            "RAW".information_schema.columns target
        ON
            target.table_schema = 'ERP'
        AND
            target.table_name = entity_key
        AND
            target.column_name = cdc_target
        JOIN
            (SELECT
                data_type
            FROM
                "RAW".information_schema.columns
            WHERE
                table_schema = 'ERP'
            AND
                table_name = 'DW_USERS_VIEW'
            AND
                column_name = UPPER('id')
            LIMIT 1
            ) record_identifier
        ON
            1=1

        WHERE
            audit_status = 'Completed'
        AND
            database_key = 'RAW'
        AND
            schema_key = 'ERP'
        AND
            entity_key = 'DW_USERS_VIEW'
        AND
            audit_key NOT IN (SELECT
                                DISTINCT audit_key
                              FROM
                                {{this}})
        ORDER BY audit_key DESC
        LIMIT 1

    {%- endcall -%}

    {% set audit_response = load_result('target_audit')['data']%}
{% else %}
    {% set audit_response= [] %}
{% endif %}
---------- END STATMENTS

---- if there is no new data, skip the entire staging quality incremental build
{% if audit_response[0] | length > 0 %}
    {% set audit_data = {
                            'audit_key' :  audit_response[0][0],
                            'cdc_target' : audit_response[0][1],
                            'lowest_cdc' : audit_response[0][2],
                            'highest_cdc' : audit_response[0][3],
                            'cdc_data_type' : audit_response[0][4],
                            'record_identifier_data_type' : audit_response[0][5]} -%}

    WITH
    audit_source_records AS (

        SELECT
            *,
            {{audit_data['audit_key']}} AS audit_key
        FROM
           RAW.ERP.DW_USERS_VIEW
        WHERE
            {{audit_data['cdc_target']}}
        BETWEEN

        {% if audit_data['cdc_data_type'] in ('TEXT','TIMESTAMP_NTZ') %}
            '{{audit_data["lowest_cdc"]}}' AND '{{audit_data["highest_cdc"]}}'
        {% else %}
            {{audit_data['lowest_cdc']}} AND {{audit_data['highest_cdc']}}
        {% endif %}
    ),

    error_events AS (
        SELECT
            error_event_action,

            -- for audit-level error events this will be NULL so we will remove them later
            -- and use the presence of NULL values to flag an audit-level event
            TRY_CAST(record_identifier AS {{audit_data['record_identifier_data_type']}}) AS id
        FROM
            {{this.database}}.{{this.schema | replace('STAGING_QUALITY','QUALITY')}}.error_event_fact
        WHERE
            audit_key = {{audit_data['audit_key']}}
    )




---- remove rejected rows, flag flagged rows, and add audit-level flag
    SELECT
        audit_source_records.*,

        CASE
            WHEN error_event_action IS NULL THEN 'Passed'
            ELSE error_event_action
        END AS row_quality_score,

        (SELECT
            CASE
                WHEN COUNT(*) > 0 THEN 'Flagged'
                ELSE 'Passed'
            END
        FROM
            error_events
        WHERE
            id IS NULL) AS audit_quality_score
    FROM
        audit_source_records
    LEFT JOIN
        error_events
    ON
        audit_source_records.id = error_events.id

    WHERE
        row_quality_score <> 'Reject'

    -- guard against duplicate audit inserts
    {% if adapter.already_exists(this.schema, this.name) %}
        AND
            audit_key NOT IN (SELECT
                                DISTINCT audit_key
                              FROM
                                {{this}})
    {% endif %}

---- when no new data is present, return an empty table
{% elif adapter.already_exists(this.schema, this.name) %}
        SELECT
            *
        FROM
            {{this}}
        WHERE 1=0

---- when no data is present and the table does not exist,
---- create a new table outline
{% else %}
        SELECT
            *,
            0::integer AS audit_key,
            ''::varchar AS row_quality_score,
            ''::varchar AS audit_quality_score
        FROM
            RAW.ERP.DW_USERS_VIEW
        WHERE
            0=1
{% endif %}



{#
---------- DEPENDENCY HACK
---- {{ref('AUDIT_FACT')}}
#}



---------- CONFIGURATION [leave this section alone!]
{{config({

    "materialized":"incremental",
    "sql_where":"TRUE",
    "schema":"STAGING_QUALITY",
    "post-hook": " CREATE TEMPORARY TABLE {{this.name}}_to_remove AS (
                    SELECT
                        FIRST_VALUE(audit_key) OVER (PARTITION BY id ORDER BY audit_key ASC)::varchar||'-'||id::varchar as remove_flag
                        FROM {{this}}
                    WHERE
                        id IN ( SELECT
                                    id
                                FROM
                                    (SELECT id,
                                            count(*) countstar
                                    FROM {{this}}
                                    GROUP BY 1
                                    HAVING countstar > 1)));

                    DELETE FROM {{this}}
                    WHERE
                        audit_key::varchar||'-'||id::varchar
                    IN (SELECT remove_flag FROM {{this.name}}_to_remove);
                    DROP TABLE {{this.name}}_to_remove;"

})}}
