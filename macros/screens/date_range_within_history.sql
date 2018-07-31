---------- date_range_within_history SCREEN
---- Verifies that each date / timestamp in a column is within the history of RevZilla, if not null.
---- Date must be between 2007-11-00 and current date.

{%- macro date_range_within_history(screen_args, kwargs) -%}
---- Pass the screen_args object with these params:
---- screen_args:
----    - column is the timestamp or date field to screen on (date will be cast as timestamp by date_part)

    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_DATE_RANGE_WITHIN_HISTORY AS (
        SELECT
            {{universal_audit_property_set('date_range_within_history_{{kwargs.provided_value}}',screen_args,kwargs)}}

        AND
            (
                    (
                            date_part('year', {{screen_args.column}}) >= 2007
                        AND
                            date_part('month', {{screen_args.column}}) >= 11
                        AND
                            {{screen_args.column}} < current_timestamp
                    )
                OR
                    {{screen_args.column}} IS NULL
            )
    )
{%- endmacro -%}
