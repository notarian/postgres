-- DROP FUNCTION public.copy_table_data_upsert(regclass, regclass, _text, _text);

CREATE OR REPLACE FUNCTION public.copy_table_data_upsert(source_table regclass, target_table regclass, conflict_columns text[], update_columns text[] DEFAULT NULL::text[])
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    column_list text;
    update_list text;
    sql text;
BEGIN
    -- Build list of common columns
    SELECT string_agg(quote_ident(attname), ', ')
    INTO column_list
    FROM pg_attribute
    WHERE attrelid = source_table
      AND attnum > 0
      AND NOT attisdropped
      AND attname IN (
          SELECT attname
          FROM pg_attribute
          WHERE attrelid = target_table
            AND attnum > 0
            AND NOT attisdropped
      );

    -- Build update list (all non-conflict columns if not specified)
    IF update_columns IS NULL THEN
        SELECT string_agg(
            format('%1$s = EXCLUDED.%1$s', quote_ident(attname)), ', '
        )
        INTO update_list
        FROM pg_attribute
        WHERE attrelid = target_table
          AND attnum > 0
          AND NOT attisdropped
          AND attname <> ALL(conflict_columns);
    ELSE
        update_list := array_to_string(
            array(
                SELECT format('%1$s = EXCLUDED.%1$s', quote_ident(c))
                FROM unnest(update_columns) AS c
            ), ', '
        );
    END IF;

    -- Build dynamic SQL with ON CONFLICT DO UPDATE
    sql := format(
        'INSERT INTO %s (%s) SELECT %s FROM %s ' ||
        'ON CONFLICT (%s) DO UPDATE SET %s',
        target_table, column_list, column_list, source_table,
        array_to_string(ARRAY(SELECT quote_ident(c) FROM unnest(conflict_columns) AS c), ', '),
        update_list
    );

    -- Execute the upsert
    EXECUTE sql;
END;
$function$
;
