-- PostgREST syntax, then some

create or replace function route_rest()
  returns integer language plpgsql as $$

  declare

    rc int;
    row record;
    relation text;
    filters text;
    selecting text;
    select_sql text;
    ordering text;
    order_sql text;
    grouping text;
    group_sql text;
    pk_field text; -- no composites yet
    pk_value text;

  begin

    insert into response_headers
      values ('Content-Type', 'application/json');

    create temporary table request_filters as
      select * from request_variables where name not in ('select', 'group', 'order', 'limit', 'offset');

    relation := (
      select 
        url_parts[2]
      from
        request
      inner join
        information_schema.tables
          on url_parts[2] = table_name
          and current_schema = table_schema
    );

    pk_value  := (select url_parts[3] from request);
    selecting := (select value from request_variables where name = 'select');
    ordering  := (select value from request_variables where name = 'order');
    grouping  := (select value from request_variables where name = 'group');

    case

      when relation is not null and pk_value is not null then

        pk_field := (
          select
            column_name
          from
            information_schema.table_constraints tc
          inner join
            information_schema.key_column_usage kcu
              on tc.constraint_name = kcu.constraint_name
          where
            tc.table_name = relation
            and tc.constraint_schema = current_schema
        );

        execute format($f$
          insert into response
            select to_jsonb(%1$I) from %1$I where %2$I = %3$L 
          $f$, relation, pk_field, pk_value
        );

      when relation is not null then

        create temporary table request_columns as
          select name from request_filters
          union select csv from unnest_csv(selecting) csv
          union select csv from unnest_csv(grouping) csv where csv !~ '^[[:digit:]]+$'
          union select csv from unnest_csv(regexp_replace(ordering, '[.](.*)$', '')) csv
        ;

        -- Ensure all column references are valid
        create temporary table request_columns_malformed as
          select
            name
          from
            request_columns
          left join
            information_schema.columns
              on name = column_name
              and relation = table_name
              and current_schema = table_schema
          where
            column_name is null
        ;

        if rc is null and (select true from request_columns_malformed limit 1) then

          insert into response
            select
              to_jsonb(t)
            from (
              select
                'unknown columns'
                  as error,
                array_agg(name)
                  as columns
              from 
                request_columns_malformed
            ) t;

            rc = 400;

        end if;

        -- Check order_sql syntax
        create temporary table request_ordering_malformed as
          select
            csv
          from
            unnest_csv(ordering) csv,
            lateral (
              select
                string_to_array(csv, '.')
                  as parts
            ) a
          where
            parts[2] is not null
            and parts[2] not in ('asc', 'desc')
        ;

        if rc is null and (select true from request_ordering_malformed limit 1) then

          insert into response
            select
              to_jsonb(t)
            from (
              select
                'malformed order_sql'
                  as error,
                array_agg(concat('order=', csv))
                  as order
              from
                request_ordering_malformed 
            ) t;

            rc = 400;

        end if;

        order_sql := (
          select
            string_agg(concat(quote_ident(field), ' ', direction), ',')
          from
            unnest_csv(ordering) csv,
            lateral (
              select
                string_to_array(csv, '.')
                  as parts
            ) a,
            lateral (
              select
                parts[1]
                  as field,
                coalesce(
                  parts[2],
                  'asc'
                )
                  as direction
            ) b
        );

        -- Ensure clauses are valid
        create temporary table request_filters_malformed as
          select
            name, value
          from
            request_filters
          where
            value !~* '^(not[.])?(eq|neq|lt|gt|lte|gte|like|ilike|in)[.](.+)'
            and value !~* '^(not[.])?(is)[.](true|false|null)'
        ;

        if rc is null and (select true from request_filters_malformed limit 1) then

          insert into response
            select
              to_jsonb(t)
            from (
              select
                'malformed clauses'
                  as error,
                array_agg(concat(name,'=',value))
                  as clauses
              from
                request_filters_malformed
            ) t;

          rc = 400;

        end if;

        if rc is null then

          -- Simple WHERE
          filters := coalesce(
            (
              select
                string_agg(
                  format($f$ %1$s (%2$I %3$s %4$s) $f$,
                    case when value ~ '^not[.]' then 'not' else '' end,
                    name,
                    case parts[1]
                      when 'eq'  then '='  
                      when 'neq' then '<>' 
                      when 'lt'  then '<'  
                      when 'gt'  then '>'  
                      when 'lte' then '<=' 
                      when 'gte' then '>=' 
                    end,
                    quote_literal(parts[2])
                  ),
                  'and'
                )
              from
                request_filters,
                lateral (
                  select
                    regexp_matches(value, '^(?:not[.])?(eq|neq|lt|gt|lte|gte)[.](.+)')
                      as parts
                ) a
            ),
            '1=1'
          );

          -- LIKE/ILIKE
          filters := filters || ' and ' || coalesce(
            (
              select
                string_agg(
                  format($f$ %1$s (%2$I %3$s %4$s) $f$,
                    case when value ~ '^not[.]' then 'not' else '' end,
                    name,
                    case parts[1]
                      when 'like'  then '~~' 
                      when 'ilike' then '~~*'
                    end,
                    quote_literal(replace(parts[2], '*', '%'))
                  ),
                  'and'
                )
              from
                request_filters,
                lateral (
                  select
                    regexp_matches(value, '^(?:not[.])?(like|ilike)[.](.+)')
                      as parts
                ) a
            ),
            '1=1'
          );

          -- IS true|false|null
          filters := filters || ' and ' || coalesce(
            (
              select
                string_agg(
                  format($f$ %1$s (%2$I is %4$s) $f$,
                    case when value ~ '^not[.]' then 'not' else '' end,
                    name,
                    lower(parts[2])
                  ),
                  'and'
                )
              from
                request_filters,
                lateral (
                  select
                    regexp_matches(lower(value), '^(?:not[.])?(is)[.](true|false|null)$')
                      as parts
                ) a
            ),
            '1=1'
          );

          -- IN (...)
          filters := filters || ' and ' || coalesce(
            (
              select
                string_agg(
                  format($f$ %1$s (%2$I in (%3$s)) $f$,
                    case when value ~ '^not[.]' then 'not' else '' end,
                    name,
                    csv
                  ),
                  'and'
                )
              from
                request_filters,
                lateral (
                  select
                    regexp_matches(value, '^(?:not[.])?(in)[.](.+)$')
                      as parts
                ) a,
                lateral (
                  select
                    quote_literal_csv(parts[2])
                      as csv
                ) b
            ),
            '1=1'
          );

          select_sql := coalesce(quote_ident_csv(selecting), '*');
          group_sql  := quote_ident_csv(grouping);

          execute format($f$
            insert into response values
              (coalesce((select jsonb_agg(t) from (select %3$s from %1$I where %2$s %4$s %5$s ) t), '[]'))
            $f$, relation, filters, select_sql, 
              case when group_sql is not null then concat('group by ', group_sql) else '' end,
              case when order_sql is not null then concat('order by ', order_sql) else '' end
          );

          rc = 200;

        end if;

      when relation is null then

        insert into response values (
          coalesce(
            (
              select
                jsonb_agg(t)
              from (
                select
                  table_schema
                    as schema,
                  table_name
                    as name
                from
                  information_schema.tables
                where
                  table_schema !~ '^(pg_|information_schema)'
              ) t
            ),
            '[]'
          )
        );
        
        rc = 200;

      else

        insert into response
          select
            to_jsonb(t)
          from (
            select
              'unknown relation'
                as error,
              url_parts[2]
                as relation
            from
              request
          ) t;

        rc = 400;

    end case;

    return rc;
  end
$$;
