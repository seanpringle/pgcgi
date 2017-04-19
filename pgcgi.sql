-- Simple HTTP CGI routing using PostgreSQL as an application server.
-- This is only a framework, not the application.

-- The "requests" table is modelled on an Apache combined access log.
-- The CGI client inserts a record:
--
--   INSERT INTO requests (environment, input) VALUES (?, ?) RETURNING output;
--
-- "environment" = CGI environment variables serialized, NAME=value, one per line
-- "input" = HTTP payload
-- "output" = A HTTP response

-- Default route. Application should override.

create or replace function route()
  returns integer language plpgsql as $$
  begin
    insert into response values ('hello world');
    return 200;
  end
$$;

-- Request log. Notice INSERT handler below

drop table if exists requests cascade;

create table requests (
  id uuid default (md5(random()::text || clock_timestamp()::text)::uuid) primary key,
  stamp timestamp without time zone default (now() at time zone 'utc'),
  duration double precision,
  ip inet,
  method text,
  url text,
  referrer text,
  username text,
  status integer,
  size integer,
  agent text,
  environment text,
  input bytea,
  output text,
  error text,
  debug jsonb
);

create index rqi_stamp on requests (stamp);

-- Wrapper for route(). Notice exception handlers

create or replace function dispatch()
  returns void language plpgsql as $$

  declare

    headers text;
    content text;
    route_status integer;

    -- tcp: 'HTTP/1.0', cgi: 'Status:'
    status_header text;

    -- error reporting
    detail text;
    context text;
    errmsg text;

  begin

    -- The response body.
    -- Multiple records will be concatenated.
    create temporary table response (
      document text
    );

    -- The response headers.
    -- Connection may be overriden.
    -- Content-Type will default to text/plain.
    create temporary table response_headers (
      name text primary key,
      value text
    );

    -- Application replaces route()
    route_status := route();
    update request set status = route_status;

    -- Collapse multiple response parts together 
    content := coalesce(
      (select string_agg(document, '') from response),
      route_status::text
    );

    insert into response_headers (name, value) values ('Content-Type', 'text/plain')
      on conflict (name) do nothing;

    insert into response_headers (name, value) values ('Connection', 'close')
      on conflict (name) do update set value = excluded.value;

    -- HTTP headers
    headers := (
      select string_agg(line, chr(10)) from (
        select name || ': ' || value as line from response_headers
      ) t
    );

    -- Temporary table "request". See handler()
    update request set output = (
      with http_status as (
        select 200 as hs_code, 'OK'                    as hs_message union all
        select 301 as hs_code, 'Moved Permanently'     as hs_message union all
        select 302 as hs_code, 'Moved Temporarily'     as hs_message union all
        select 400 as hs_code, 'Bad Request'           as hs_message union all
        select 403 as hs_code, 'Permission Denied'     as hs_message union all
        select 404 as hs_code, 'Not Found'             as hs_message union all
        select 409 as hs_code, 'Conflict'              as hs_message union all
        select 500 as hs_code, 'Internal Server Error' as hs_message
      )
      select
        array_to_string(
          ARRAY[
            'Status: ' || route_status || ' ' || (select hs_message from http_status where hs_code = route_status),
            headers,
            '',
            content
          ],
          chr(10)
        )
    );

  exception

    -- 400 Bad request
    when data_exception then

      get stacked diagnostics
        detail  = PG_EXCEPTION_DETAIL,
        context = PG_EXCEPTION_CONTEXT;

      errmsg := concat(SQLSTATE, E'\n', SQLERRM, E'\n', detail, E'\n\n', context);

      update request set
        error = errmsg,
        output = array_to_string(
          ARRAY[
            'Status: 400 Bad Request',
            'Content-Type: text/html',
            'Connection: close',
            '',
            concat('<pre>', SQLERRM, '</pre>')
          ],
          chr(10)
        );

    -- 500 Internal Server Error
    when OTHERS then

      get stacked diagnostics
        detail  = PG_EXCEPTION_DETAIL,
        context = PG_EXCEPTION_CONTEXT;

      errmsg := concat(SQLSTATE, E'\n', SQLERRM, E'\n', detail, E'\n\n', context);

      update request set
        error = errmsg,
        output = array_to_string(
          ARRAY[
            'Status: 500 Internal Server Error',
            'Content-Type: text/html',
            'Connection: close',
            '',
            concat('<pre>', SQLERRM, '</pre>')
          ],
          chr(10)
        );

  end
$$;

-- Translate special or %XX encoded characters in a URL.

create or replace function url_decode(url text)
  returns text language sql as $$
    with
    string as (
      select replace(url, '+', '%20') as s
    ),
    chars as (
      select row_number() over () as cn, c from (
        select convert_from(decode(array_to_string(regexp_matches((select s from string), '[%]([a-zA-Z0-9]{2})', 'g'), ''), 'hex'), 'UTF8') as c
      ) t
    ),
    parts as (
      select row_number() over () as pn, p from
        regexp_split_to_table((select s from string), '[%][a-zA-Z0-9]{2}')
      as p
    )
    select string_agg(p || coalesce(c, ''), '') as url
      from parts left join chars on pn = cn;
$$;

-- Encode special characters for inclusion in a URL.

create or replace function url_encode(str text)
  returns text language sql as $$
    with
    map as (
      select '!' as c, '%21' as p union all
      select '*' as c, '%2A' as p union all
      select chr(39) as c, '%27' as p union all
      select '(' as c, '%28' as p union all
      select ')' as c, '%29' as p union all
      select ';' as c, '%3B' as p union all
      select ':' as c, '%3A' as p union all
      select '@' as c, '%40' as p union all
      select '&' as c, '%26' as p union all
      select '=' as c, '%3D' as p union all
      select '+' as c, '%2B' as p union all
      select '$' as c, '%24' as p union all
      select ',' as c, '%2C' as p union all
      select '/' as c, '%2F' as p union all
      select '?' as c, '%3F' as p union all
      select '#' as c, '%23' as p union all
      select '[' as c, '%5B' as p union all
      select ']' as c, '%5D' as p
    ),
    list as (
      select replace(substr(coalesce(str,''), i, 1), '%', '%25') as s
      from generate_series(1, length(coalesce(str,''))) i
    ),
    result as (
      select string_agg(coalesce(p, s), '') as res
      from list left join map on s = c
    )
    select replace(coalesce(res, ''), ' ', '+') from result
$$;

-- POST data can be character set challenged
create or replace function bytea_to_array(buffer bytea, pattern bytea)
  returns bytea[] language plpgsql immutable parallel safe as $$
  declare
    parts bytea[];
    pos integer;
    pat integer;
  begin
    pat := octet_length(pattern);
    pos := position(pattern in buffer);
    while pos > 0 loop
      parts  := array_append(parts, substring(buffer for pos-1));
      buffer := substring(buffer from pos + pat);
      pos    := position(pattern in buffer);
    end loop;
    parts := array_append(parts, buffer);
    return parts;
  end
$$;

create or replace function bytea_part(buffer bytea, pattern bytea, pos integer)
  returns bytea language sql immutable parallel safe as $$
    select (bytea_to_array(buffer, pattern))[pos];
$$;

-- Process a HTTP CGI request.
-- This is the INSERT trigger for "requests" table.

create or replace function handler()
  returns trigger language plpgsql as $$

  declare

    boundary bytea;
    chunks bytea[];
    chunk bytea;

  begin

    -- Decode environment variables <NAME>=<value>\n
    create temporary table request_environment as
      with lines as (
        select row_number() over () as ln, trim(line, E' \r\n') as line
        from (select unnest(string_to_array(new.environment, chr(10))) as line) t
      )
      select
        split_part(line, '=', 1) as name,
        substr(line, length(split_part(line, '=', 1))+2) as value
      from lines
      where ln > 1
        and line ~ '^[^=]+=.+$'
        and length(line) > 0
      ;

    -- CGI supplies request headers as HTTP_% environment variables
    create temporary table request_headers as
      select
        initcap(replace(regexp_replace(name, '^HTTP_', ''), '_', '-'))
          as name,
        value
      from
        request_environment
      where
        name like 'HTTP\_%'
    ;

    -- Extract HTTP method GET, POST etc
    new.method := coalesce(
      (select value from request_environment where name = 'REQUEST_METHOD'),
      'GET'
    );

    -- Extract and decode HTTP url
    new.url := coalesce(
      (select value from request_environment where name = 'REQUEST_URI'),
      '/'
    );

    -- Decode GET variables in the URL ?name=value[&...]
    create temporary table request_variables as
      with args as (
        select
          url_decode(split_part(pair, '=', 1)) as name,
          url_decode(split_part(pair, '=', 2)) as value
        from unnest(
          string_to_array(
            split_part(new.url, '?', 2), '&')
        ) as pair
      )
      select
        distinct(name) as name,
        (select value from args a2 where a1.name = a2.name limit 1) as value
      from args a1;

    create unique index
      on request_variables (name);

    create temporary table request_files (
      name text,
      file text,
      mime text,
      content bytea
    );

    -- Debug POST variables if relevant
    if new.method = 'POST' then

      case

        when (select true from request_environment where name = 'CONTENT_TYPE' and value like 'multipart/form-data;%') then

          boundary := bytea_part(new.input, E'\r\n', 1);
          chunks := bytea_to_array(new.input, boundary);

          foreach chunk in array chunks loop

            create temporary table postvars as
              with
                args as (
                  select
                    trim(split_part(pair, ':', 1)) as name,
                    trim(split_part(pair, ':', 2)) as value
                  from
                    unnest(
                      string_to_array(
                        convert_from(bytea_part(chunk, E'\r\n\r\n', 1), 'UTF8'), E'\r\n')
                    ) pair
                )
              select
                distinct(name) as name,
                (select value from args a2 where a1.name = a2.name limit 1) as value
              from args a1;

            if (select true from postvars where name = 'Content-Disposition' and value ~ 'name=".+?"' and value ~ 'filename=".+?"') then

              insert into request_files
                select
                  (regexp_matches(value, 'name="([^"]+)"'))[1],
                  (regexp_matches(value, 'filename="([^"]+)"'))[1],
                  coalesce((select value from postvars where name = 'Content-Type'), 'application/octet-stream'),
                  bytea_part(chunk, E'\r\n\r\n', 2)
                from
                  postvars
                where
                  name = 'Content-Disposition'
              ;

            end if;

            drop table postvars;

          end loop;

        when (select true from request_environment where name = 'CONTENT_TYPE' and value like 'application/x-www-form-urlencoded;%') then

          insert into request_variables
            with args as (
              select
                url_decode(split_part(pair, '=', 1)) as name,
                url_decode(split_part(pair, '=', 2)) as value
              from unnest(
                string_to_array(
                  trim(convert_from(new.input, 'UTF8'), E' \r\n'), '&')
              ) as pair
            )
            select
              distinct(name) as name,
              (select value from args a2 where a1.name = a2.name limit 1) as value
            from args a1
            on conflict (name) do nothing;

      end case;

    end if;

    new.status := 200;
    new.size   := 0;

    new.referrer := (select value from request_headers where name = 'Referer');
    
    new.agent := (select value from request_headers where name = 'User-Agent');
    
    new.username := coalesce(
      (select value from request_headers where name = 'X-Remote-User'),
      (select value from request_environment where name = 'REMOTE_USER')
    );

    new.ip := coalesce(
      (select value from request_headers where name = 'X-Forwarded-For'),
      (select value from request_environment where name = 'REMOTE_ADDR')
    );

    create temporary table request (
      like requests
    );

    alter table request
      add column url_path  text,
      add column url_parts text[],
      add column url_query text;

    insert into request (
      id,
      stamp,
      duration,
      ip,
      method,
      url,
      referrer,
      username,
      status,
      size,
      agent,
      environment,
      input,
      output,
      url_path,
      url_parts,
      url_query
    ) values (
      new.id,
      new.stamp,
      new.duration,
      new.ip,
      new.method,
      new.url,
      new.referrer,
      new.username,
      new.status,
      new.size,
      new.agent,
      new.environment,
      new.input,
      new.output,
      split_part(new.url, '?', 1),
      string_to_array(trim(both '/' from split_part(new.url, '?', 1)), '/'),
      split_part(new.url, '?', 2)
    );

    perform dispatch();

    new.status
      := (select status from request);
    new.output
      := (select output from request);
    new.error
      := (select error from request);

    new.size := octet_length(new.output);

--    new.debug :=
--         jsonb_build_object('request_headers',
--        coalesce((select jsonb_agg(request_headers) from request_headers), '{}'::jsonb))
--      || jsonb_build_object('request_variables',
--        coalesce((select jsonb_agg(request_variables) from request_variables), '{}'::jsonb))
--      || jsonb_build_object('request_environment',
--        coalesce((select jsonb_agg(request_environment) from request_environment), '{}'::jsonb))
--    ;

    new.duration := (
      select extract(epoch from clock_timestamp() at time zone 'utc' - new.stamp)
    );

    return new;
  end
$$;

create trigger requests before insert on requests
  for each row execute procedure handler();
