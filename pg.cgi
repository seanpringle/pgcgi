#!/bin/bash

environment=$(env | xxd -p)
input=$(cat | xxd -p)

psql -U postgres -A -t -q -f - <<EOD
  SET client_min_messages TO WARNING;
  INSERT INTO requests (environment, input)
  VALUES (
    convert_from(decode('${environment}', 'hex'), 'UTF8'),
    decode('${input}', 'hex')
  )
  RETURNING output;
EOD
