import logging

logger = logging.getLogger(__name__)

class PostgresInformation():
    def __init__(self, db):
        self.db = db

    def columns(self):
        query = """
SELECT n.nspname AS schema,
       c.relname AS table,
       pg_catalog.pg_table_size(c.oid) AS tablesize,
       a.attname AS name,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
   FROM pg_catalog.pg_attrdef d
   WHERE d.adrelid = a.attrelid
     AND d.adnum = a.attnum
     AND a.atthasdef) AS default_value,
       a.attnotnull AS not_null,
       a.attnum AS position
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
WHERE c.relkind = 'r'
  AND c.relpersistence <> 't'
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY n.nspname,
         c.relname,
         a.attnum;
"""
        #FIXME: toast handling, table inheritance

        result = self.db.run_query(query)
        return result

    def indexes(self):
        """ Fetch information about indexes

        """
        query = """
SELECT n.nspname AS schema,
       c.relname AS table,
       i.indkey::text AS columns,
       c2.relname AS name,
       pg_relation_size(c2.oid) AS size_bytes,
       i.indisprimary AS is_primary,
       i.indisunique AS is_unique,
       i.indisvalid AS is_valid,
       pg_catalog.pg_get_indexdef(i.indexrelid, 0, TRUE) AS index_def,
       pg_catalog.pg_get_constraintdef(con.oid, TRUE) AS constraint_def
FROM pg_catalog.pg_class c,
     pg_catalog.pg_class c2,
     pg_catalog.pg_namespace n,
     pg_catalog.pg_index i
LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid
                                           AND conindid = i.indexrelid
                                           AND contype IN ('p', 'u', 'x'))
WHERE c.relkind = 'r'
  AND c.relpersistence <> 't'
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND c.oid = i.indrelid
  AND i.indexrelid = c2.oid
  AND n.oid = c.relnamespace
ORDER BY n.nspname,
         c.relname,
         i.indisprimary DESC,
         i.indisunique DESC,
         c2.relname;
"""
        #FIXME: column references for index expressions

        result = self.db.run_query(query)
        for row in result:
            # We need to convert the Postgres legacy int2vector to an int[]
            row['columns'] = map(int, str(row['columns']).split())
        return result

    def constraints(self):
        """


        :return:
        """
        query = """
SELECT n.nspname AS schema,
       c.relname AS table,
       conname AS name,
       pg_catalog.pg_get_constraintdef(r.oid, TRUE) AS constraint_def,
       r.conkey AS columns,
       n2.nspname AS foreign_schema,
       c2.relname AS foreign_table,
       r.confkey AS foreign_columns
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_constraint r ON r.conrelid = c.oid
LEFT JOIN pg_catalog.pg_class c2 ON r.confrelid = c2.oid
LEFT JOIN pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace
WHERE r.contype = 'f'
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
ORDER BY n.nspname,
         c.relname,
         name;
"""
        #FIXME: This probably misses check constraints and others?
        return self.db.run_query(query)

    def triggers(self):

        #FIXME: Needs to be implemented
        query = """
SELECT t.tgname, pg_catalog.pg_get_triggerdef(t.oid, true), t.tgenabled
        FROM pg_catalog.pg_trigger t
        WHERE t.tgrelid = '16795' AND NOT t.tgisinternal
        ORDER BY 1
"""

    def version(self):
        return self.db.run_query("SELECT VERSION()")[0]['version']

    def table_stats(self):
        query = "SELECT * FROM pg_stat_user_tables s JOIN pg_statio_user_tables sio ON s.relid = sio.relid"
        result = self.db.run_query(query)

        for row in result:
            del row['relid']
            row['table'] = row.pop('relname')
            row['schema'] = row.pop('schemaname')

        return result

    def index_stats(self):
        query = "SELECT * FROM pg_stat_user_indexes s JOIN pg_statio_user_indexes sio ON s.indexrelid = sio.indexrelid"
        result = self.db.run_query(query)

        for row in result:
            del row['relid']
            del row['indexrelid']
            row['table'] = row.pop('relname')
            row['schema'] = row.pop('schemaname')
            row['index'] = row.pop('indexrelname')

        return result

    def bloat(self):
        """Fetch table & index bloat from database

This query has been lifted from check_postgres by Greg Sabino Mullane,
code can be found at https://github.com/bucardo/check_postgres

        """

        query = '''
SELECT
  current_database() AS db, schemaname, tablename, reltuples::bigint AS tups, relpages::bigint AS pages, otta,
  ROUND(CASE WHEN otta=0 OR sml.relpages=0 OR sml.relpages=otta THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
  CASE WHEN relpages < otta THEN 0 ELSE relpages::bigint - otta END AS wastedpages,
  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END AS wastedbytes,
  CASE WHEN relpages < otta THEN '0 bytes'::text ELSE (bs*(relpages-otta))::bigint || ' bytes' END AS wastedsize,
  iname, ituples::bigint AS itups, ipages::bigint AS ipages, iotta,
  ROUND(CASE WHEN iotta=0 OR ipages=0 OR ipages=iotta THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
  CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
  CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
  CASE WHEN ipages < iotta THEN '0 bytes' ELSE (bs*(ipages-iotta))::bigint || ' bytes' END AS wastedisize,
  CASE WHEN relpages < otta THEN
    CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta::bigint) END
    ELSE CASE WHEN ipages < iotta THEN bs*(relpages-otta::bigint)
      ELSE bs*(relpages-otta::bigint + ipages-iotta::bigint) END
  END AS totalwastedbytes
FROM (
  SELECT
    nn.nspname AS schemaname,
    cc.relname AS tablename,
    COALESCE(cc.reltuples,0) AS reltuples,
    COALESCE(cc.relpages,0) AS relpages,
    COALESCE(bs,0) AS bs,
    COALESCE(CEIL((cc.reltuples*((datahdr+ma-
      (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)),0) AS otta,
    COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
  FROM
     pg_class cc
  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname <> 'information_schema'
  LEFT JOIN
  (
    SELECT
      ma,bs,foo.nspname,foo.relname,
      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
      (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
      SELECT
        ns.nspname, tbl.relname, hdr, ma, bs,
        SUM((1-coalesce(null_frac,0))*coalesce(avg_width, 2048)) AS datawidth,
        MAX(coalesce(null_frac,0)) AS maxfracsum,
        hdr+(
          SELECT 1+count(*)/8
          FROM pg_stats s2
          WHERE null_frac<>0 AND s2.schemaname = ns.nspname AND s2.tablename = tbl.relname
        ) AS nullhdr
      FROM pg_attribute att
      JOIN pg_class tbl ON att.attrelid = tbl.oid
      JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
      LEFT JOIN pg_stats s ON s.schemaname=ns.nspname
      AND s.tablename = tbl.relname
      AND s.inherited=false
      AND s.attname=att.attname,
      (
        SELECT
          (SELECT current_setting('block_size')::numeric) AS bs,
            CASE WHEN SUBSTRING(SPLIT_PART(v, ' ', 2) FROM '#"[0-9]+.[0-9]+#"%' for '#')
              IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
          CASE WHEN v ~ 'mingw32' OR v ~ '64-bit' THEN 8 ELSE 4 END AS ma
        FROM (SELECT version() AS v) AS foo
      ) AS constants
      WHERE att.attnum > 0 AND tbl.relkind='r'
      GROUP BY 1,2,3,4,5
    ) AS foo
  ) AS rs
  ON cc.relname = rs.relname AND nn.nspname = rs.nspname
  LEFT JOIN pg_index i ON indrelid = cc.oid
  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
) AS sml
'''

        result = self.db.run_query(query)
        return result

    def bgwriter_stats(self):
        query = "SELECT * FROM pg_stat_bgwriter"
        return self.db.run_query(query)

    def db_stats(self):
        query = "SELECT * FROM pg_stat_database WHERE datname = current_database()"
        return self.db.run_query(query)

    def settings(self):
        query = "SELECT name, setting, unit, boot_val, reset_val, source, sourcefile, sourceline FROM pg_settings"
        result = self.db.run_query(query)

        for row in result:
            row['current_value'] = row.pop('setting')
            row['boot_value'] = row.pop('boot_val')
            row['reset_value'] = row.pop('reset_val')

        return result

    def locks(self):
        query = """
SELECT d.datname AS database,
       n.nspname AS schema,
       c.relname AS relation,
       l.locktype,
       l.page,
       l.tuple,
       l.virtualxid,
       l.transactionid::text,
       l.virtualtransaction,
       l.pid,
       l.mode,
       l.granted
FROM pg_locks l
LEFT JOIN pg_catalog.pg_class c ON l.relation = c.oid
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_database d ON d.oid = l.database
WHERE l.pid <> pg_backend_pid();
"""

        return self.db.run_query(query)
