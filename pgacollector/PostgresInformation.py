import logging

logger = logging.getLogger(__name__)

class PostgresInformation():
    def __init__(self, db):
        self.db = db

    def relations(self, with_views):
        query = """
        SELECT c.oid,
               n.nspname AS schema_name,
               c.relname AS table_name,
               pg_catalog.pg_table_size(c.oid) AS size_bytes,
               c.relkind AS relation_type,
               s.*,
               sio.*
          FROM pg_catalog.pg_class c
          LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
          LEFT JOIN pg_catalog.pg_stat_user_tables s ON (s.relid = c.oid)
          LEFT JOIN pg_catalog.pg_statio_user_tables sio ON (sio.relid = c.oid)
         WHERE c.relkind IN (%s)
               AND c.relpersistence <> 't'
               AND c.relname NOT IN ('pg_stat_statements')
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        """ % ("'r','v','m'" if with_views else "'r'")
        result = self.db.run_query(query)
        return result

    def columns(self, with_views):
        query = """
        SELECT c.oid,
               a.attname AS name,
               pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
               pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_value,
               a.attnotnull AS not_null,
               a.attnum AS position
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
        LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
        WHERE c.relkind IN (%s)
              AND c.relpersistence <> 't'
              AND c.relname NOT IN ('pg_stat_statements')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND a.attnum > 0
              AND NOT a.attisdropped
        """ % ("'r','v','m'" if with_views else "'r'")

        result = self.db.run_query(query)
        return result

    def indexes(self, with_views):
        query = """
        SELECT c.oid,
               c2.oid AS index_oid,
               i.indkey::text AS columns,
               c2.relname AS name,
               pg_catalog.pg_relation_size(c2.oid) AS size_bytes,
               i.indisprimary AS is_primary,
               i.indisunique AS is_unique,
               i.indisvalid AS is_valid,
               pg_catalog.pg_get_indexdef(i.indexrelid, 0, TRUE) AS index_def,
               pg_catalog.pg_get_constraintdef(con.oid, TRUE) AS constraint_def,
               s.idx_scan, s.idx_tup_read, s.idx_tup_fetch,
               sio.idx_blks_read, sio.idx_blks_hit
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
          JOIN pg_catalog.pg_index i ON (c.oid = i.indrelid)
          JOIN pg_catalog.pg_class c2 ON (i.indexrelid = c2.oid)
          LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid
                                                     AND conindid = i.indexrelid
                                                     AND contype IN ('p', 'u', 'x'))
          LEFT JOIN pg_stat_user_indexes s ON (s.indexrelid = c2.oid)
          LEFT JOIN pg_statio_user_indexes sio ON (sio.indexrelid = c2.oid)
         WHERE c.relkind IN (%s)
               AND c.relpersistence <> 't'
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        """ % ("'r','v','m'" if with_views else "'r'")
        #FIXME: column references for index expressions

        result = self.db.run_query(query)
        for row in result:
            # We need to convert the Postgres legacy int2vector to an int[]
            row['columns'] = map(int, str(row['columns']).split())
        return result

    def constraints(self):
        query = """
        SELECT c.oid,
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
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        """
        #FIXME: This probably misses check constraints and others?
        return self.db.run_query(query)

    def view_definitions(self):
        query = """
        SELECT c.oid,
               pg_catalog.pg_get_viewdef(c.oid) AS view_definition
          FROM pg_catalog.pg_class c
          LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('v','m')
               AND c.relpersistence <> 't'
               AND c.relname NOT IN ('pg_stat_statements')
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        """
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
        return self.db.run_query("SELECT version()")[0]['version']

    def table_bloat(self):
        # Based on https://github.com/pgexperts/pgx_scripts/blob/master/administration/table_bloat_check.sql
        # Original snippet is Copyright (c) 2014, PostgreSQL Experts, Inc.
        query = """
        WITH constants AS (
          SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 8 AS ma
        ),
        no_stats AS (
          SELECT table_schema, table_name
           FROM information_schema.columns
           LEFT OUTER JOIN pg_stats ON table_schema = schemaname
                                       AND table_name = tablename
                                       AND column_name = attname
          WHERE attname IS NULL
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
          GROUP BY table_schema, table_name
        ),
        null_headers AS (
          SELECT hdr+1+(sum(case when null_frac <> 0 THEN 1 else 0 END)/8) as nullhdr,
                 SUM((1-null_frac)*avg_width) as datawidth,
                 MAX(null_frac) as maxfracsum,
                 schemaname,
                 tablename,
                 hdr, ma, bs
            FROM pg_stats CROSS JOIN constants
            LEFT OUTER JOIN no_stats ON schemaname = no_stats.table_schema
                                        AND tablename = no_stats.table_name
           WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                 AND no_stats.table_name IS NULL
                 AND EXISTS (SELECT 1
                               FROM information_schema.columns
                              WHERE schemaname = columns.table_schema
                                    AND tablename = columns.table_name)
           GROUP BY schemaname, tablename, hdr, ma, bs
        ),
        data_headers AS (
          SELECT ma, bs, hdr, schemaname, tablename,
                 (datawidth+(hdr+ma-(case when hdr % ma=0 THEN ma ELSE hdr % ma END)))::numeric AS datahdr,
                 (maxfracsum*(nullhdr+ma-(case when nullhdr % ma=0 THEN ma ELSE nullhdr % ma END))) AS nullhdr2
            FROM null_headers
        ),
        table_estimates AS (
          SELECT pg_class.oid,
                 relpages * bs as table_bytes,
                 CEIL((reltuples*
                      (datahdr + nullhdr2 + 4 + ma -
                        (CASE WHEN datahdr % ma=0
                          THEN ma ELSE datahdr % ma END)
                        )/(bs-20))) * bs AS expected_bytes
            FROM data_headers
            JOIN pg_class ON tablename = relname
            JOIN pg_namespace ON relnamespace = pg_namespace.oid
                                 AND schemaname = nspname
           WHERE pg_class.relkind = 'r'
        )
        SELECT oid,
          CASE WHEN table_bytes > 0
          THEN table_bytes::NUMERIC
          ELSE NULL::NUMERIC END
          AS table_bytes,
          CASE WHEN expected_bytes > 0
          THEN expected_bytes::NUMERIC
          ELSE NULL::NUMERIC END
          AS expected_bytes,
          CASE WHEN expected_bytes > 0 AND table_bytes > 0
          AND expected_bytes <= table_bytes
          THEN (table_bytes - expected_bytes)::NUMERIC
          ELSE 0::NUMERIC END AS wasted_bytes
        FROM table_estimates;
        """
        return self.db.run_query(query)

    def index_bloat(self):
        # Based on https://github.com/pgexperts/pgx_scripts/blob/master/administration/index_bloat_check.sql
        # Original snippet is Copyright (c) 2014, PostgreSQL Experts, Inc.
        query = """
        WITH btree_index_atts AS (
          SELECT nspname, relname, reltuples, relpages, indrelid, relam,
                 regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
                 indexrelid as index_oid
            FROM pg_index
            JOIN pg_class ON pg_class.oid=pg_index.indexrelid
            JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            JOIN pg_am ON pg_class.relam = pg_am.oid
           WHERE pg_am.amname = 'btree' AND pg_class.relpages > 0
        ),
        index_item_sizes AS (
          SELECT i.nspname,
                 i.relname,
                 i.reltuples,
                 i.relpages,
                 i.relam,
                 (quote_ident(s.schemaname) || '.' || quote_ident(s.tablename))::regclass AS starelid,
                 a.attrelid AS table_oid,
                 index_oid,
                 current_setting('block_size')::numeric AS bs,
                 8 AS maxalign,
                 24 AS pagehdr,
                 /* per tuple header: add index_attribute_bm if some cols are null-able */
                 CASE WHEN max(coalesce(s.null_frac, 0)) = 0
                     THEN 2
                     ELSE 6
                 END AS index_tuple_hdr,
                 /* data len: we remove null values save space using it fractionnal part from stats */
                 sum( (1 - coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS nulldatawidth
            FROM pg_attribute a
            JOIN pg_stats s ON (quote_ident(s.schemaname) || '.' || quote_ident(s.tablename))::regclass = a.attrelid AND s.attname = a.attname
            JOIN btree_index_atts i ON i.indrelid = a.attrelid AND a.attnum = i.attnum
           WHERE a.attnum > 0
           GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        ),
        index_aligned AS (
          SELECT maxalign, bs, nspname, relname AS index_name, reltuples,
                 relpages, relam, table_oid, index_oid,
                 ( 6
                   + maxalign
                   /* Add padding to the index tuple header to align on MAXALIGN */
                   - CASE
                       WHEN index_tuple_hdr % maxalign = 0 THEN maxalign
                       ELSE index_tuple_hdr % maxalign
                     END
                   + nulldatawidth
                   + maxalign
                   /* Add padding to the data to align on MAXALIGN */
                   - CASE
                       WHEN nulldatawidth::integer % maxalign = 0 THEN maxalign
                       ELSE nulldatawidth::integer % maxalign
                     END
                )::numeric AS nulldatahdrwidth, pagehdr
           FROM index_item_sizes
        ),
        otta_calc AS (
          SELECT bs, nspname, table_oid, index_oid, index_name, relpages,
                 coalesce(
                    ceil(reltuples * nulldatahdrwidth)::numeric / bs
                    - pagehdr::numeric
                    /* btree and hash have a metadata reserved block */
                    + CASE WHEN am.amname IN ('hash', 'btree') THEN 1 ELSE 0 END,
                    0
                 ) AS otta
          FROM index_aligned
          LEFT JOIN pg_am am ON index_aligned.relam = am.oid
        )
        SELECT sub.index_oid,
          CASE
            WHEN sub.relpages <= otta THEN 0
            ELSE bs * (sub.relpages - otta)::bigint
          END AS wasted_bytes
        FROM otta_calc AS sub
             JOIN pg_class AS c ON c.oid = sub.table_oid
             JOIN pg_stat_user_indexes AS stat ON sub.index_oid = stat.indexrelid
        """
        return self.db.run_query(query)

    def bgwriter_stats(self):
        query = "SELECT * FROM pg_stat_bgwriter"
        return self.db.run_query(query)

    def db_stats(self):
        query = "SELECT * FROM pg_stat_database WHERE datname = current_database()"
        return self.db.run_query(query)

    def server_stats(self):
        query = """
        SELECT pg_postmaster_start_time() AS postmaster_start_time,
               pg_conf_load_time() AS conf_load_time
        """
        return self.db.run_query(query)

    def settings(self):
        query = "SELECT name, setting, unit, boot_val, reset_val, source, sourcefile, sourceline FROM pg_settings"
        result = self.db.run_query(query)

        for row in result:
            row['current_value'] = row.pop('setting')
            row['boot_value'] = row.pop('boot_val')
            row['reset_value'] = row.pop('reset_val')

        return result

    def have_stat_activity_helper(self):
        query = """
        SELECT 1 AS enabled
          FROM pg_proc
          JOIN pg_namespace ON (pronamespace = pg_namespace.oid)
         WHERE nspname = 'pganalyze' AND proname = 'get_stat_activity'
        """
        return self.db.run_query(query) == [{"enabled": 1}]

    def backends(self):
        # http://www.postgresql.org/docs/devel/static/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW
        #
        # Note: We don't include query to avoid sending sensitive data
        query = """
        SELECT pid, usename, application_name, client_addr::text, backend_start,
               xact_start, query_start, state_change, waiting, state
        """

        if self.have_stat_activity_helper():
            query += " FROM pganalyze.get_stat_activity()"
        else:
            query += " FROM pg_stat_activity"

        query += " WHERE pid <> pg_backend_pid() AND datname = current_database()"

        return self.db.run_query(query)

    def replication(self):
        # http://www.postgresql.org/docs/devel/static/monitoring-stats.html#PG-STAT-REPLICATION-VIEW
        query = """
        SELECT pid, usename, application_name, client_addr::text, client_port,
               backend_start, state, sync_priority, sync_state, sent_location::text,
               write_location::text, flush_location::text, replay_location::text
          FROM pg_stat_replication
         WHERE pid <> pg_backend_pid()
        """

        return self.db.run_query(query)

    def replication_conflicts(self):
        # http://www.postgresql.org/docs/devel/static/monitoring-stats.html#PG-STAT-DATABASE-CONFLICTS-VIEW
        query = """
        SELECT confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock
          FROM pg_stat_database_conflicts
         WHERE datname = current_database()
        """

        return self.db.run_query(query)

    def locks(self):
        # http://www.postgresql.org/docs/devel/static/view-pg-locks.html
        query = """
        SELECT n.nspname AS schema,
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
        WHERE l.pid <> pg_backend_pid() AND
              (d.datname IS NULL OR d.datname = current_database())
        """

        return self.db.run_query(query)

    def functions(self):
        query = """
        SELECT pn.nspname AS schema_name,
               pp.proname AS function_name,
               pl.lanname AS language,
               pp.prosrc AS source,
               pp.probin AS source_bin,
               pp.proconfig AS config,
               pg_get_function_arguments(pp.oid) AS arguments,
               pg_get_function_result(pp.oid) AS result,
               pp.proisagg AS aggregate,
               pp.proiswindow AS window,
               pp.prosecdef AS security_definer,
               pp.proleakproof AS leakproof,
               pp.proisstrict AS strict,
               pp.proretset AS returns_set,
               pp.provolatile AS volatile,
               ps.calls,
               ps.total_time,
               ps.self_time
          FROM pg_proc pp
         INNER JOIN pg_namespace pn ON (pp.pronamespace = pn.oid)
         INNER JOIN pg_language pl ON (pp.prolang = pl.oid)
          LEFT JOIN pg_stat_user_functions ps ON (ps.funcid = pp.oid)
         WHERE pl.lanname != 'internal'
               AND pn.nspname NOT IN ('pg_catalog', 'information_schema')
               AND pp.proname NOT IN ('pg_stat_statements', 'pg_stat_statements_reset')
        """
        return self.db.run_query(query)
