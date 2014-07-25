# Copyright (c) 2014, pganalyze Team <team@pganalyze.com>
#  All rights reserved.

import logging
import re
import sys

logger = logging.getLogger(__name__)


class PgStatPlans():

    def __init__(self, db):
        self.db = db
        if db.version_numeric < 90100:
            logger.error("To use pg_stat_plans you must have at least Postgres 9.1 or newer")
            sys.exit(1)

    def fetch_queries(self, send_query_parameters):
        both_fields = ["userid", "dbid",
                       "calls", "rows", "total_time",
                       "shared_blks_hit", "shared_blks_read", "shared_blks_written",
                       "local_blks_hit", "local_blks_read", "local_blks_written",
                       "temp_blks_read", "temp_blks_written"]

        query_fields = ["time_variance", "time_stddev"] + both_fields

        plan_fields = ["planid", "had_our_search_path", "from_our_database",
                       "query_explainable", "last_startup_cost", "last_total_cost"] + both_fields

        query = "SELECT pq.normalized_query AS pq_normalized_query"
        query += ", p.query AS p_query"

        # Generate list of fields we'e interested in
        query += ", " + ", ".join(map(lambda s: "pq.%s AS pq_%s" % (s, s), query_fields))
        query += ", " + ", ".join(map(lambda s: "p.%s AS p_%s" % (s, s), plan_fields))

        query += " FROM pg_stat_plans p"
        query += " LEFT JOIN pg_stat_plans_queries pq ON p.planid = ANY (pq.planids)"

        #FIXME: Should all these exclusions moved down to the query_explainable check?

        # EXPLAIN, COPY and SET commands cannot be explained
        query += " WHERE p.query !~* '^\\s*(EXPLAIN|COPY|SET)\\y'"

        # Plans in pg_catalog cannot be explained
        query += " AND p.query !~* '\\ypg_catalog\\.'"

        # We don't want our stuff in the statistics
        query += " AND p.query !~* '^%s'" % re.sub(r'([*/])', r'\\\1', self.db.querymarker)

        # Remove all plans which we can't explain
        query += " AND p.from_our_database = TRUE"
        query += " AND p.planid = ANY (pq.planids);"

        fetch_plan = "SELECT pg_stat_plans_explain(%s, %s, %s) AS explain"
        set_explain_format = "SET pg_stat_plans.explain_format TO JSON; "

        self.db.run_query(set_explain_format, True)

        queries = {}

        # Fetch joined list of all queries and plans
        for row in self.db.run_query(query, False):

            # merge pg_stat_plans_queries values into result
            query = dict((key[3:], row[key]) for key in filter(lambda r: r.find('pq_') == 0, row))
            normalized_query = query['normalized_query']

            logger.debug("Processing query: %s" % normalized_query)

            # if we haven't seen the query yet - add it
            if 'normalized_query' not in queries:
                queries[normalized_query] = query

            # merge pg_stat_plans values into result
            plan = dict((key[2:], row[key]) for key in filter(lambda r: r.find('p_') == 0, row))

            # Remove example queries containing literals if so desired by the user
            if not send_query_parameters:
                del (plan['query'])

            # initialize plans array
            if 'plans' not in queries[normalized_query]:
                queries[normalized_query]['plans'] = []

            # try explaining the query if pg_stat_plans thinks it's possible
            if plan['query_explainable']:
                try:
                    result = self.db.run_query(fetch_plan % (plan['planid'], plan['userid'], plan['dbid']), True)
                    plan['explain'] = result[0]['explain']
                except Exception as e:
                    logger.debug("Got an error while explaining: %s" % e)
                    plan['explain_error'] = str(e)
                    self.db.rollback()
                    self.db.run_query(set_explain_format, True)

            queries[normalized_query]['plans'].append(plan)

        return queries.values()