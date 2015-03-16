import logging
import sys

logger = logging.getLogger(__name__)

db_driver = None

try:
    import psycopg2 as pg
    db_driver = 'psycopg'
except Exception as e:
    pass

if db_driver == None:
    try:
        import pg8000 as pg
        db_driver = 'pg8000'
    except Exception as e:
        pass

if db_driver == None:
    print("*** Couldn't import database driver")
    print("*** Please install the python-psycopg2 package or the pg8000 module")
    sys.exit(1)

class DB():

    def __init__(self, dbname, querymarker, username=None, password=None, host=None, port=None):
        self.querymarker = '/* ' + querymarker + ' */'
        self.conn = self._connect(dbname, username, password, host, port)
        logger.debug("Connected to database using %s driver" % db_driver)

        self._register_pg_type_wrappers()
        self.version_numeric = int(self.run_query('SHOW server_version_num')[0]['server_version_num'])

    def run_query(self, query, should_raise=False, commit=False):
        # pg8000 is picky regarding % characters in query strings, escaping with extreme prejudice
        if db_driver == 'pg8000' and '%' in query:
            logger.debug("Escaping % characters in query string")
            query = query.replace('%', '%%')

        logger.debug("Running query: %s" % query)

        # Prepending querymarker to be able to filter own queries during subsequent runs
        query = self.querymarker + query

        cur = self.conn.cursor()

        try:
            cur.execute(query)

            if commit:
                self.conn.commit()
        except Exception as e:
            if should_raise:
                raise e
            logger.error("Got an error during query execution")
            for line in str(e).splitlines():
                logger.error(line)
            sys.exit(1)

        # Didn't get any column definition back, this is most likely a return-less command (SET et al)
        if cur.description is None:
            return []

        # Fetch column headers
        columns = [f[0] for f in cur.description]

        # Build list of hashes
        result = [dict(zip(columns, row)) for row in cur.fetchall()]
        return result

    def rollback(self):
        self.conn.rollback()

    def _pg8000_float_numeric_wrapper(self, data, offset, length):
        return float(self._pg8000_numeric_in(data, offset, length))

    def _connect(self, dbname, username, password, host, port):
        try:
            kw = {
                'database': dbname,
                'user': username,
                'password': password,
                'host': host,
                'port': port
            }

            # psycopg2 <= 2.4.2 fails if you pass None arguments, filter them out by hand.
            kw = dict((key, value) for key, value in kw.iteritems() if value is not None)
            # pg8000 expects port to be of type integer
            if 'port' in kw:
                kw['port'] = int(kw['port'])

            try:
                return self._connect_with_kw(kw)
            except Exception as e:
                logger.debug("Failure: %s, retrying with SSL", str(e))
                kw['ssl'] = True
                return self._connect_with_kw(kw)

        except Exception as e:
            logger.error("Failed to connect to database: %s", str(e))
            sys.exit(1)

    def _connect_with_kw(self, kw):
        logger.debug("Connecting to database, using driver %s, parameters: %s" % (pg.__name__, kw))
        return pg.connect(**kw)

    def _register_pg_type_wrappers(self):

        # Convert decimal values to float since JSON can't handle Decimals
        if pg.__name__ == 'pg8000':
            self._pg8000_numeric_in = self.conn.pg_types[1700][1]
            self.conn.pg_types[1700] = (pg.core.FC_TEXT, self._pg8000_float_numeric_wrapper)

        if pg.__name__ == 'psycopg2':
            dec2float = pg.extensions.new_type(
                pg.extensions.DECIMAL.values,
                'DEC2FLOAT',
                lambda value, curs: float(value) if value is not None else None)
            pg.extensions.register_type(dec2float)
