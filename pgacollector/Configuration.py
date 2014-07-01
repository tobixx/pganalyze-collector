# Copyright (c) 2014, pganalyze Team <team@pganalyze.com>
#  All rights reserved.

import logging
import sys
import urlparse
import os
from .SystemInformation import SystemInformation
from stat import *
import ConfigParser

logger = logging.getLogger(__name__)


class Configuration():

    def __init__(self, option):
        self.option = option

    def read(self):
        if SystemInformation().on_heroku:
            return self.read_heroku()
        else:
            return self.read_file()

    def read_heroku(self):
        logger.debug("Reading heroku-style config from environment DATABASE_URL")
        urlparse.uses_netloc.append('postgres')
        url = urlparse.urlparse(os.environ['DATABASE_URL'])

        config = {'username': url.username,
                  'password': url.password,
                  'host': url.hostname,
                  'port': url.port,
                  'dbname': url.path[1:]}

        api_key = os.environ['PGANALYZE_APIKEY']
        api_url = self.option['api_url']

    def read_file(self):
        logger.debug("Reading filesystem config")

        configfile = None
        for candidate in self.option['configfile']:
            try:
                mode = os.stat(candidate).st_mode
            except Exception as e:
                logger.debug("Couldn't stat file: %s" % e)
                continue

            if not S_ISREG(mode):
                logger.debug("%s isn't a regular file" % candidate)
                continue

            if int(oct(mode)[-2:]) != 0:
                logger.error("Configfile is accessible by other users, please run `chmod go-rwx %s`" % candidate)
                sys.exit(1)

            if not os.access(candidate, os.R_OK):
                logger.debug("%s isn't readable" % candidate)
                continue

            configfile = candidate
            break

        if not configfile:
            logger.error("Couldn't find a readable config file, perhaps create one with --generate-config?")
            sys.exit(1)

        configparser = ConfigParser.RawConfigParser()

        try:
            configparser.read(configfile)
        except Exception as e:
            logger.error(
                "Failure while parsing %s: %s, please fix or create a new one with --generate-config" % (configfile, e))
            sys.exit(1)

        configdump = {}
        logger.debug("read config from %s" % configfile)
        for k, v in configparser.items('pganalyze'):
            configdump[k] = v
            # Don't print the password to debug output
            if k == 'db_password': v = '***removed***'
            logger.debug("%s => %s" % (k, v))

        dbconf = {}
        dbconf['username'] = configdump.get('db_username')
        dbconf['password'] = configdump.get('db_password')
        dbconf['host'] = configdump.get('db_host')
        # Set db_host to localhost if not specified and db_password present to force non-unixsocket-connection
        if not dbconf['host'] and dbconf['password']:
            dbconf['host'] = 'localhost'
        dbconf['port'] = configdump.get('db_port')
        dbconf['dbname'] = configdump.get('db_name')
        dbconf['api_key'] = configdump.get('api_key')
        dbconf['api_url'] = configdump.get('api_url', self.option['api_url'])

        if not dbconf['dbname'] and dbconf['api_key']:
            logger.error(
                "Missing database name and/or api key in configfile %s, perhaps create one with --generate-config?" % configfile)
            sys.exit(1)
        return dbconf

    def write(self):

        apikey = self.option['apikey'] if self.option['apikey'] is not None else 'fill_me_in'

        sample_config = '''[pganalyze]
    api_key: %s
    db_name: fill_me_in
    #db_username:
    #db_password:
    #db_host: localhost
    #db_port: 5432
    #api_url: %s
    ''' % (apikey, self.option['api_url'])

        cf = self.option['configfile'][0]

        try:
            f = os.open(cf, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0600)
            os.write(f, sample_config)
            os.close(f)
        except Exception as e:
            logger.error("Failed to write configfile: %s" % e)
            sys.exit(1)
        logger.info("Wrote standard configuration to %s, please edit it and then run the script again" % cf)

