FROM phusion/baseimage:0.9.16

RUN groupadd -r pganalyze && useradd -r -g pganalyze pganalyze
ENV HOME_DIR /home/pganalyze

RUN apt-get update && apt-get install -y python

ADD . $HOME_DIR

RUN chown pganalyze:pganalyze -R $HOME_DIR

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["/sbin/my_init"]
