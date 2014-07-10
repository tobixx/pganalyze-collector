MYNAME=pganalyze-collector
PKGVER := $(shell git describe --tags)
ZIPNAME := $(MYNAME)-$(PKGVER).zip
DEPENDENCIES := -d python -d python-psycopg2
buildpackage = fpm --verbose -s dir -t $(1) -a all -n $(MYNAME) -v $(PKGVER) $(DEPENDENCIES)\
	       -m "<team@pganalyze.com>" --url "https://pganalyze.com/"\
	       --description "pganalyze collector script"\
	       --vendor "pganalyze" --license="BSD"\
	       ./build/opt/.=/opt/$(MYNAME) ./build/$(MYNAME)=/usr/bin/$(MYNAME)

SHIPME=LICENSE pganalyze-collector.py vendor pgacollector CHANGELOG.md

all: deb rpm zip


deb: build
	$(call buildpackage,deb)


rpm: build
	$(call buildpackage,rpm)


zip: build
	cp build/opt/pganalyze-collector.py build/opt/__main__.py
	cd build/opt; zip --quiet ../../$(ZIPNAME).tmp -r $(SHIPME) __main__.py
	echo "#!/usr/bin/env python" | cat - $(ZIPNAME).tmp > $(ZIPNAME)
	chmod +x $(ZIPNAME)
	rm $(ZIPNAME).tmp
	rm build/opt/__main__.py


build: clean
	mkdir -p build/opt
	cp -a $(SHIPME) ./build/opt
	ln -s /opt/pganalyze-collector/pganalyze-collector.py ./build/$(MYNAME)


clean:
	rm -rf build *.deb *.rpm *.zip

