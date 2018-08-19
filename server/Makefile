SPECFILE=provisioning-server.spec

INSTBASE=/var/www/provisioning
CONFBASE=/etc/httpd/conf.d
DESTDIR=$(shell pwd)/fakeroot
TOPDIR=$(shell pwd)/haterpm
CLIENTDESTDIR=$(shell pwd)/fakeroot.client
NAME := $(shell perl -ne 'if (/^Name:/) { print ((split /\s+/)[1]), "\n"}' $(SPECFILE))
VERSION := $(shell perl -ne 'if (/^Version:/) { print ((split /\s+/)[1]), "\n"}' debian.control)
SOURCES=lib/Provisioning/Provision.pm lib/Provisioning/HardwareInventory.pm lib/Provisioning/Common.pm lib/Provisioning/InventoryCommon.pm
SWSOURCES=lib/SwReality/SwReality.pm
CONFFILES=conf/provisioning.conf conf/swreality.conf
DOCSFILES=docs/status

all:
	@echo Nothing to build

install:
	mkdir -p $(DESTDIR)/$(INSTBASE)
	mkdir -p $(DESTDIR)/$(CONFBASE)
	mkdir -p $(DESTDIR)/var/log/provisioning
	mkdir -p $(DESTDIR)/var/log/swreality
	mkdir -p $(DESTDIR)/var/log/inventory
	mkdir -p $(DESTDIR)/var/log/httpd/provisioning
	mkdir -p $(DESTDIR)/var/log/httpd/swreality
	install -d -m 0755 $(DESTDIR)/$(INSTBASE)/lib/Provisioning
	install -d -m 0755 $(DESTDIR)/$(INSTBASE)/lib/SwReality
	install -d -m 0755 $(DESTDIR)/$(INSTBASE)/docs
	for f in $(SOURCES); do \
		install -m 0755 -c $$f $(DESTDIR)/$(INSTBASE)/lib/Provisioning; \
	done
	for f in $(SWSOURCES); do \
		install -m 0755 -c $$f $(DESTDIR)/$(INSTBASE)/lib/SwReality; \
	done
	for f in $(DOCSFILES); do \
		install -m 0644 -c $$f $(DESTDIR)/$(INSTBASE)/docs; \
	done
	for f in $(CONFFILES); do \
		install -m 0644 -c $$f $(DESTDIR)/$(CONFBASE); \
	done

buildpkg:
	rm -rf haterpm
	mkdir -p $(TOPDIR) $(TOPDIR)/BUILD $(TOPDIR)/RPMS $(TOPDIR)/SOURCES \
		$(TOPDIR)/SPECS $(TOPDIR)/SRPMS $(DESTDIR)/$(NAME)-$(VERSION)
	rsync -avPR $(NAME).spec Makefile $(SOURCES) $(SWSOURCES) $(CONFFILES) $(DOCSFILES) $(DESTDIR)/$(NAME)-$(VERSION)
	tar cvCfz $(DESTDIR) $(TOPDIR)/SOURCES/$(NAME)-$(VERSION).tar.gz \
		$(NAME)-$(VERSION)
	rpmbuild --define "_topdir $(TOPDIR)" -bb $(SPECFILE)


buildpkg-deb:
	rm -rf fakeroot
	mkdir -p $(DESTDIR)/$(NAME)-$(VERSION)
	rsync -avPR $(NAME).spec Makefile $(SOURCES) $(SWSOURCES) $(CONFFILES) $(DOCSFILES) $(DESTDIR)/$(NAME)-$(VERSION)
	mkdir $(DESTDIR)/$(NAME)-$(VERSION)/debian
	for i in debian.*; do cp $$i $(DESTDIR)/$(NAME)-$(VERSION)/debian/`echo $$i | sed -e 's/debian.//'`; done
	(cd $(DESTDIR)/$(NAME)-$(VERSION) && debuild -us -uc )

clean:
	rm -rf $(TOPDIR) $(DESTDIR)
