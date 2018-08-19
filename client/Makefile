SPECFILE=jazzhands-server-inventory.spec

INSTBASE=/usr/libexec/jazzhands/server-inventory
DESTDIR=$(shell pwd)/fakeroot
TOPDIR=$(shell pwd)/haterpm
CLIENTDESTDIR=$(shell pwd)/fakeroot.client
NAME := $(shell perl -ne 'if (/^Name:/) { print ((split /\s+/)[1]), "\n"}' $(SPECFILE))
VERSION := $(shell perl -ne 'if (/^Version:/) { print ((split /\s+/)[1]), "\n"}' $(SPECFILE))
SOURCES=server-inventory.pl
MODULES=DeviceInventory _LocalHooks

all:
	@echo Nothing to build

install:
	mkdir -p $(DESTDIR)/$(INSTBASE)
	mkdir -p $(DESTDIR)/$(INSTBASE)/modules
	for i in $(SOURCES); do \
		install -m 0755 -c $$i $(DESTDIR)/$(INSTBASE)/`basename $$i .pl`; \
	done
	for i in $(MODULES); do \
		install -m 0755 -c modules/$${i}.pm $(DESTDIR)/$(INSTBASE)/modules; \
	done

buildpkg:
	rm -rf haterpm
	mkdir -p $(TOPDIR) $(TOPDIR)/BUILD $(TOPDIR)/RPMS $(TOPDIR)/SOURCES \
		$(TOPDIR)/SPECS $(TOPDIR)/SRPMS $(DESTDIR)/$(NAME)-$(VERSION)
	rsync -avP $(NAME).spec Makefile $(SOURCES) modules $(DESTDIR)/$(NAME)-$(VERSION)
	tar cvCfz $(DESTDIR) $(TOPDIR)/SOURCES/$(NAME)-$(VERSION).tar.gz \
		$(NAME)-$(VERSION)
	rpmbuild --define "_topdir $(TOPDIR)" -bb $(SPECFILE)

clean:
	rm -rf $(TOPDIR) $(DESTDIR)
