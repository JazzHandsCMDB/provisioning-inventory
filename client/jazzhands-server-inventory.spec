Summary:    jazzhands-server-inventory - perform server hardware inventory
Vendor:     JazzHands
Name:       jazzhands-server-inventory
Version:    1.0
Release:    0
License:    Unknown
Group:      System/Management
Url:        http://www.jazzhands.net/
BuildArch:  noarch
BuildRoot:	%{_tmppath}/%{name}-%{version}-%(id -u -n)
Source:		%{name}-%{version}.tar.gz
Requires:	lshw
Requires:	ipmitool
Requires:	lvm2
Requires:	udev
Requires:	pciutils
Requires:	dmidecode
Requires:	smartmontools
Requires:	perl-JSON
Requires:	perl-JSON-XS
Requires:	perl-NetAddr-IP
Requires:	perl-libwww-perl

%if %{_vendor} != "suse"
%if 0%{?rhel} > 5
Requires:   lldpd
%endif
%endif

%description
Tool to perform server hardware inventories (CPU, memory, disk configuration,
etc) and upload it to a central server for processing

%prep

%setup -q -n %{name}-%{version}

%{__make}

%build

%install
%{__make} DESTDIR=%{buildroot} install

%files
%dir /usr/libexec/jazzhands/server-inventory
%dir /usr/libexec/jazzhands/server-inventory/modules
%attr (-, root, bin) /usr/libexec/jazzhands/server-inventory/server-inventory
%attr (-, root, bin) /usr/libexec/jazzhands/server-inventory/modules/DeviceInventory.pm
