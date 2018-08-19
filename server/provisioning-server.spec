Summary:	provisioning-server - Apache module for provisioning
Vendor:		Provisioning
Name:		provisioning-server
Version:	1.12.4
Release:	0
License:	Unknown
Group:		System/Management
Url:		http://www.appnexus.com/
Requires:	httpd, mod_ssl, mod_auth_kerb, perl-Apache-DBI
BuildArch:	noarch
BuildRoot:	%{_tmppath}/%{name}-%{version}-%(id -u -n)
Source:		%{name}-%{version}.tar.gz

%description
Apache module for automated device provisioning, hardware and software reality reporting backend

%prep

%setup -q -n %{name}-%{version}

%{__make}

%build

%install
%{__make} DESTDIR=%{buildroot} install

%files
%attr (-, root, bin) /var/www/provisioning/lib/Provisioning/Common.pm
%attr (-, root, bin) /var/www/provisioning/lib/Provisioning/Provision.pm
%attr (-, root, bin) /var/www/provisioning/lib/Provisioning/InventoryCommon.pm
%attr (-, root, bin) /var/www/provisioning/lib/Provisioning/HardwareInventory.pm
%attr (-, root, bin) /var/www/provisioning/lib/SwReality/SwReality.pm
%attr (-, root, bin) /var/www/provisioning/docs/status
%attr (-, root, bin) /etc/httpd/conf.d/provisioning.conf
%attr (-, root, bin) /etc/httpd/conf.d/swreality.conf
%attr (-, root, bin) /var/log/httpd/swreality
%attr (-, root, bin) /var/log/httpd/provisioning
%attr (-, apache, bin) /var/log/swreality
%attr (-, apache, bin) /var/log/provisioning
%attr (-, apache, bin) /var/log/inventory
