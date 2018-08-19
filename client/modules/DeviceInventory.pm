package DeviceInventory;

use strict;
use warnings;

use Exporter 'import';
use vars qw(@ISA %EXPORT_TAGS @EXPORT);
use FileHandle;
use File::stat;
use JSON;
use Data::Dumper;
use Math::BigInt;
use POSIX qw(uname);

our @EXPORT_OK = qw(_options);

our $Programs = {
	dmidecode => '/usr/sbin/dmidecode',
	ipmitool => '/usr/bin/ipmitool',
	lsb_release => '/usr/bin/lsb_release',
	lldptool => '/usr/sbin/lldptool',
	lldpctl => '/usr/sbin/lldpctl',
	lshw => [ '/usr/sbin/lshw', '/usr/bin/lshw' ],
	MegaCli => [ '/usr/sbin/MegaCli64', '/opt/MegaRAID/MegaCli/MegaCli64', '/usr/sbin/megacli' ],
	tcpdump => '/usr/sbin/tcpdump',
	smartctl => '/usr/sbin/smartctl',
	ip => '/sbin/ip',
	pvs => [ '/sbin/pvs', '/usr/sbin/pvs'],
	vgs => [ '/sbin/vgs', '/usr/sbin/vgs'],
	lvs => [ '/sbin/lvs', '/usr/sbin/lvs'],
	lspci => [ '/sbin/lspci', '/usr/sbin/lspci', '/usr/bin/lspci' ],
	udevadm => '/sbin/udevadm'
};

%EXPORT_TAGS =
	(
		'all' => [ qw(_options) ],
	);

sub _options {
	if(ref $_[0] eq 'HASH') {
		return $_[0];
	}
	my %ret = @_;
	for my $v ( grep { /^-/ } keys %ret ) {
		$ret{ substr( $v, 1 ) } = $ret{$v};
	}
	\%ret;
}

sub SetError {
	my $error = shift;

	if (ref($error) eq "ARRAY") {
		push @{$error}, @_;
		return;
	}

	if (ref($error) eq "SCALAR") {
		$$error = shift;
		return;
	}
}

sub new {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	my $opt = &_options(@_);

	my $self = {
		inventory => {}
	};

	bless $self, $class;
}

sub _runcmd {
	my $opt = _options(@_);

	my $tries = $opt->{tries} || 1;

	my @output;
	if (!$opt->{command}) {
		SetError($opt->{errors}, 'Command not passed to _runcmd');
		return undef;
	}
	my $command = undef;
	if (ref($opt->{command}) eq 'ARRAY') {
		$command = (grep { -x $_ } @{$opt->{command}})[0];
	} else {
		$command = $opt->{command};
	}
	if (!$command) {
		SetError($opt->{errors}, sprintf('Command not found in %s',
			(join ',', @{$opt->{command}})));
		return undef;
	}

#	printf STDERR "Command is: %s\n", $command;
	if (! -x $command) {
		SetError($opt->{errors}, sprintf('Command not found: %s', $command));
		return undef;
	}
	
	my $CMD;
	if (!exists($opt->{args})) {
		$opt->{args} = [];
	}

	if ($opt->{debug}) {
		printf STDERR "Running: %s %s\n",
			$command,
			(join ' ', @{$opt->{args}});
	}

	my $current_try = 0;

	my $nullfh;

	while ($current_try < $tries ){
		my $pid;
		eval {
			local $SIG{ALRM} = sub { die "alarm\n"; };
			if ($opt->{timeout}) {
				alarm $opt->{timeout};
			}

			$pid = open ($CMD, "-|");

			if (!defined($pid)) {
				SetError($opt->{errors},
					sprintf('Error running "%s": %s',
						(join (' ', $command, @{$opt->{args}})),
						$!));
				die "error\n";
			}
			if (!$pid) {
				open (STDERR, '>&', STDOUT);
				exec $command, @{$opt->{args}};

				# Should never get here

				exit 0;
			}

			chomp(@output = <$CMD>);
			close $CMD;
		};
		alarm 0;
		if ($@) {
			if ($@ eq "alarm\n") {
				kill 'KILL', $pid if $pid;
				close $CMD if defined($CMD);
			}
		} elsif ($? >> 8) {
			if ($opt->{debug}) {
				printf STDERR "Process %s died with error code %s\n",
					(join ' ', $opt->{command}, @{$opt->{args}}),
					$? >> 8;
			}
		} else {
			return \@output;
		}
		$current_try += 1;
	}
	if ($opt->{debug}) {
		print STDERR "returning failure\n";
	}
	return undef;
}

my $dmidecode_map = {
	handle => {
		'BIOS Information' => 'BIOS',
		'System Information' => 'system',
		'Base Board Information' => 'motherboard',
		'Chassis Information' => 'chassis',
		'Processor Information' => 'processor',
		'Port Connector Information' => 'port',
		'System Slot Infomration' => 'slot',
		'On Board Device Information' => 'device',
		'Physical Memory Array' => 'memory_array',
		'Memory Device' => 'memory',
	},
	system => {
		Manufacturer 	=> 'vendor',
		'Product Name' 	=> 'model',
		Version			=> 'model_version',
		'Serial Number'	=> 'serial_number',
		UUID			=> 'UUID',
		Family			=> 'system_type'
	},
	BIOS => {
		Vendor			=> 'vendor',
		Version			=> 'version',
		'BIOS Revision'	=> 'revision',
	},
	motherboard	=> {
		Manufacturer	=> 'vendor',
		'Product Name'	=> 'model',
		Version			=> 'version',
		'Serial Number'	=> 'serial_number',
	},
	chassis => {
		Manufacturer	=> 'vendor',
		Type			=> 'chassis_type',
		'Serial Number'	=> 'serial_number',
	},
	processor => {
		'Socket Designation'	=> 'socket',
		'Upgrade'				=> 'socket_type',
		Family					=> 'family',
		Manufacturer			=> 'vendor',
		ID						=> 'id',
		Signature				=> 'signature',
		Version					=> 'model',
		Voltage					=> 'voltage',
		'External Clock'		=> 'clock',
		'Max Speed'				=> 'speed',
		'Core Count'			=> 'cores',
		'Thread Count'			=> 'threads',
	},
	port => {
		'External Reference Designator'	=> 'port_name',
		'External Connector Type'		=> 'port_style',
		'Port Type'						=> 'port_type',
	},
	slot => {
		Designation		=> 'slot_name',
		Type			=> 'slot_type',
		Length			=> 'length',
		'Bus Address'	=> 'bus_address',
	},
	device => {
		Type			=> 'device_type',
		Status			=> 'device_status',
		Description		=> 'description'
	},
	memory_array => {
		'Maximum Capacity'	=> 'max_memory',
		'Number Of Devices'	=> 'memory_slots',
	},
	memory => {
		'Form Factor'	=> 'form_factor',
		'Size'			=> 'memory_size',
		'Type'			=> 'memory_type',
		'Locator'		=> 'locator',
		'Bank Locator'	=> 'slot',
		'Speed'			=> 'speed',
		'Manufacturer'	=> 'vendor',
		'Serial Number'	=> 'serial_number',
		'Part Number'	=> 'part_number',
	}
};

sub GetOSInfo {
	#
	# Populate some basic OS info, since some things care about that
	#
	
	my $self = shift;
	my $opt = _options(@_);

	my $os = {};
	$os->{type} = (POSIX::uname)[0];

	if ($os->{type} eq 'Linux') {
		#
		# We have to do this in two pieces, because CentOS/RedHat sucks
		#
		my $output = _runcmd(
			errors => $opt->{errors},
			debug => $opt->{debug},
			command => $Programs->{lsb_release},
			args => [ qw(-i -s) ]
			);

		if (defined($output)) {
			chomp($os->{distribution} = $output);
		}

		#
		# We have to do this in two pieces, because CentOS/RedHat sucks
		#
		$output = _runcmd(
			errors => $opt->{errors},
			debug => $opt->{debug},
			command => $Programs->{lsb_release},
			args => [ qw(-r -s) ]
			);

		if (defined($output)) {
			chomp($os->{release} = $output);
		}
	} else {
		$os->{release} = (POSIX::uname)[2];
	}

	$self->{os} = $os;
	$self->{inventory}->{os} = $os;
	return 1;
}

sub GetBasicSystemInfo {
	#
	# Execute dmidecode and pull the basic information
	# We could use lshw -json for this, but shockingly its format is more
	# annoying
	#
	my $self = shift;
	my $opt = _options(@_);

	my $output = _runcmd(
		errors => $opt->{errors},
		debug => $opt->{debug},
		command => $Programs->{dmidecode},
	#	args => [ qw( -t system ) ]
		);

	if (!defined($output)) {
		return undef;
	}

	if (!exists($self->{inventory}->{system})) {
		$self->{inventory}->{system} = {};
	}
	my $sysinv = $self->{inventory}->{system};

	my $handle;
	my $type;
	my $type_hash;
	foreach my $entry (@$output) {
		#
		# If the line begins with 'Handle', then this is a new
		# device
		#
		next if $entry =~ /^#/;
		if ($entry =~ /^Handle/) {
			$type = undef;
			$handle = {};
			$type_hash = undef;
			next;
		}
		if ($entry =~ /^\S/) {
			next if !$dmidecode_map->{handle}->{$entry};

			$type = $dmidecode_map->{handle}->{$entry};
			$type_hash = $dmidecode_map->{$type};
			if (!exists($sysinv->{$type})) {
				$sysinv->{$type} = $handle;
			} else {
				if (ref($sysinv->{$type}) eq 'HASH') {
					$sysinv->{$type} = [ $sysinv->{$type} ];
				}
				push @{$sysinv->{$type}}, $handle;
			}
			next;
		}
		my ($key, $value) = ($entry =~ /^\s+([^:]+): (.*)/);
		next if !$key;
		$value =~ s/\s*$//;
		if (defined($type_hash->{$key})) {
			$handle->{$type_hash->{$key}} = $value;
		}
	}

	#
	# The C6100s just suck
	#
	if ($sysinv->{system}->{model} && $sysinv->{system}->{model} eq 'C6100') {
		# Set the memory type to DDR3 and swap 'locator' and 'slot'
		map {
			$_->{memory_type} = 'DDR3';
			my $x = $_->{slot};
			$_->{slot} = $_->{locator};
			$_->{locator} = $x;
		} @{$sysinv->{memory}};
	}

	#
	# The SuperMicros also suck
	#

	if ($sysinv->{system}->{model} && $sysinv->{system}->{model} eq 'H8QG6') {
		# remove the serial number from the memory, because srsly
		print "Removing memory serial...\n";
		map {
			delete $_->{serial_number};
		} @{$sysinv->{memory}};
	}

	if (($sysinv->{system}->{serial_number} eq "1234567890")
			|| ($sysinv->{system}->{serial_number} eq "0123456789")
			|| ($sysinv->{system}->{serial_number} eq "............")) {
		if ($sysinv->{motherboard}->{serial_number} eq "1234567890") {
			$sysinv->{system}->{serial_number} = 
				$sysinv->{system}->{UUID};
		} else {
			$sysinv->{system}->{serial_number} = 
				$sysinv->{motherboard}->{serial_number};
		}
	}

	#
	# Do a little postprocessing
	#
	$sysinv->{total_memory} = 0;
	foreach my $memory (@{$sysinv->{memory}}) {
		my $memsize = 0;
		($memsize) = $memory->{memory_size} =~ /(\d+) MB$/;
		if (!$memsize) {
			if (($memsize) = $memory->{memory_size} =~ /(\d+) GB$/) {
				$memsize = $memsize * 1024;
			}
		}
		next if !$memsize;
		$sysinv->{total_memory} += $memsize;
	}
	1;
}

my $pci_map = {
	Class => "class",
	Vendor => "vendor",
	Device => "device",
	SVendor => "subsystem_vendor",
	SDevice => "subsystem_device"
};

sub GetPCIInfo {
	#
	# Execute dmidecode and pull the basic information
	# We could use lshw -json for this, but its format is more annoying
	#
	my $self = shift;
	my $opt = _options(@_);

	my $output = _runcmd(
		errors => $opt->{errors},
		debug => $opt->{debug},
		command => $Programs->{lspci},
		args => [ qw( -nn -mm -vv -D ) ]
		);

	if (!defined($output)) {
		return undef;
	}

	my ($version) = ($output->[0] =~ /^lspci version (\d+)/);

	if (!$version || $version < 3) {
		$output = _runcmd(
			errors => $opt->{errors},
			debug => $opt->{debug},
			command => $Programs->{lspci},
			args => [ qw( -n -m -vv -D ) ]
			);

		if (!defined($output)) {
			return undef;
		}

		if (!exists($self->{inventory}->{PCI})) {
			$self->{inventory}->{PCI} = {};
		}
		my $pci = $self->{inventory}->{PCI};
		my $comp;
		my $newslot = 1;
		foreach my $entry (@$output) {
			if ($entry =~ /^\s*$/) {
				$newslot = 1;
				next;
			}
			my ($key, $value) = $entry =~ /^(.*):\s+(.*)/;
			if ($key eq 'Device' && $newslot == 1) {
				$newslot = 0;
				$comp = { slot => $value };
				$pci->{$value} = $comp;
				next;
			}
			next if !$pci_map->{$key};
			$comp->{$pci_map->{$key}} = {
				id => $value
			};
		}
		$output = _runcmd(
			errors => $opt->{errors},
			debug => $opt->{debug},
			command => $Programs->{lspci},
			args => [ qw( -m -vv -D ) ]
			);

		if (!defined($output)) {
			return undef;
		}

		$newslot = 1;
		foreach my $entry (@$output) {
			if ($entry =~ /^\s*$/) {
				$newslot = 1;
				next;
			}
			my ($key, $value) = $entry =~ /^(.*):\s+(.*)/;
			if ($key eq 'Device' && $newslot == 1) {
				$newslot = 0;
				$comp = $pci->{$value};
				next;
			}
			next if !$pci_map->{$key};
			$comp->{$pci_map->{$key}}->{name} = $value;
		}

	} else {
		$output = _runcmd(
			errors => $opt->{errors},
			debug => $opt->{debug},
			command => $Programs->{lspci},
			args => [ qw( -nn -mm -vv -D ) ]
			);

		if (!defined($output)) {
			return undef;
		}

		if (!exists($self->{inventory}->{PCI})) {
			$self->{inventory}->{PCI} = {};
		}
		my $pci = $self->{inventory}->{PCI};
		my $comp;
		foreach my $entry (@$output) {
			next if $entry =~ /^\s*$/;
			my ($key, $value) = $entry =~ /^(.*):\s+(.*)/;
			if ($key eq 'Slot') {
				$comp = { slot => $value };
				$pci->{$value} = $comp;
				next;
			}
			next if !$pci_map->{$key};
			my ($desc, $id) = ($value =~ /(.*) \[(.*)\]/);
			$comp->{$pci_map->{$key}} = {
				name => $desc,
				id => $id
			};
		}
	}
	1;
}

my $iface_map = {
	serial			=> 'mac_address',
	product			=> 'model',
	vendor			=> 	'vendor',
	logicalname		=> 'interface_name',
	businfo			=> 'pci_slot',
	capabilities	=> 'capabilities',
};

sub GetHardwareInfo {
	my $self = shift;
	my $opt = _options(@_);

	if ($opt->{debug}) {
		printf STDERR "Pulling hardware information with lshw\n";
	}
	my $output = _runcmd(
		errors => $opt->{errors},
		debug => $opt->{debug},
		command => $Programs->{lshw},
		args => [ qw( -json -numeric -quiet) ],
		);

	if (!defined($output)) {
		return undef;
	}

	$self->{inventory}->{lshw_output} =
		decode_json join("\n", join("\n", @$output));

    if ($self->{inventory}->{system}->{system}->{model} && 
		$self->{inventory}->{system}->{system}->{model} eq 'H8QG6') {

		print "Removing memory serial...\n";
		my $memory = FindHashChild(
				hash => $self->{inventory}->{lshw_output},
				key => 'class',
				value => 'memory'
			);

		map {
			delete $_->{serial};
		} @$memory;
	}
	1;
}

sub GetNetworkInfo {
	my $self = shift;
	my $opt = _options(@_);

	my $output;
	my $errors;
	
	if ($opt->{debug}) {
		printf STDERR "Pulling interface information with lshw\n";
	}
	my $lshw_output;
	if ($self->{inventory}->{lshw_output}) {
		$self->GetHardwareInfo;
	}
	$lshw_output = $self->{inventory}->{lshw_output};

	my $netcrap = [];
	my @stufftocheck = ( $lshw_output );
	while (my $item = shift @stufftocheck) {
		if (exists $item->{children}) {
			push @stufftocheck, @{$item->{children}};
		}
		if ($item->{class} && $item->{class} eq 'network') {
			push @$netcrap, $item;
		}
	}

	#
	# Get a list of interfaces that we care about
	#
	$output = _runcmd(
		errors => \$errors,
		debug => $opt->{debug},
		command => $Programs->{ip},
		args => [ "link" ],
		);

	if (!defined($output)) {
		return undef;
	}

	my $netinfo = [];
	foreach my $line (@$output) {
		if ($line =~ /^\d+: /) {
			my ($name) = $line =~ /^\d+: ([^:]+):/;
			next if ($name =~ /^lo/);
			push @$netinfo, grep { $_->{logicalname} eq $name } @$netcrap;
		}
	}

	my $setlldp = 0;

	#
	# Only bother with this if we're doing LLDP things
	#
	if (!$opt->{lldp_method} || $opt->{lldp_method} ne 'disabled') {
		foreach my $iface_info (@$netinfo) {
			#
			# Skip various virtual interface types
			#
			if ($iface_info->{configuration}->{driver} && (
					grep { $iface_info->{configuration}->{driver} eq $_ }
						( qw (vif veth bonding docker)))) {
				if ($opt->{debug}) {
					printf "Skipping probe of interface %s\n",
						$iface_info->{logicalname};
				}
				next;
			}

			#
			# See if the link is already up, otherwise up it.  Don't care if
			# we succeed
			#
			if (!($opt->{alter_interfaces}) &&
					$iface_info->{configuration}->{link} eq 'no') {
				if ($opt->{debug}) {
					printf STDERR "Not bringing interface %s up due to config parameter\n",
						$iface_info->{logicalname}
				}
				next;
			}

			if ($iface_info->{configuration}->{link} eq 'no') {
				if ($opt->{debug}) {
					printf STDERR "Bringing up link %s\n",
						$iface_info->{logicalname};
				}
				system(qw (ip link set up), $iface_info->{logicalname});
				#
				# Wait 5 seconds to make sure link comes up if it's going to
				#
				sleep (5);
			}

			#
			# Bring up LLDP on the interface if it isn't already up
			#

			if ($opt->{lldp_method}) {
				if ($opt->{lldp_method} eq 'lldptool') {
					$output = _runcmd(
						errors => \$errors,
						debug => $opt->{debug},
						command => $Programs->{lldptool},
						args => [ qw( -l adminStatus -i ),
							$iface_info->{logicalname} ],
						);

					if (defined($errors)) {
						printf "Error running lldptool: %s\n.  Turning LLDP off\n",
							$errors;
						$opt->{lldp} = 'no';
						last;
					}

					if (!defined($output) || grep /disabled/, @$output) {
						map {
							_runcmd(
								errors => \$errors,
								debug => $opt->{debug},
								command => $Programs->{lldptool},
								args => [ @$_,
									$iface_info->{logicalname} ],
								);
						} ( [ qw (-L adminStatus=rxtx -i) ],
							[ qw (-T enableTx=yes -V portDesc -i) ],
							[ qw (-T enableTx=yes -V sysName -i) ],
							[ qw (-T enableTx=yes -V sysDesc -i) ],
						);
						$setlldp = 1;
					}
				}
			}
		}

		# If we changed LLDP things, then sleep for 60 seconds to let things
		# get information

		if ($setlldp) {
			if ($opt->{debug}) {
				print STDERR "Sleeping to let LLDP information come in\n";
			}
			sleep (60);
		}
	}

	my $interfaces = [];
	foreach my $iface_info (@$netinfo) {
		my $interface = {};
		map { $interface->{$iface_map->{$_}} = $iface_info->{$_}
			if exists $iface_info->{$_} } keys %$iface_map;
		push @$interfaces, $interface;

		#
		# Attempt to find the other side of this port with LLDP
		# unless we're told not to
		#

		next if (!$opt->{lldp_method} || $opt->{lldp_method} eq 'disabled');
		next if (!$iface_info->{logicalname});

		if (!($opt->{alter_interfaces}) &&
				$iface_info->{configuration}->{link} eq 'no') {
			next;
		}

		if ($iface_info->{configuration}->{driver} && (
				grep { $iface_info->{configuration}->{driver} eq $_ }
					( qw (vif veth bonding docker)))) {
			if ($opt->{debug}) {
				printf "Skipping LLDP probe of interface %s\n",
					$iface_info->{logicalname};
			}
			next;
		}

		my $lldp = {};
		if ($opt->{lldp_method} eq 'lldptool') {
			if ($opt->{debug}) {
				print STDERR "Getting LLDP information via lldptool\n";
			}
			$output = _runcmd(
				errors => $opt->{errors},
				debug => $opt->{debug},
				command => $Programs->{lldptool},
				args => [ qw( -t -n -i ),
					$iface_info->{logicalname} ]
				);

			#
			# If there is no LLDP information, then go on to the next thing
			#
			if (!defined($output)) {
				next;
			}
			if (!@$output) {
				next;
			}

			my $tlv;

			foreach my $line (@$output) {
				if ($line =~ /^\S.*TLV$/) {
					($tlv) = $line =~ /^(.*) TLV$/;
					next;
				}
				next if (!$tlv);
				if ($tlv eq 'Chassis ID' && $line =~ /^\s+MAC:/) {
					my ($mac) = $line =~ /^\s+MAC: (\S+)/;
					$lldp->{chassis_id} = $mac;
				}
				if ($tlv eq 'Port Description') {
					my ($name) = $line =~ /^\s+(.*)$/;
					# Strip off the unit for a Juniper port.  Because they suck
					if ($name =~ /\.0$/) {
						$name =~ s/\.0$//;
						$lldp->{interface} = $name;
					}
				}
				if ($tlv eq 'Port ID' && $line =~ /^\s+Ifname:/) {
					# strip off the unit number if it's given
					my ($name) = $line =~ /Ifname: (.*)$/;
					$name =~ s/\.0$//;
					$name =~ s/\s+//g;
					$lldp->{interface} = $name;
				}
				if ($tlv eq 'System Name') {
					my ($name) = $line =~ /^\s+(.*)/;
					$lldp->{device_name} = $name;
				}
				if ($tlv eq 'System Description') {
					my ($name) = $line =~ /^\s+(.*)/;
					$lldp->{device_type} = $name;
				}
			}

		} elsif ($opt->{lldp_method} eq 'lldpctl') {
			if ($opt->{debug}) {
				print STDERR "Getting LLDP information via lldpctl\n";
			}
			$output = _runcmd(
				errors => $opt->{errors},
				debug => $opt->{debug},
				command => $Programs->{lldpctl},
				args => [ qw( -f keyvalue ),
					$iface_info->{logicalname} ]
				);

			#
			# If there is no LLDP information, then go on to the next thing
			#
			if (!defined($output)) {
				next;
			}
			if (!@$output) {
				next;
			}

			foreach my $line (@$output) {
				my ($key, $value) = $line =~ /^lldp\.[^.]+\.([^=]*)=(.*)$/;
				next if !$key;
				if ($key eq 'port.ifname') {
					my $name = $value;
					# Strip off the unit for a Juniper port
					$name =~ s/\.0$//;
					$lldp->{interface} = $name;
				}
				if ($key eq 'chassis.name') {
					$lldp->{device_name} = $value;
				}
				if ($key eq 'chassis.descr') {
					$lldp->{device_type} = $value;
				}
				if ($key eq 'chassis.mac') {
					$lldp->{chassis_id} = $value;
				}
			}
		} elsif ($opt->{lldp_method} eq 'tcpdump') {
			if ($opt->{debug}) {
				print STDERR "Getting LLDP information via tcpdump\n";
			}

			my $output = _runcmd(
				errors => $opt->{errors},
				debug => $opt->{debug},
				command => $Programs->{tcpdump},
				args => [ qw( -v -c 1 -s 1500 -i ), 
					$iface_info->{logicalname}, qw ( ether proto 0x88cc ) ],
				timeout => 60
				);

			if (!defined($output)) {
				next;
			}
			if (!@$output) {
				if ($opt->{debug}) {
					printf STDERR "No tcpdump output for %s.\n",
						$iface_info->{logicalname};
				}
				next;
			}


			my $tlv;
			foreach my $line (@$output) {
				if ($line =~ /TLV \(\d+\)/) {
					($tlv) = $line =~ /TLV \((\d+)\)/;
				}
				next if (!$tlv);
				if ($tlv == 1 && $line =~ /Subtype MAC address.*:/) {
					my ($mac) = $line =~ /: (\S+)/;
					$lldp->{id} = $mac;
				}
				if ($tlv == 4 && $line =~ /length.*:/) {
					my ($name) = $line =~ /: (\S+)/;
					$name =~ s/\.0$//;
					$lldp->{interface} = $name;
				}
				if ($tlv == 2 && $line =~ /Interface Name.*:/) {
					# strip off the unit number if it's given
					my ($name) = $line =~ /: (.*)$/;
					$name =~ s/\.0$//;
					$name =~ s/\s+//g;
					$lldp->{interface} = $name;
				}
				if ($tlv == 5 && $line =~ /length.*:/) {
					my ($name) = $line =~ /: (\S+)/;
					$lldp->{device_name} = $name;
				}
				if ($tlv == 6 && $line !~ /length.*:/) {
					my ($name) = $line =~ /^\s+(.+)$/;
					$lldp->{device_type} = $name;
				}

			}
		}
		if (%$lldp) {
			$interface->{lldp} = $lldp;
		}

		#
		# Bring the interface back down if it was before
		#

		if ($iface_info->{configuration}->{link} eq 'no') {
			if ($opt->{debug}) {
				printf STDERR "Bringing link %s down\n",
					$iface_info->{logicalname};
			}
			system(qw (ip link set down), $iface_info->{logicalname});
		}
	}
	$self->{inventory}->{network_interfaces} = $interfaces;
	1;
}

my $ipmi_mc_map = {
	'Firmware Revision'		=> 'bmc_version',
};

my $ipmi_lan_map = {
	'IP Address Source'		=> 'bmc_ip_source',
	'IP Address'			=> 'bmc_ip_address',
	'MAC address'			=> 'bmc_mac_address',
	'MAC Address'			=> 'bmc_mac_address',
};

sub GetBMCInfo {
	my $self = shift;
	my $opt = _options(@_);

	if ($opt->{debug}) {
		print STDERR "Pulling BMC information\n";
	}
	my $bmc = {};
	my $output;
	$output = _runcmd(
		errors => $opt->{errors},
		debug => $opt->{debug},
		command => $Programs->{ipmitool},
		args => [ qw( mc info ) ]
		);

	if (!defined($output)) {
		return undef;
	}

	foreach my $bmc_info (@$output) {
		my ($key, $value) = $bmc_info =~ /^(.*\S)\s*: (.*\S)\s*$/;
		next if (!$key);
		$bmc->{$ipmi_mc_map->{$key}} = $value
			if exists $ipmi_mc_map->{$key};
	}

	$output = _runcmd(
		errors => $opt->{errors},
		debug => $opt->{debug},
		command => $Programs->{ipmitool},
		args => [ qw( lan print ) ]
		);

	if (!defined($output)) {
		return undef;
	}

	foreach my $bmc_info (@$output) {
		my ($key, $value) = $bmc_info =~ /^(.*\S)\s*: (.*\S)\s*$/;
		next if (!$key);
		$bmc->{$ipmi_lan_map->{$key}} = $value
			if exists $ipmi_lan_map->{$key};
	}

	$self->{inventory}->{bmc} = $bmc;

	1;
}

my $adp_map = {
	'Product Name'			=> 'model',
	'Serial No'				=> 'serial_number',
	'FW Package Build'		=> 'firmware_version',
};
my $drive_map = {
	'Enclosure Device ID'	=> 'enclosure_id',
	'Slot Number'			=> 'slot_number',
	'Device Id'				=> 'device_id',
	'Device Type'			=> 'device_type',
	'Raw Size'				=> 'disk_size',
	'Coerced Size'			=> 'usable_size',
	'Device Firmware Level'	=> 'firmware_version',
	'Inquiry Data'			=> 'inquiry',
	'Device Speed'			=> 'disk_speed',
	'Media Type'			=> 'media_type',
	'PD Type'				=> 'disk_proto',
};
my $smartctl_map = {
	'Device Model'			=> 'product',
	'Product'			=> 'product',
	'Serial number'			=> 'serial',
	'Rotational Rate'		=> 'media_type',
	'Transport protocol'		=> 'disk_proto',
};

my $lsi_pci_map = {
	'Bus Number'			=> 'bus',
	'Device Number'			=> 'device',
	'Function Number'		=> 'function',
};

sub GetOSDiskInfo {
	my $self = shift;
	my $opt = _options(@_);

	my $debug = $opt->{debug};
	if ($debug) {
		printf STDERR "Pulling RAID information\n";
	}
	my $lshw_output;
	if ($self->{inventory}->{lshw_output}) {
		$self->GetHardwareInfo;
	}
	$lshw_output = $self->{inventory}->{lshw_output};

	my $controllers = FindHashChild(
		hash => $lshw_output,
		key => 'class',
		value => 'storage'
	);

	my $disk_info = {};
	my $megaraid_count = 0;

	$disk_info->{adapters} = [];
	my $adapter;
	my $output;
	my $lsipcimap = {};
	foreach my $adp (@$controllers) {
		$adapter = {};
		push @{$disk_info->{adapters}}, $adapter;

		##
		## Deal with ATA and fake "controllers".  Fuck you, Linux
		##
		if (!exists($adp->{configuration}) ||
				!exists($adp->{configuration}->{driver})) {

			foreach my $disk (@{FindHashChild(
				hash => $adp,
				key => 'class',
				value => 'disk'
			)}) {
if ((!exists($disk->{logicalname})) || (ref($disk->{logicalname}) eq 'ARRAY')) {
printf STDERR "simple logicalname doesn't exist for disk $disk->{id}\n";
next;
}
				$output = _runcmd(
					errors => $opt->{errors},
					debug => $opt->{debug},
					command => $Programs->{udevadm},
					args => [ 'info', '-q', 'path', '-n', $disk->{logicalname} ]
					);

				if (!defined($output)) {
					if ($debug) {
						printf STDERR "udevadm for system disk failed\n";
					}
					return undef;
				}
				my ($pci_addr, $disk_addr) = (split('/', $output->[0]))[3,4];
				printf "PCI address: %s, disk address: %s\n", $pci_addr,
					$disk_addr;

				my $target_adp = (FindHashChild(
					hash => $lshw_output,
					key => 'businfo',
					value => 'pci@' . $pci_addr
				))->[0];

				next if (!$target_adp);

				$disk->{physid} = $disk_addr;

				if (!exists($target_adp->{children})) {
					$target_adp->{children} = [];
				}
				push @{$target_adp->{children}}, $disk;
				if (exists($target_adp->{os_disks})) {
					push @{$target_adp->{os_disks}}, $disk;
				}
			}
			delete($adp->{children});
			next;
		}

		if ($adp->{configuration}->{driver} eq 'megaraid_sas') {
			#
			# If we don't have a PCI mapping of the controllers yet, pull
			# that
			#

			if (!%$lsipcimap) {
				$output = _runcmd(
					errors => $opt->{errors},
					debug => $opt->{debug},
					tries => 50,
					command => $Programs->{MegaCli},
					args => [ '-AdpGetPCIInfo', '-aall', '-NoLog' ]
					);

				if (!defined($output)) {
					if ($debug) {
						printf STDERR "MegaCli -AdpGetPCIInfo failed\n";
					}
					return undef;
				}
				my $index;
				my $pciinfo = {};
				my $i;
				foreach my $line (@$output) {
					if (defined($index) && $line =~ /^\s*$/) {
						$lsipcimap->{sprintf("0000:%02s:%02s.%s",
							$pciinfo->{bus},
							$pciinfo->{device},
							$pciinfo->{function})
						} = $index;
						$pciinfo = {};
						$index = undef;
						next;
					}
					if (($i) =
						($line =~ /PCI information for Controller (\d+)/))
					{ 
						$index = $i;
						next;
					}
					my ($key, $value) = $line =~ /^(.*\S)\s*: (.*\S)\s*$/;
					next if (!$key);
					$pciinfo->{$lsi_pci_map->{$key}} = $value
						if exists $lsi_pci_map->{$key};
				}
			}

			#
			# Pull the underlying information for the OS-visible "disks"
			#
			$adapter->{pci_slot} = $adp->{businfo};
			$adapter->{pci_slot} =~ s/^pci\@//;
			$adapter->{adapter_number} = $lsipcimap->{$adapter->{pci_slot}};
			#
			# Get information about the adapter itself (model, serial number)
			#
			$output = _runcmd(
				errors => $opt->{errors},
				debug => $opt->{debug},
				tries => 50,
				command => $Programs->{MegaCli},
				args => [ '-AdpAllInfo', '-a' . $adapter->{adapter_number},
					'-NoLog' ]
				);

			if (!defined($output)) {
				if ($debug) {
					printf STDERR "MegaCli -AdpAllInfo  failed\n";
				}
				return undef;
			}

			foreach my $adp_info (@$output) {
				my ($key, $value) = $adp_info =~ /^(.*\S)\s*: (.*\S)\s*$/;
				next if (!$key);
				$adapter->{$adp_map->{$key}} = $value
					if exists $adp_map->{$key};
			};

			#
			# Get information about the physical disks attached to this
			# adapter
			#
			my $disks = [];
			$adapter->{adapter_disks} = $disks;
			$output = _runcmd(
				errors => $opt->{errors},
				debug => $opt->{debug},
				tries => 50,
				command => $Programs->{MegaCli},
				args => [ '-PDList', '-a' . $adapter->{adapter_number}, '-NoLog' ]
				);

			if (!defined($output)) {
				if ($debug) {
					printf STDERR "MegaCli -PDList for adapter %s failed\n",
						$adapter->{adapter_number};
				}
				return undef;
			}

			my $disk;
			foreach my $disk_info (@$output) {
				#
				# The fact that this is necessary is just amazing
				#
				$disk_info =~ s/^(.+)Hotspare Information.*/$1/;
				my ($key, $value) = $disk_info =~ /^([^:]+):\s(.*)/;
				next if (!$key);
				if ($key eq 'Enclosure Device ID') {
					$disk = {};
					push @$disks, $disk;
				}
				next if !defined($disk);
				$disk->{$drive_map->{$key}} = $value
					if exists $drive_map->{$key};
			};

			map {
				my ($size, $sectors) = $_->{disk_size} =~
					/([\d.]+ .B)\s+\[(\S+) Sectors\]/;
				if ($size) {
					$_->{disk_size} = $size;
					$_->{disk_sectors} = Math::BigInt->new($sectors)->bstr;
					$_->{size} = (Math::BigInt->new($sectors) * 512)->bstr;
				}

				if ($_->{inquiry}) {
					($_->{vendor}, $_->{product}, $_->{serial}) =
						unpack("A8A16A*", $_->{inquiry});
					#
					# Attempt to fix stupid broken inquiries.  I don't know
					# if this is LSI or Seagate to blame, but I have my
					# suspicions
					#
					if (!($_->{vendor})) {
						(undef, $_->{serial}, $_->{product}) =
							unpack("A12A8A16", $_->{inquiry});
						$_->{vendor} = 'Seagate';
						
					}
					$_->{model} = $_->{product};
				}

				$_->{physid} = join(':',
					($_->{enclosure_id} || ''),
					$_->{slot_number});
			} @$disks;

			#
			# Fuck you, LSI.  Get model and serial number information with
			# smartctl
			#

			#
			# We need to find the name of an OS disk on this adapter to
			# give to smartctl
			#
			my $osdisks = FindHashChild(
				hash => $adp,
				key => 'class',
				value => 'disk'
			);
			my $diskname;
			if (@$osdisks) {
				$diskname = $osdisks->[0]->{logicalname};
				foreach $disk (@$disks) {
					my $inq_type = 
						(defined($disk->{disk_proto}) && 
							$disk->{disk_proto} eq 'SAS') ? 
						'megaraid' : 
						'sat+megaraid';
					$output = _runcmd(
						errors => $opt->{errors},
						debug => $opt->{debug},
						tries => 50,
						command => $Programs->{smartctl},
						args => [ 
							'-i',
							'-d', $inq_type .',' .  $disk->{device_id},
							$diskname ]
						);
					if (!defined($output)) {
						next;
					}
					foreach my $disk_info (@$output) {
						my ($key, $value) = $disk_info =~ /^([^:]+):\s+(.*)/;
						next if (!$key);
						$disk->{$smartctl_map->{$key}} = $value
							if exists $smartctl_map->{$key};
					}
				}
			}

			#
			# Get information about the logical disks configured on this adapter
			#

			my $ldisks = [];
			$adapter->{logical_disks} = $ldisks;
			$output = _runcmd(
				errors => $opt->{errors},
				debug => $opt->{debug},
				tries => 50,
				command => $Programs->{MegaCli},
				args => [ '-LdPdInfo', '-a' . $adapter->{adapter_number},
					'-NoLog' ]
				);

			if (!defined($output)) {
				if ($debug) {
					printf STDERR "MegaCli -LdPdInfo for adapter %s failed\n",
						$adapter->{adapter_number};
				}
				return undef;
			}

			my $ldisk;
			foreach my $disk_info (@$output) {
				my ($key, $value) = $disk_info =~ /^([^:]+):\s+(.*)/;
				next if (!$key);
				$key =~ s/\s*$//;
				if ($key eq 'Virtual Drive') {
					if ($ldisk && %$ldisk) {
						push @$ldisks, $ldisk;
					}
					$ldisk = {};
					my ($vd, $target) = $value =~ /^(\d+) \(Target Id: (\d+)\)/;
					$ldisk->{volume_id} = $vd;
					$ldisk->{scsi_id} = $target;
					$ldisk->{physical_disks} = [];

					#
					# Some versions of MegaCli suck even harder than others
					# and only show the RAID information for the first
					# logical volume in -LdPdInfo
					#
					my $o = _runcmd(
						errors => $opt->{errors},
						debug => $opt->{debug},
						tries => 50,
						command => $Programs->{MegaCli},
						args => [ '-LdInfo', '-L' . $vd,
							'-a' . $adapter->{adapter_number},
							'-NoLog' ]
						);

					if (!defined($o)) {
						if ($debug) {
							printf STDERR "MegaCli -LdInfo for adapter %s, LD %s failed\n",
								$adapter->{adapter_number}, $vd;
						}
						return undef;
					}
					foreach my $line (@$o) {
						($key, $value) = $line =~ /^([^:]+):\s+(.*)/;
						next if (!$key);
						$key =~ s/\s*$//;
						
						if ($key eq 'Name') {
							$ldisk->{name} = $value;
							next;
						}
						if ($key eq 'RAID Level') {
							my ($primary, $secondary, $rlq) = $value =~
								/Primary-(\d+), Secondary-(\d+), RAID Level Qualifier-(\d+)/;
							$ldisk->{raid_level} = {
								primary => $primary,
								secondary => $secondary,
								raid_level_qualifier => $rlq,
							};
							next;
						}
					}
					next;
				}
				next if !defined($ldisk);

				if ($key eq 'PD') {
					$disk = {};
					push @{$ldisk->{physical_disks}}, $disk;
					next;
				}
				next if !defined($disk);

				if ($key eq 'Enclosure Device ID') {
					$disk->{enclosure_id} = $value;
					next;
				}
				if ($key eq 'Slot Number') {
					$disk->{slot_number} = $value;
					next;
				}

				if ($key =~ /Drive\'s posi?tion/) {
					my ($group, $span, $arm) = $value =~
						/DiskGroup: (\d+), Span: (\d+), Arm: (\d+)/;
					$disk->{drive_position} = {
						disk_group => $group,
						span => $span,
						arm => $arm,
					};
					next;
				}
			}
			if ($ldisk && %$ldisk) {
				push @$ldisks, $ldisk;
			}
			map {
				map {
					$_->{physid} = ($_->{enclosure_id} || '') . ':' .
						$_->{slot_number};
				} @{$_->{physical_disks}}
			} @{$ldisks};
		}
		#
		# Get the OS disks
		#

		my $osdisks = [ grep {
				$_->{class} eq 'disk' ||
				$_->{class} eq 'volume'
			} exists($adp->{children}) ? @{$adp->{children}} : ()
		];

		$adp->{os_disks} = $osdisks;

		foreach my $disk (@$osdisks) {
if ((!exists($disk->{logicalname})) || (ref($disk->{logicalname}) eq 'ARRAY')) {
printf STDERR "simple logicalname doesn't exist for disk $disk->{id}\n";
}
else {
			$output = _runcmd(
				errors => $opt->{errors},
				debug => $opt->{debug},
				command => $Programs->{smartctl},
				args => [ '-i', $disk->{logicalname} ]
				);
			if (!defined($output)) {
				if ($debug) {
					printf STDERR "smartctl failed for disk %s.  Using lshw\n",
						$disk->{logicalname};
				}

			} else {
				foreach my $disk_info (@$output) {
					my ($key, $value) = $disk_info =~ /^([^:]+):\s+(.*)/;
					next if (!$key);
					$disk->{$smartctl_map->{$key}} = $value
						if exists $smartctl_map->{$key};
				}
			}
}
		}
		#
		# Rename the 'children' key to 'partitions'
		#
		map { $_->{partitions} = delete $_->{children} } @$osdisks;
	}
	#
	# Get Linux LVM information
	#

	$output = _runcmd(
		errors => $opt->{errors},
		debug => $opt->{debug},
		command => $Programs->{pvs},
		args => [ qw( --separator : --units b --noheadings --nosuffix --options), 'name,vg_name,fmt,attr,size,uuid' ]
		);

	if (!defined($output)) {
		if ($debug) {
			printf STDERR "pvs command failed\n";
		}
		return undef;
	}

	#
	# Skip the rest if there are no physical volumes defined
	#
	if (@$output) {
		my $lvm = {};
		$disk_info->{linux_lvm} = $lvm;

		$lvm->{physical_volumes} = [
			map {
				s/^\s+//;
				my @col = split /:/, $_;
				{
					physical_volume => $col[0],
					volume_group => $col[1],
					type => $col[2],
					attributes => $col[3],
					size => $col[4],
					uuid => $col[5],
					units => 'bytes'
				};
			} @$output
		];

		$output = _runcmd(
			errors => $opt->{errors},
			debug => $opt->{debug},
			command => $Programs->{vgs},
			args => [ qw( --separator : --units b --noheadings --nosuffix --options), 'name,attr,size,uuid' ]
			);

		if (!defined($output)) {
			return undef;
		}

		$lvm->{volume_groups} = [
			map {
				s/^\s+//;
				my @col = split /:/, $_;
				{
					volume_group => $col[0],
					attributes => $col[1],
					size => $col[2],
					uuid => $col[3],
					units => 'bytes'
				};
			} @$output
		];

		$output = _runcmd(
			errors => $opt->{errors},
			debug => $opt->{debug},
			command => $Programs->{lvs},
			args => [ qw( --separator : --units b --noheadings --nosuffix --options), 'name,vg_name,attr,size,uuid,lv_kernel_major,lv_kernel_minor' ]
			);

		if (!defined($output)) {
			return undef;
		}

		$lvm->{logical_volumes} = [
			map {
				s/^\s+//;
				my @col = split /:/, $_;
				{
					logical_volume => $col[0],
					volume_group => $col[1],
					attributes => $col[2],
					size => $col[3],
					uuid => $col[4],
					kernel_major => $col[5],
					kernel_minor => $col[6],
					units => 'bytes',
				}
			} @$output
		];

		#
		# Attempt to map the mounted filesytems and swap spaces to their
		# logical volumes
		#
		
		my $fh;
		if (open ($fh, '<', '/proc/self/mountinfo')) {
			my $line;
			while ($line = readline($fh)) {
				my @crap = split /\s+/, $line;
				my ($xdev, $mountpoint) = @crap[2,4];

				#
				# Skip all of the "optional" fields.  Whoever came up
				# with the mountinfo file format needs to be eviscerated
				#
				my @rest = @crap[6..$#crap];
				my $vv;
				do {$vv = shift @rest} while ($vv and $vv ne '-');

				my ($fstype, $devname) = @rest[0,1];
				my ($maj, $min) = split /:/, $xdev;
				my $lv;
				if ($lv = (grep {
						$_->{kernel_major} == $maj &&
						$_->{kernel_minor} == $min
					} @{$lvm->{logical_volumes}})[0])
				{
					$lv->{filesystem_type} = $fstype;
					$lv->{mount_point} = $mountpoint;
				}
			}
		}

		if (open ($fh, '<', '/proc/swaps')) {
			my $line;
			while ($line = readline($fh)) {
				my ($filename, $type) =
					(split /\s+/, $line);
				next if !$type || $type ne 'partition' || !$filename ||
					(! -b $filename);
				my $st = stat($filename);
				next if !$st;
				my $lv;
				if ($lv = (grep {
						(($_->{kernel_major} << 8) | $_->{kernel_minor}) ==
							$st->rdev
					} @{$lvm->{logical_volumes}})[0])
				{
					$lv->{filesystem_type} = 'swap';
				}
			}
		}
	}

	$self->{inventory}->{disks} = $disk_info;
	1;
}

sub FindHashChild {
	my $opt = _options(@_);

	my $key = $opt->{key};
	my $value = $opt->{value};

	return if (!$opt->{hash} || !$key);

	my $found = [];
	my @searchlist = ($opt->{hash});

	while (@searchlist) {
		my $target = shift @searchlist;
		# push all children that are hashes or arrays onto the search list
		if (ref($target) eq 'ARRAY') {
			unshift @searchlist,
				(grep { ref($_) eq 'ARRAY' || ref($_) eq 'HASH' } @$target);
		} elsif (ref($target) eq 'HASH') {
			unshift @searchlist,
				(grep { ref($_) eq 'ARRAY' || ref($_) eq 'HASH' }
					values %$target);
			if (exists($target->{$key})) {
				if (!defined($value) || ($target->{$key} eq $value)) {
					push @$found, $target;
				}
			}
		}
	}
	return $found;
}

1;
