#!/usr/bin/env perl

use FindBin qw($RealBin);
use lib "$RealBin/modules";

use strict;
use warnings qw(all);

use POSIX;
use Getopt::Long;
use JSON;
use NetAddr::IP;
use FileHandle;

use LWP;

use Data::Dumper;

my $post_url = 'https://provisioning/provision';
my $timeout = 60;

my ($readfile, $writefile);
my $submit = 1;
my $interval = 60;
my $standalone = 0;
my $debug = 0;

eval {
	require _LocalHooks;
};

if (!(GetOptions(
	'standalone!'	=> \$standalone,
	'write=s'		=> \$writefile,
	'read=s'		=> \$readfile,
	'debug+'		=> \$debug,
	'submit!'		=> \$submit,
	'url=s'			=> \$post_url,
	'interval=i'	=> \$interval
	    defined(&_LocalHooks::local_getopts) ?
        _LocalHooks::local_getopts(
            $local_options
        )
    : ()
))) {
	exit 1;
}

if ($readfile && $writefile) {
	print STDERR "Only one of --read or --write may be specified\n";
	exit 1;
}

my @errors;

#
# First start up lldpd
#
if ( -x "/usr/sbin/lldpd" ) {
	system(qw(/usr/sbin/lldpd));
}
if ($standalone) {
	system (qw(/sbin/modprobe ipmi_devintf));
	#
	# Make sure LLDP works for i40e cards.  Hates Intel
	#
	my $fh;
	if (open($fh, '<', '/etc/mtab')) {
		my @mtab = <$fh>;
		close $fh;
		if (!grep m%^debugfs\s+/sys/kernel/debug\s+%, @mtab) {
			print "Mounting debugfs on /sys/kernel/debug\n";
			system('mount -t debugfs debugfs /sys/kernel/debug');
		}
		my $dirh;
		if (opendir($dirh, '/sys/kernel/debug/i40e')) {
			while (readdir $dirh) {
				next if /^\./;
				if (open($fh, '>', '/sys/kernel/debug/i40e/' . $_ . 
					'/command')) 
				{
					print $fh "lldp stop\n";
					close $fh;
				}
			}
			close $dirh;
		}
	}
}

my $inventory;
#
# Loop forever here until we get success back from the server
#
$inventory = DeviceInventory->new;
if ($readfile) {
	my $fh;
	if (!(open ($fh, '<', $readfile))) {
		printf STDERR "Unable to open %s: %s\n", $readfile, $!;
		exit 1;
	}
	local $/ = undef;
	my $stuff = <$fh>;
	close $fh;
	eval { $inventory->{inventory} = decode_json($stuff) };
	if (!defined($inventory->{inventory})) {
		printf STDERR "Could not read valid JSON from %s\n", $readfile;
		exit 1;
	}
} else {

	if ($debug) {
		printf STDERR "Getting OS Info\n";
	}
	if (!($inventory->GetOSInfo(
		debug => $debug,
		errors => \@errors
	))) {
		printf "%s\n", (join "\n", @errors);
		exit 1;
	}

	if ($debug) {
		printf STDERR "Doing basic system inventory\n";
	}
	if (!($inventory->GetBasicSystemInfo(
		debug => $debug,
		errors => \@errors
	))) {
		printf "%s\n", (join "\n", @errors);
		exit 1;
	}

	if ($debug) {
		printf STDERR "Doing PCI inventory\n";
	}
	if (!($inventory->GetPCIInfo(
		debug => $debug,
		errors => \@errors
	))) {
		printf "%s\n", (join "\n", @errors);
		exit 1;
	}

	if ($debug) {
		printf STDERR "Doing hardware inventory\n";
	}
	if (!($inventory->GetHardwareInfo(
		debug => $debug,
		errors => \@errors
	))) {
		printf "%s\n", (join "\n", @errors);
		exit 1;
	}

	if ($debug) {
		printf STDERR "Doing network interface inventory\n";
	}
	if (!($inventory->GetNetworkInfo(
		debug => $debug,
		lldp_method => 'lldpd',
		alter_interfaces => 1,
		errors => \@errors
	))) {
		printf "%s\n", (join "\n", @errors);
		exit 1;
	}

	if ($debug) {
		printf STDERR "Doing BMC inventory\n";
	}
	if (!($inventory->GetBMCInfo(
		debug => $debug,
		errors => \@errors
	))) {
		printf "%s\n", (join "\n", @errors);
		exit 1;
	}

	if ($debug) {
		printf STDERR "Doing disk inventory\n";
	}
	if (!defined($inventory->GetOSDiskInfo(
			debug => $debug,
			errors => \@errors
	))) {
		$inventory->{inventory}->{disk_inventory} = 'absent';
	}
}
if ($writefile) {
	my $fh;
	if (!(open ($fh, '>', $writefile))) {
		printf STDERR "Unable to open %s: %s\n", $writefile, $!;
		exit 1;
	}
	my $json_string;
	eval { $json_string = JSON::XS->new->pretty(1)->encode(
		$inventory->{inventory});
	};
	print $fh $json_string;
	close $fh;
}

#print encode_json($inventory->{inventory});

my $request = {
	api_version => '1.1',
	command => 'provision',
	inventory => $inventory->{inventory}
};

if (!$submit) {
	exit;
}

foreach my $interface 
		(@{$inventory->{inventory}->{network_interfaces}}) {

	#
	# Skip any interface which did not get LLDP stuff, because it's not
	# the right interface
	#
	printf "Checking interface %s\n", $interface->{interface_name};
	if (!$interface->{interface_name} || 
			!exists($interface->{lldp}->{device_name})) {
		printf "No LLDP information found for %s\n",
			$interface->{interface_name};
		next;
	}
	my $pidfile = "/var/run/dhclient-" . 
				${interface}->{interface_name} . ".pid";
	if (! -f $pidfile && !grep /inet /, 
			`/sbin/ip addr list dev $interface->{interface_name}`) {
		printf STDERR "Attempting to get DHCP address on interface %s... ",
			$interface->{interface_name};
		if (system("/sbin/dhclient -v -1 -pf " . $pidfile . " " .
				$interface->{interface_name})) {
			print STDERR "dhclient failed\n";
			next;
		}
		print "success\n";
		sleep 5;
		last;
	} else {
		printf STDERR "Interface %s already configured\n",
			$interface->{interface_name};
		last;
	}
}

while (1) {
	my $ua = LWP::UserAgent->new( ssl_opts => { 
		verify_hostname => 0 ,
		SSL_verify_mode => 'SSL_VERIFY_NONE',
	} );

	$ua->agent("server-inventory/1.0");
	$ua->timeout($timeout);
	my $header = HTTP::Headers->new();
	$header->header('Content-Type' => 'application/json');

	my $json_req;
	eval { $json_req = JSON::XS->new->pretty(1)->encode($request); };
	if (!$json_req) {
		print STDERR 'unable to encode JSON';
		next;
	}

	my $req = HTTP::Request->new(
		'POST',
		$post_url,
		$header,
		$json_req);

	my $res;
	eval {
		local $SIG{ALRM} = sub { die "timeout"; };
		alarm($timeout);
		$res = $ua->request($req);
		alarm(0);
	};
	if ($@ eq 'timeout') {
		print STDERR "connection timed out";
		next;
	}
	if (!$res) {
		print STDERR "Bad return from web server\n";
		next;
	}
	if (!$res->is_success) {
		printf STDERR "Error: %s\n", $res->status_line;
		next;
	}
	undef $ua;
	my $result;
	eval { $result = JSON::XS->new->decode($res->content) };
	if (!$result) {
		printf STDERR "Bad return from web server (non-JSON content): %s\n",
			$res->content;;
		next;
	}
	if ($result->{status} ne 'accept' ) {
		printf STDERR "Error returned from server: Status: %s, Message: %s\n",
			$result->{status},
			$result->{message};
		next;
	}
	
	print "Provisioning successful.\n";
	printf q{
Device ID:      %d
Device Name:    %s
Serial Number:  %s
Host ID:        %s
BMC Device ID:  %d
BMC IP Address: %s
},
		$result->{server_information}->{device_id},
		$result->{server_information}->{device_name},
		$result->{server_information}->{serial_number},
		$result->{server_information}->{UUID},
		$result->{server_information}->{bmc_device_id},
		$result->{server_information}->{bmc_ip_address};

	last;
} continue {
#
# For now, just exit and let the respawn start things all over again if
# we're running standalone
#
	sleep $interval;
	if ($standalone) {
		exit 1;
	}
}

if ($standalone) {
	if (defined(&_LocalHooks::SetUpBMC)) {
	_LocalHooks::SetUpBMC(
		inventory => $inventory,
		local_options => $local_options
	);

	print STDERR <<EOF;

Dropping to a shell to chill.

NOTE:   Exiting this shell may restart the provisioning process, which is
        harmless

EOF

	exec {'/bin/sh'} '-sh';
}
