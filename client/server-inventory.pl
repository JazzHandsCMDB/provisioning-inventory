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

my $API_VERSION = '1.0';

my $post_url = "https://provisioning/inventory";
my $config_file = "/etc/jazzhands/server-inventory/config.json";
my $timeout = 60;

my ($readfile, $writefile);
my $submit = 1;
my $interval = 60;
my $count = 0;
my $maxtries = 10;
my $backoff = 30;
my $force = 0;
my $force_lvm = 0;
my $debug = 0;
my $dryrun = 0;
my $local_options = {};
my $delay = 0;
my $command = 'inventory';
my $hostname = undef;
my $username = undef;
my $passwd = undef;
my $lldp_method = undef;
my $probe_bmc = 1;
my $pidfile = '/var/run/server-inventory.pid';
my $i_wrote_the_pid_file = 0;

use DeviceInventory;

eval {
	require _LocalHooks;
};


if (!(GetOptions(
	'random-delay=i'	=> \$delay,
	'write=s'			=> \$writefile,
	'read=s'			=> \$readfile,
	'config=s'			=> \$config_file,
	'command=s'			=> \$command,
	'submit!'			=> \$submit,
	'force-lvm!'		=> \$force_lvm,
	'force-all!'		=> \$force,
	'debug+'			=> \$debug,
	'dryrun|n!'			=> \$dryrun,
	'url=s'				=> \$post_url,
	'hostname=s'		=> \$hostname,
	'username=s'		=> \$username,
	'password=s'		=> \$passwd,
	'pidfile=s'			=> \$pidfile,
	'interval=i'		=>\$interval,
	'max-tries=i' 		=> \$maxtries,
	'backoff=i'			=> \$backoff,
	'lldp_method|lldp-method=s'		=> \$lldp_method,
	'bmc!'				=> \$probe_bmc,
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

my $inventory;
$inventory = DeviceInventory->new;

my $config;
if ($config_file && -f $config_file) {
	my $fh;
	if (!(open ($fh, '<', $config_file))) {
		printf STDERR "Unable to open configuration file %s: %s\n",
			$config_file, $!;
		exit 1;
	}
	local $/ = undef;
	my $stuff = <$fh>;
	close $fh;
	eval { $config = decode_json($stuff) };
	if (!defined($config)) {
		printf STDERR "Could not read valid JSON from %s\n", $config_file;
		exit 1;
	}
}

if ($lldp_method) {
	$config->{lldp_method} = $lldp_method;
}

if (defined(&_LocalHooks::prerun)) {
	_LocalHooks::prerun(
		inventory => $inventory,
		local_options => $local_options
	);	
}

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
	if ($delay) {
		my $pfh;
		if (-f $pidfile) {
			if (!(open ($pfh, '<', $pidfile))) {
				printf STDERR "Unable to pid file %s: %s\n", $pidfile, $!;
			} else {
				my $pid = <$pfh>;
				chomp ($pid);
				close $pfh;
				if ($pid && $pid =~ /^\d+$/) {
					system("ps $pid >/dev/null 2>&1");
					if (!($? >> 8)) {
						if ($debug) {
							printf STDERR "Exiting because of currently running system-inventory process %s\n",
								$pid;
						}
						exit 1;
					}
				}
			}
			unlink ($pidfile);
		}
		if ($debug) {
			print STDERR "Ok to run\n";
		}
	}

	if (!-f $pidfile) {
		my $pfh;
		if (!(open ($pfh, '>', $pidfile))) {
			printf STDERR "Unable to open pid file %s for writing: %s\n",
				$pidfile, $!;
			exit 1;
		} else {
			printf $pfh "%s\n", $$;
		}
		close $pfh;
		$i_wrote_the_pid_file = 1;
	}
	
	if ($delay) {
		my $sleep_time = int(rand($delay));
		if ($debug) {
			printf STDERR "Sleeping for %d seconds\n", $sleep_time;
		}
		sleep($sleep_time);
	}

	if ($debug) {
		printf STDERR "Getting OS Information\n";
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
		lldp_method => (
			defined($config->{lldp_method}) ? 
				$config->{lldp_method} : 
				'lldptool'
		),
		errors => \@errors
	))) {
		printf "%s\n", (join "\n", @errors);
		exit 1;
	}

	if ($debug) {
		printf STDERR "Doing BMC inventory\n";
	}
	if ($probe_bmc) {
		if (!($inventory->GetBMCInfo(
			debug => $debug,
			errors => \@errors
		))) {
			printf "%s\n", (join "\n", @errors);
			exit 1;
		}
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
	eval { 
		$json_string = JSON->new->pretty(1)->encode(
			$inventory->{inventory});
	};
	print $fh $json_string;
	close $fh;
}

if (!$submit) {
	exit;
}

my $request = {
	api_version => $API_VERSION,
	command => $command,
	inventory => $inventory->{inventory},
	debug => $debug,
	dryrun => $dryrun
};

if ($force) {
	$request->{force} = 1;
}

if ($force_lvm) {
	$request->{force_lvm} = 1;
}

if ($hostname) {
	$request->{hostname} = $hostname;
}

#
# Retry this a number of times with a backoff to attempt to submit it.
# If things fail at the end, then 
#
while ($count < $maxtries) { 
	my $ua = LWP::UserAgent->new( ssl_opts => { 
		verify_hostname => 0 ,
		SSL_verify_mode => 0
	} );

	$ua->agent("server-inventory/1.0");
	$ua->timeout($timeout);
	my $header = HTTP::Headers->new();
	$header->header('Content-Type' => 'application/json');

	my $json_req;
	eval { $json_req = JSON->new->pretty(1)->encode($request); };
	if (!$json_req) {
		print STDERR 'unable to encode JSON';
		next;
	}

	if ($debug) {
		printf STDERR "Sending JSON request to %s\n", $post_url;
	}
	my $req = HTTP::Request->new(
		'POST',
		$post_url,
		$header,
		$json_req);

	if ($passwd) {
		$req->authorization_basic($username, $passwd);
	}
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
	eval { $result = JSON->new->decode($res->content) };
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
	
	last;
} continue {
	$count += 1;
	if ($count < $maxtries) {
		sleep($interval);
		$interval += $backoff;
	}
}

END {
	if (defined(&_LocalHooks::postrun)) {
		_LocalHooks::postrun(
			inventory => $inventory,
			local_options => $local_options
		);
	}
	if ($i_wrote_the_pid_file) {
		unlink ($pidfile);
	}
}
