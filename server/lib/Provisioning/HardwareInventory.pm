package Provisioning::HardwareInventory;
use strict;

use Apache2::RequestRec ();
use Apache2::RequestIO ();
use Apache2::RequestUtil ();
use Apache2::Connection ();
use Apache2::Log ();
use Apache2::Const -compile => qw(:common :log);
use APR::Table;
use APR::Const -compile => qw(:error SUCCESS);

use JazzHands::Common qw(:all);
use JazzHands::DBI;
use JazzHands::AppAuthAL;
use DBI;
use JSON;
use Provisioning::Common;
use Provisioning::InventoryCommon;

use Data::Dumper;

sub handler {
	#
	# Request handle
	#
	my $r = shift;
	
	#
	# Admin is set if the remote side authenticates with a principal that
	# is associated with an administrator
	#
	my $admin = 0;

	#
	# force is set by the remote side if the inventory is supposed to be
	# reset.  Requires admin access.
	#
	my $force = 0;

	#
	# force_lvm is set by the remote side if the LVM pieces should be 
	# rebuilt unconditionally.  Does not require admin access.
	#
	my $force_lvm = 0;

	#
	# Debug can be requested by the client side to spew lots of things
	#
	my $debug = 0;

	#
	# Dry run can be requested by the client side to roll back the transaction
	#
	my $dryrun = 0;

	my $authapp = 'provisioning';
	my $json_logdir = '/var/log/inventory';

	$r->content_type('application/json');
	my $json = JSON->new;
	$json->allow_blessed(1);

	#
	# This is needed for Apache::DBI
	#
	Apache2::RequestUtil->request($r);

	my $response = {
		status => undef
	};

	###
	### Validate the request
	###

	my $client_ip;
	eval {
		$client_ip = $r->connection->client_ip;
	};
	if (!$client_ip) {
		eval {
			$client_ip = $r->connection->remote_ip;
		};
	}
	if ($r->method ne 'POST') {
		$response->{status} = 'error',
		$response->{message} = 'must present JSON data in a POST request';
		$r->print($json->encode($response));
		$r->log_error('not a POST request');
		return Apache2::Const::OK;
	}

	my $headers = $r->headers_in;
	$r->subprocess_env;

	my $json_data;
	$r->read($json_data, $r->headers_in->{'Content-Length'});

	my $input;
	eval { $input = $json->decode($json_data) };
	if ($input->{debug}) {
		$debug = $input->{debug};
	}
	if ($input->{dryrun}) {
		$dryrun = $input->{dryrun};
	}
	if (!defined($input)) {
		$response->{status} = 'error';
		$response->{message} = 'invalid JSON passed in POST request';
	} elsif (!defined($input->{command}) ) {
		$response->{status} = 'reject';
		$response->{message} = 'no command given';
	} elsif ($input->{command} ne 'inventory') {
		$response->{status} = 'reject';
		$response->{message} = 'only "inventory" command currently supported';
	} elsif (!defined($input->{inventory})) {
		$response->{status} = 'reject';
		$response->{message} = 'must send system inventory to provision';
	}
		
	if (defined($response->{status})) {
		$r->print($json->encode($response));
		$r->log_error($response->{message}, ' in request from ',
			$client_ip);
		return Apache2::Const::OK;
	}

	$r->log_rerror(
		Apache2::Log::LOG_MARK,
		Apache2::Const::LOG_NOTICE,
		APR::Const::SUCCESS,
		sprintf("Received %s request from %s",
			$input->{command},
			$client_ip
		)
	);

	my @errors;
	my $jhdbi = JazzHands::DBI->new;
	my $dbh = $jhdbi->connect_cached(
		application => $authapp,
		dbiflags => { AutoCommit => 0, PrintError => 0 },
		errors => \@errors
	);
	if (!$dbh) {
		$response->{status} = 'error';
		$response->{message} = 'error connecting to database';
		$r->print($json->encode($response));
		$r->log_error($response->{message} . ': ' . $JazzHands::DBI::errstr);
		return Apache2::Const::OK;
	};

	$jhdbi->set_session_user('inventory');

	$dbh->do('set constraints all deferred');
#	$dbh->do('set client_min_messages = debug');

	###
	### Attempt to determine which device the request belongs to.
	###
	### What the query below does is to take the following identifying
	### information out of the request and attempt to find a device that
	### matches.  If zero devices or more than one device are matched,
	### then an error is logged and the update is aborted.
	###
	### This is a bit more difficult for a C6100 chassis, because all nodes
	### will report the same serial number and UUID, so for those
	### we just have to assume that the hostname and/or IP address
	### are good enough.
	###

	my ($q, $sth);
	my $ret;

	my $inventory = $input->{inventory};
	my $hostinfo = MassageInfo($inventory);
	my $user = $ENV{REMOTE_USER} || '';
	my $hostname;
	if ($user =~ m%^host/%) {
		$hostname = $user =~ s%host/(.*)@.*%$1%r;
		if ($input->{force} || $input->{force_lvm}) {
			$force_lvm = 1;
		}
	} else {

		###
		### If the request was not authenticated with a host principal,
		### then the pricipal must be a valid admin user.
		###

		$hostname = '';
		$user =~ s/@.*$//;
		$jhdbi->set_session_user('inventory/' . $user);

		if ($debug) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf(
					'Validating inventory user: %s',
					$user
				)
			);
		}
		if (!defined($ret = runquery(
			description => 'validating inventory user',
			request => $r,
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			query => q {
				SELECT
					login
				FROM
					property p JOIN
					account_collection ac ON
						(p.property_value_account_coll_id =
						ac.account_collection_id) JOIN
					v_acct_coll_acct_expanded acae ON
						(ac.account_collection_id =
						acae.account_collection_id) JOIN
					account a ON
						(acae.account_id = a.account_id)
					WHERE
						(property_name, property_type) =
							('AdminAccountCollection', 'DeviceInventory') AND
						login = ?
			},
			args => [
				$user
			]
		))) {
			return Apache2::Const::OK;
		}

		if (@$ret) {
			if ($debug) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_DEBUG,
					APR::Const::SUCCESS,
					sprintf(
						'User %s is authorized to run inventory',
						$user
					)
				);
			}
			$admin = 1;
		} elsif ($client_ip eq '127.0.0.1' ||
				$client_ip eq '::1') {
			###
			### There should probably be a config override to allow localhost
			### to be an administrator.  Just saying.
			###
			$admin = 1;
			$user = 'local';
		} else {
			$response->{status} = 'reject';
			$response->{message} =
				sprintf('User %s is not allowed to run inventory',
					$user);
			$r->log_error(
				sprintf(
					'Rejecting %s request from %s: %s',
					$input->{command},
					$client_ip,
					$response->{message}
				)
			);
			$r->print($json->encode($response));
			return Apache2::Const::OK;
		}

		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			sprintf(
				'%s request from admin user %s for %s',
				$input->{command},
				$user,
				$client_ip
			)
		);
		if ($admin) {
			if ($input->{hostname}) {
				$hostname = $input->{hostname};
			}
			if ($input->{force}) {
				$force = 1;
				$force_lvm = 1;
			}
			if ($input->{force_lvm}) {
				$force_lvm = 1;
			}
		} else {
			if ($input->{force} || $input->{force_lvm}) {
				$force_lvm = 1;
			}
		}
	}

	###
	### Fetch device and component information from the database
	###
	$hostinfo->{hostname} = $hostname;

	if (!defined($ret = runquery(
		description => 'fetching requested device from database',
		request => $r,
		debug => $debug,
		dbh => $dbh,
		allrows => 1,
		query => q {
			WITH ip AS (
				SELECT
					device_id, array_agg(host(ip_address)) as ip_addresses
				FROM
					device d JOIN
					network_interface_netblock nin USING (device_id) JOIN
					netblock n ON (nin.netblock_id = n.netblock_id)
				GROUP BY
					device_id
				)
			SELECT
				device_id,
				d.component_id,
				asset_id,
				device_name,
				physical_label,
				ip_addresses,
				serial_number,
				host_id,
				device_type_id
			FROM
				device d JOIN
				device_type dt USING (device_type_id) JOIN
				asset a USING (component_id) LEFT JOIN
				ip USING (device_id)
			WHERE
				device_name = ? OR
				physical_label = ? OR
				? = ANY (ip_addresses) OR
				(CASE WHEN
					model IN ('C6100', 'Server') THEN false
				 ELSE
					serial_number = ? OR
					host_id = ?
				END)
		},
		args => [
			$hostinfo->{hostname},
			$hostinfo->{hostname},
			$client_ip,
			$hostinfo->{serial_number},
			$hostinfo->{UUID}
		]
	))) {
		return Apache2::Const::OK;
	}

	if (!(@$ret)) {
		$response->{status} = 'reject';
		$response->{message} = 'No devices found matching inventory request.  Device may have been deleted or needs to be provisioned';
		$r->log_error(
			sprintf(
				'Rejecting %s request from hostname %s, IP Address %s, serial %s, UUID %s:  %s',
				$input->{command},
				$hostinfo->{hostname},
				$client_ip,
				$hostinfo->{serial_number},
				$hostinfo->{UUID},
				$response->{message}
			)
		);
		$r->print($json->encode($response));
		return Apache2::Const::OK;
	}

	if ($#$ret > 0) {
		$response->{status} = 'reject';
		$response->{message} = 'Multiple devices found matching inventory request.';
		$r->log_error(
			sprintf(
				'Rejecting %s request from hostname %s, IP Address %s, serial %s, UUID %s:  %s',
				$input->{command},
				$hostinfo->{hostname},
				$client_ip,
				$hostinfo->{serial_number},
				$hostinfo->{UUID},
				$response->{message}
			)
		);
		$r->print($json->encode($response));
		return Apache2::Const::OK;
	}


	my $device = {
		device_id => $ret->[0]->[0],
		component_id => $ret->[0]->[1],
		asset_id => $ret->[0]->[2],
		device_name => $ret->[0]->[3],
		physical_label => $ret->[0]->[4],
		ip_addresses => $ret->[0]->[5],
		serial_number => $ret->[0]->[6],
		host_id => $ret->[0]->[7],
		device_type_id => $ret->[0]->[8]
	};

	my $fn = $device->{serial_number} . '.json';
	if (open(my $lfh, ">", $json_logdir . '/' . $fn)) {
		print $lfh $json->pretty->encode({
			hostinfo => $hostinfo,
			inventory => $json_data
		});
		close $lfh;
	} else {
		$r->log_error(sprintf('Unable to write JSON log for %s (%s): %s, putting it here: %s',
			$ENV{REMOTE_ADDR}, $hostinfo->{serial_number}, $!,
			$response->{message}));
	}

	foreach my $label ( 
		($device->{device_name} ~~ $device->{physical_label}) ?
			($device->{device_name}) :
			($device->{device_name}, $device->{physical_label})
	) {
		next if (!$label);
		my $linkname = $json_logdir . '/' . $label . '.json';
		if (-l $linkname) {
			unlink $linkname;
		}
		symlink($fn, $linkname);
	}

	$r->log_rerror(
		Apache2::Log::LOG_MARK,
		Apache2::Const::LOG_INFO,
		APR::Const::SUCCESS,
		sprintf(
			'Processing %s request for %s (device_id %d)',
			$input->{command},
			(join '/', (
				map { defined($_) ? $_ : () } (
					$device->{physical_label},
				    $device->{device_name})
				)),
			$device->{device_id}
		)
	);

	my $usererror = [];

	if (!defined(Provisioning::InventoryCommon::ChassisInventory(
		request => $r,
		usererror => $usererror,
		dbh => $dbh,
		inventory => $inventory,
		hostinfo => $hostinfo,
		device => $device,
		debug => $debug,
		force => $force
	))) {
		$response->{status} = 'error';
		$response->{message} = join(',', @$usererror);
		goto DONE;
	}

	if (!defined(Provisioning::InventoryCommon::CPUInventory(
		request => $r,
		usererror => $usererror,
		dbh => $dbh,
		inventory => $inventory,
		device => $device,
		debug => $debug,
		force => $force
	))) {
		$response->{status} = 'error';
		$response->{message} = join(',', @$usererror);
		goto DONE;
	}

	if (!defined(Provisioning::InventoryCommon::MemoryInventory(
		request => $r,
		usererror => $usererror,
		dbh => $dbh,
		inventory => $inventory,
		device => $device,
		debug => $debug,
		force => $force
	))) {
		$response->{status} = 'error';
		$response->{message} = join(',', @$usererror);
		goto DONE;
	}

	if ($inventory->{disk_inventory} ~~ 'absent') {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			sprintf('Skipping disk inventory for %s due to client failure',
				(join '/', (
					map { defined($_) ? $_ : () } (
						$device->{physical_label},
						$device->{device_name})
					))));
	} else {
		if (!defined(Provisioning::InventoryCommon::RAIDInventory(
			request => $r,
			usererror => $usererror,
			dbh => $dbh,
			inventory => $inventory,
			device => $device,
			debug => $debug,
			force => $force
		))) {
			$response->{status} = 'error';
			$response->{message} = join(',', @$usererror);
			goto DONE;
		}

		if (!defined(Provisioning::InventoryCommon::OSDiskInventory(
			request => $r,
			usererror => $usererror,
			dbh => $dbh,
			inventory => $inventory,
			device => $device,
			debug => $debug,
			force => $force
		))) {
			$response->{status} = 'error';
			$response->{message} = join(',', @$usererror);
			goto DONE;
		}

		if ($force_lvm) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS,
				sprintf('Force LVM rebuild requested for %s',
					(join '/', (
						map { defined($_) ? $_ : () } (
							$device->{physical_label},
							$device->{device_name})
						))
				)
			);
		}

		if (!defined(Provisioning::InventoryCommon::LVMInventory(
			request => $r,
			usererror => $usererror,
			dbh => $dbh,
			inventory => $inventory,
			device => $device,
			debug => $debug,
			force => $force_lvm
		))) {
			$response->{status} = 'error';
			$response->{message} = join(',', @$usererror);
			goto DONE;
		}
	}
	$response->{status} = 'accept';

	#
	# Validate that this port is connected to where the database thinks it
	# is.  Best effort, don't error if this fails (but it will get logged)
	# Failure doesn't leave the database handle invalidated, so we can
	# just fire and forget
	#
	foreach my $iface (@{$hostinfo->{network_interfaces}}) {
		if (exists($iface->{lldp}) && $iface->{lldp}->{chassis_id}) {
			Provisioning::InventoryCommon::SetLLDPConnection(
				request => $r,
				usererror => $usererror,
				dbh => $dbh,
				device_id => $device->{device_id},
				slot_name => $iface->{interface_name},
				remote_host_id => $iface->{lldp}->{chassis_id},
				remote_slot_name => $iface->{lldp}->{interface},
				debug => $debug
			);
		}
	}
DONE:

	if (!$response->{status} || $response->{status} ne 'accept') {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_ERR,
			APR::Const::SUCCESS,
			"Rolling back transaction on error"
			);
		if (!($dbh->rollback)) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_ERR,
				APR::Const::SUCCESS,
				sprintf("Error rolling back transaction: %s", $dbh->errstr)
				);
		}
	}
	if ($dryrun) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			"Dry run requested.  Rolling back database transaction"
			);
		if (!($dbh->rollback)) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_ERR,
				APR::Const::SUCCESS,
				sprintf("Error rolling back transaction: %s", $dbh->errstr)
				);
			$response->{status} = 'error';
			$response->{message} = sprintf(
				'Error committing database transaction.  See server log for transaction %d for details',
					$r->connection->id
			);
		}
	} else {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_DEBUG,
			APR::Const::SUCCESS,
			sprintf("Committing database transaction for Apache request %d",
				$r->connection->id
			));
		if (!($dbh->commit)) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_ERR,
				APR::Const::SUCCESS,
				sprintf("Error committing transaction: %s", $dbh->errstr)
				);
			$response->{status} = 'error';
			$response->{message} = 'Error committing database transaction';
		}
	}

	$dbh->disconnect;

	$r->log_rerror(
		Apache2::Log::LOG_MARK,
		Apache2::Const::LOG_INFO,
		APR::Const::SUCCESS,
		sprintf(
			'Finished %s request for %s (device_id %d)',
			$input->{command},
			(join '/', (
				map { defined($_) ? $_ : () } (
					$device->{physical_label},
				    $device->{device_name})
				)),
			$device->{device_id}
		)
	);

	$r->print($json->encode($response));

	return Apache2::Const::OK;
}

1;

