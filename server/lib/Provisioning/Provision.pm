package Provisioning::Provision;
use strict;

use Apache2::RequestRec ();
use Apache2::RequestIO ();
use Apache2::RequestUtil ();
use Apache2::Log ();
use Apache2::Const -compile => qw(:common :log);
use APR::Const -compile => qw(:error SUCCESS);
use APR::Table;

use JazzHands::Common qw(:all);
use JazzHands::DBI;
use JazzHands::AppAuthAL;
use DBI;
use JSON;
use Net::Stomp;

use Data::Dumper;
use Provisioning::Common;
use Provisioning::InventoryCommon;

sub handler {
	#
	# Request handle
	#
	my $r = shift;

	#
	# Dry run can be requested by the client side to roll back the transaction
	#
	my $dryrun = 0;

	#
	# Dry run can be requested by the client side to roll back the transaction
	#
	my $debug = 0;
	#
	# Force doesn't currently do anything.  Maybe we should make it re-provision
	# the box
	#
	my $force = 0;

	my $authapp = 'provisioning';
	my $json_logdir = '/var/log/provisioning';

	$r->content_type('application/json');
	my $json = JSON->new;
	$json->allow_blessed(1);

	#
	# This is needed for Apache::DBI.  Sigh.
	#
	Apache2::RequestUtil->request($r);

	my $response = {
		status => undef
	};


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
	if (!defined($input)) {
		$response->{status} = 'error';
		$response->{message} = 'invalid JSON passed in POST request';
	} elsif (!defined($input->{command}) ) {
		$response->{status} = 'reject';
		$response->{message} = 'no command given';
	} elsif ($input->{command} ne 'provision') {
		$response->{status} = 'reject';
		$response->{message} = 'only "provision" command currently supported';
	} elsif (!defined($input->{inventory})) {
		$response->{status} = 'reject';
		$response->{message} = 'must send system inventory to provision';
	}

	if ($input->{debug}) {
		$debug = $input->{debug};
	}
	if ($input->{dryrun}) {
		$dryrun = $input->{dryrun};
	}
	my $ignore_lldp = 0;
	if ($input->{ignore_lldp}) {
		$ignore_lldp = $input->{ignore_lldp};
	}

	my $ignore_location = 0;
	if ($input->{ignore_location}) {
		$ignore_location = $input->{ignore_location};
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
			$client_ip));

	my @errors;
	my $jhdbi = JazzHands::DBI->new;
	my $dbh = $jhdbi->connect(
		application => $authapp,
		dbiflags => { AutoCommit => 0, PrintError => 0 },
		errors => \@errors
	);
	if (!$dbh) {
		$response->{status} = 'error';
		$response->{message} = 'error connecting to database';
		$r->print($json->encode($response));
		$r->log_error($response->{message} . ': ' . $DBI::errstr);
		return Apache2::Const::OK;
	};
	$jhdbi->set_session_user('provisioning');

	my $check;
	my $site_code;
	if (!($check = ValidateAddress(
			errors => \@errors,
			DBHandle => $dbh,
			ip_address => $client_ip))) {
		if (!defined($check)) {
			$response->{status} = 'error';
			$response->{message} = 'error validating address';
		} else {
			$response->{status} = 'reject';
			$response->{message} = 'IP address not authorized';
		}
		$r->print($json->encode($response));
		$r->log_error(join ("; ", @errors));
		return Apache2::Const::OK;
	}
	my $inventory = $input->{inventory};
	my $hostinfo = MassageInfo($inventory);
	$hostinfo->{site_code} = $check;

	#
	# We got the packet from the host and pulled out the information
	# that we care about for this phase, so dig through things and
	# see if we can insert it.  For the time being, the insert must
	# be completely clean or we log an error and send a reject
	#
	# That means all of the following must be true
	#
	#  - There must not be a device with the same mode/serial number
	#    combination (C6100s which share service tags also append the
	#    sled serial number)
	#  - There must not be a device which has the same MAC address for
	#    any probed interfaces in the database
	#  - The LLDP-probed switch must exist in the databasea
	#  - The LLDP-probed port must be empty
	#  - We must be able to tie this system to a system_configuration_id
	#  - There must not be a BMC with the same MAC address in the
	#    database

	my ($q, $sth);

	#
	# We could do this all with one query and see if any rows get returned
	# but it's easier to report the exact failure scenario if we 
	# do them individually.
	#
	if (!$hostinfo->{serial_number}) {
		$response->{status} = 'reject';
		$response->{message} = 'No serial number given for provisioned device';
		$r->log_error(sprintf('Rejecting provisioning request from %s: %s',
			$client_ip, $response->{message}));
		$r->print($json->encode($response));
		return Apache2::Const::OK;
	}

	$r->log_rerror(
		Apache2::Log::LOG_MARK, 
		Apache2::Const::LOG_NOTICE,
		APR::Const::SUCCESS, 
		sprintf("Processing request for a device with serial number %s",
			$hostinfo->{serial_number}));

	my $ret;

	my $lf = $hostinfo->{serial_number};
	$lf =~ s%/%-%g;
	if (open(my $lfh, ">", $json_logdir . "/initial/" . $lf)) {
		print $lfh $json->pretty->encode({
			hostinfo => $hostinfo,
			inventory => $input->{inventory}
		});
		close $lfh;
	} else {
		$r->log_error(sprintf('Unable to write JSON log for %s (%s): %s, putting it here: %s',
			$client_ip, $hostinfo->{serial_number}, $!,
			$response->{message}));
	}

	#
	# See if there's a matching serial
	#

	if (!defined($ret = runquery(
		description => 'validating device serial number',
		request => $r,
		dbh => $dbh,
		query => q {
			SELECT
				device_id, device_name, physical_label, device_status
			FROM
				jazzhands.device d JOIN
				jazzhands.asset a USING (component_id)
			WHERE
				a.serial_number = ?
		},
		args => [ $hostinfo->{serial_number} ]
	))) {
		return Apache2::Const::OK;
	}

	if (@$ret) {
		#
		# If the device with this serial was previously removed, then
		# remove the component linkage and create a new device
		#
		if ($ret->[3] eq 'removed') {
			if (!defined(runquery(
				description =>
					sprintf('removing component linkage for device %d',
						$ret->[0]),
				request => $r,
				dbh => $dbh,
				query => q {
					UPDATE
						jazzhands.device
					SET
						component_id = NULL
					WHERE
						device_id = ?
				},
				args => [ $ret->[0] ]
			))) {
				return Apache2::Const::OK;
			}
			$force = 1;
		} else {
			$hostinfo->{device_id} = $ret->[0];
			$hostinfo->{device_name} = $ret->[1];
			if (!defined($ret = runquery(
				description => 'fetch BMC info',
				request => $r,
				dbh => $dbh,
				query => q {
					SELECT
						b.device_id, ip_address
					FROM
						jazzhands.device d JOIN
						jazzhands.device_management_controller dmc USING
							(device_id) JOIN
						device b ON (dmc.manager_device_id = b.device_id AND
							dmc.device_mgmt_control_type = 'bmc') JOIN
						network_interface_netblock nin ON 
							(b.device_id = nin.device_id) JOIN
						netblock n ON (nin.netblock_id = n.netblock_id)
					WHERE
						d.device_id = ?
				},
				args => [ $hostinfo->{device_id} ]
			))) {
				return Apache2::Const::OK;
			}

			$hostinfo->{bmc_device_id} = $ret->[0];
			$hostinfo->{bmc_ip_address} = $ret->[1];

			$response->{status} = 'accept';
			$response->{server_information} = $hostinfo;
			$response->{message} = sprintf(
				'Device id %d (%s) already has serial %s assigned',
				$hostinfo->{device_id}, $hostinfo->{device_name},
				$hostinfo->{serial_number});
			$r->log_error(sprintf('Rejecting provisioning request from %s (but sending accept): %s',
				$client_ip, $response->{message}));
			$r->print($json->encode($response));
			return Apache2::Const::OK;
		}
	}

	foreach my $iface (@{$hostinfo->{network_interfaces}}) {
		#
		# See if there's a matching MAC
		#
		if (!defined($ret = runquery(
			description => 'validating device MAC address',
			request => $r,
			dbh => $dbh,
			query => q {
				SELECT
					device_id, device_name, physical_label,
					network_interface_name
				FROM
					jazzhands.device d JOIN
					jazzhands.network_interface ni USING (device_id)
				WHERE
					mac_addr = ?
			},
			args => [ $iface->{mac_address} ]
		))) {
			return Apache2::Const::OK;
		}

		if (@$ret) {
			$response->{status} = 'reject';
			$response->{message} = sprintf(
				'Device id %d (%s) already has MAC %s assigned to interface %s',
				$ret->[0], $ret->[2], $iface->{mac_address}, $ret->[3]);
			$r->log_error(sprintf('Rejecting provisioning request from %s: %s',
				$client_ip, $response->{message}));
			$r->print($json->encode($response));
			return Apache2::Const::OK;
		}
		#
		# Check the switch information if it's connected to something
		#
		if (exists($iface->{lldp}) && $iface->{lldp}->{device_name}) {
			if (!defined($ret = runquery(
				description => 'validating switch existence',
				request => $r,
				dbh => $dbh,
				query => q {
					SELECT
						device_id, site_code, rack_id
					FROM
						jazzhands.device d LEFT JOIN
						jazzhands.rack_location USING (rack_location_id)
					WHERE
						device_name = ?
				},
				args => [ $iface->{lldp}->{device_name} ]
			))) {
				return Apache2::Const::OK;
			}

			if (!@$ret) {
				$response->{status} = 'reject';
				$response->{message} = sprintf(
					'Switch %s does not exist in the database',
					$iface->{lldp}->{device_name});
				$r->log_error(sprintf(
					'Rejecting provisioning request from %s: %s',
					$client_ip, $response->{message}));
				$r->print($json->encode($response));
				return Apache2::Const::OK;
			}

			#
			# Stash the site and rack id for later
			#
			if (!$hostinfo->{rack_id}) {
				$hostinfo->{site_code} = $ret->[1];
				$hostinfo->{rack_id} = $ret->[2];
				($hostinfo->{location_port}) = 
					$iface->{lldp}->{interface} =~ /(\d+)$/;
				my $dt = $iface->{lldp}->{device_type};
				if ($dt =~ /Juniper Networks/) {
					$hostinfo->{switch_type} = 'juniper';
				} elsif ($dt =~ /Arista Networks/) {
					$hostinfo->{switch_type} = 'arista';
				} else {
					$hostinfo->{switch_type} = $dt;
				}
			}

			#
			# Make sure the port exists and is not connected to anything
			#

			my $switch_dev_id = $ret->[0];

			if (!defined($ret = runquery(
				description => 'validating port existence',
				request => $r,
				dbh => $dbh,
				query => q {
					SELECT 
						port_name, other_port_name, other_device_id,
						coalesce(device_name, physical_label),
						physical_port_id
					FROM
						v_l1_all_physical_ports pp LEFT JOIN
						device d ON (pp.other_device_id = d.device_id)
					WHERE 
						port_type = 'network' AND
						pp.device_id = ? AND
						pp.port_name = ?
				},
				args => [ $switch_dev_id, $iface->{lldp}->{interface} ]
			))) {
				return Apache2::Const::OK;
			}
			if (!@$ret) {
				$response->{status} = 'reject';
				$response->{message} = sprintf(
					'Switch port %s does not exist in the database on %s',
					$iface->{lldp}->{interface},
					$iface->{lldp}->{device_name}
					);
				$r->log_error(sprintf(
					'Rejecting provisioning request from %s: %s',
					$client_ip, $response->{message}));
				$r->print($json->encode($response));
				return Apache2::Const::OK;
			}
			if ($ret->[3]) {
				$response->{status} = 'reject';
				$response->{message} = sprintf(
					'Port %s on %s is already connected to port %s on %s',
					$iface->{lldp}->{interface},
					$iface->{lldp}->{device_name},
					$ret->[1], ($ret->[3] || ('device ' || $ret->[2]))
					);
				$r->log_error(sprintf(
					'Rejecting provisioning request from %s: %s',
					$client_ip, $response->{message}));
				$r->print($json->encode($response));
				return Apache2::Const::OK;
			}
			$iface->{other_physical_port_id} = $ret->[4];
		}

	}

	#
	# See if there's a matching BMC MAC
	#
	if (!defined($ret = runquery(
		description => 'validating device MAC address',
		request => $r,
		dbh => $dbh,
		query => q {
			SELECT
				device_id, device_name, physical_label,
				network_interface_name
			FROM
				jazzhands.device d JOIN
				jazzhands.network_interface ni USING (device_id)
			WHERE
				mac_addr = ?
		},
		args => [ $hostinfo->{bmc_mac} ]
	))) {
		return Apache2::Const::OK;
	}

	if (@$ret) {
		$response->{status} = 'reject';
		$response->{message} = sprintf(
			'Device id %d (%s) already has MAC %s assigned to interface %s',
			$ret->[0], $ret->[2], $hostinfo->{bmc_mac}, $ret->[3]);
		$r->log_error(sprintf('Rejecting provisioning request from %s: %s',
			$client_ip, $response->{message}));
		$r->print($json->encode($response));
		return Apache2::Const::OK;
	}
	
	#
	# Determine the system_configuration_id and device_type
	#

	if (!defined($ret = runquery(
		description => 'determining server configuration',
		request => $r,
		dbh => $dbh,
		query => q {
			SELECT
				device_type_id,
				COALESCE(dt.description, dt.model),
				server_configuration_id,
				device_prefix
			FROM
				jazzhands.device_type dt JOIN
				jazzhands.property p USING (company_id) JOIN
				device_provisioning.device_type_to_server_config USING (device_type_id)
			WHERE
				p.property_name = 'DeviceVendorProbeString' AND
				p.property_type = 'DeviceProvisioning' AND
				p.property_value = ? AND
				dt.model = ? AND
				max_memory >= ? AND
				max_disks >= ?
			ORDER BY
				max_disks,
				max_memory
		},
		args => [ 
			$hostinfo->{vendor},
			$hostinfo->{model},
			$hostinfo->{memory},
			$hostinfo->{numdisks}
		]
	))) {
		return Apache2::Const::OK;
	}

	if (!@$ret) {
		if (!defined($ret = runquery(
			description => 'inserting new server configuration',
			request => $r,
			dbh => $dbh,
			query => q {
				SELECT
					*
				FROM
					device_provisioning.insert_server_device_type(
						company_name := ?,
						model := ?
					)
			},
			args => [ 
				$hostinfo->{vendor},
				$hostinfo->{model}
			]
		))) {
			return Apache2::Const::OK;
		}

		if (!@$ret) {
			$response->{status} = 'reject';
			$response->{message} = sprintf(
				'device type or server configuration not found for vendor %s, model %s, memory %d, disks %d',
				$hostinfo->{vendor},
				$hostinfo->{model},
				$hostinfo->{memory},
				$hostinfo->{numdisks});
			$r->log_error(sprintf('Rejecting provisioning request from %s: %s',
				$client_ip, $response->{message}));
			$r->print($json->encode($response));
			return Apache2::Const::OK;
		}
		$hostinfo->{device_type_id} = $ret->[1];
		$hostinfo->{server_configuration_id} = $ret->[2];
		$hostinfo->{prefix} = $ret->[3];
	} else {
		$hostinfo->{device_type_id} = $ret->[0];
		$hostinfo->{model} = $ret->[1];
		$hostinfo->{server_configuration_id} = $ret->[2];
		$hostinfo->{prefix} = $ret->[3];
	}

	if (!$hostinfo->{site_code}) {
		$response->{status} = 'reject';
		$response->{message} = "Unable to determine site code";
		$r->log_error(sprintf('Rejecting provisioning request from %s: %s',
			$client_ip, $response->{message}));
		$r->print($json->encode($response));
		return Apache2::Const::OK;
	}

	if (!$hostinfo->{rack_id}) {
# Should make this a property as to whether to reject or not for unknown rack
#		$response->{status} = 'reject';
#		$response->{message} = "Unable to determine rack_id";
		$r->log_error(sprintf('Unable to determine rack_id provisioning request from %s',
			$client_ip));
	}

	#
	# Things check out okay.  We can insert ourselves
	#

	if (!defined($ret = runquery(
		description => 'fetching device name template',
		request => $r,
		dbh => $dbh,
		return_type => 'hashref',
		query => q {
			SELECT
				property_value
			FROM
				jazzhands.property
			WHERE
				p.property_name = 'DeviceNameTemplate' AND
				p.property_type = 'DeviceProvisioning'
		},
	))) {
		return Apache2::Const::OK;
	}
	my $devname_template;
	if (!%$ret) {
		# if this isn't set, just get enough to get it into the database
		$devname_template = '%{serial}.%{model}';
	} else {
		$devname_template = $ret->{property_value};
	}

	if (!defined($ret = runquery(
		description => 'fetching BMC name template',
		request => $r,
		dbh => $dbh,
		return_type => 'hashref',
		query => q {
			SELECT
				property_value
			FROM
				jazzhands.property
			WHERE
				p.property_name = 'BMCNameTemplate' AND
				p.property_type = 'DeviceProvisioning'
		},
	))) {
		return Apache2::Const::OK;
	}
	my $bmcname_template;
	if (!%$ret) {
		# if this isn't set, just get enough to get it into the database
		$bmcname_template = '%{serial}.%{model}';
	} else {
		$bmcname_template = $ret->{property_value};
	}

	#
	# Create a hostname
	#

	my $hostname = DoNameTranslation(
		hostinfo => $hostinfo,
		template => '%{serial}.%{translated_model}'
	);

	if ($hostname eq "") {
		$response->{status} = 'reject';
		$response->{message} = "No valid characters to create hostname";
		$r->log_error(sprintf('Rejecting provisioning request from %s: %s',
			$client_ip, $response->{message}));
		$r->print($json->encode($response));
		return Apache2::Const::OK;
	}

	$hostinfo->{device_name} = $hostname;

	#
	# Make a good guess at mapping the switch port to the rack U, otherwise
	# just insert it into the rack
	#
	
	if (!defined($ret = runquery(
		description => 'determining rack location',
		request => $r,
		dbh => $dbh,
		query => q {
			SELECT
				rack_u
			FROM
				device_provisioning.device_type_rack_location
			WHERE
				device_type_id = ? AND
				switch_type = ? AND
				port = ? AND
				(site_code = ? OR site_code IS NULL)
			ORDER BY
				site_code
			LIMIT 1
		},
		args => [
			$hostinfo->{device_type_id},
			$hostinfo->{switch_type},
			$hostinfo->{location_port},
			$hostinfo->{site_code}
		]
	))) {
	} else {
		if (!@$ret) {
			$r->warn(sprintf('Unable to determine rack u location for device type %d, switch type %s, port %d, site %s in request from %s for serial %s',
				$hostinfo->{device_type_id},
				$hostinfo->{switch_type},
				$hostinfo->{location_port},
				$hostinfo->{site_code}, 
				$client_ip,
				$hostinfo->{serial_number}));
			$hostinfo->{rack_u} = undef;
		} else {
			$hostinfo->{rack_u} = $ret->[0];
		}
	}

	#
	# Insert the device
	#

	$r->log_rerror(
		Apache2::Log::LOG_MARK, 
		Apache2::Const::LOG_INFO,
		APR::Const::SUCCESS, 
		sprintf("Attempting to insert a %s %s server (device type %d) with serial %s, device name %s, BMC MAC %s",
			$hostinfo->{vendor},
			$hostinfo->{model},
			$hostinfo->{device_type_id},
			$hostinfo->{serial_number},
			$hostinfo->{device_name},
			$hostinfo->{bmc_mac}));

	$ret = undef;
	if ($hostinfo->{rack_id}) {
		if (!defined($ret = runquery(
			description => 'performing server insertion',
			request => $r,
			dbh => $dbh,
			query => q {
				SELECT
					device_id,
					bmc_device_id,
					bmc_ip_address
				FROM
					device_provisioning.insert_server(
						device_name := ?,
						device_type_id := ?,
						server_configuration_id := ?,
						serial_number := ?,
						host_id := ?,
						rack_id := ?,
						rack_u := ?,
						bmc_mac := ?)
			},
			args => [
				$hostinfo->{device_name},
				$hostinfo->{device_type_id},
				$hostinfo->{server_configuration_id},
				$hostinfo->{serial_number},
				$hostinfo->{UUID},
				$hostinfo->{rack_id},
				$hostinfo->{rack_u},
				$hostinfo->{bmc_mac}
			]
		))) {
			return Apache2::Const::OK;
		}
	} elsif ($ignore_location) {
		if (!defined($ret = runquery(
			description => 'performing server insertion',
			request => $r,
			dbh => $dbh,
			query => q {
				SELECT
					device_id,
					bmc_device_id,
					bmc_ip_address
				FROM
					device_provisioning.insert_server_device(
						device_name := ?,
						device_type_id := ?,
						site_code := ?,
						serial_number := ?,
						host_id := ?,
						bmc_mac := ?)
			},
			args => [
				$hostinfo->{device_name},
				$hostinfo->{device_type_id},
				$hostinfo->{site_code},
				$hostinfo->{serial_number},
				$hostinfo->{UUID},
				$hostinfo->{bmc_mac}
			]
		))) {
			return Apache2::Const::OK;
		}
	} else {
		$response->{status} = 'error';
		$response->{message} = "No location information given";
		$r->log_error(sprintf('Rejecting provisioning request from %s: %s',
		   $client_ip, $response->{message}));
		$r->print($json->encode($response));
		$dbh->rollback;
		return Apache2::Const::OK;
	}
	if (!@$ret) {
		$response->{status} = 'error';
		$response->{message} = "Server/BMC insert seems to have failed";
		$r->log_error(sprintf('Rejecting provisioning request from %s: %s',
			$client_ip, $response->{message}));
		$r->print($json->encode($response));
		$dbh->rollback;
		return Apache2::Const::OK;
	}
	$hostinfo->{device_id} = $ret->[0];
	$hostinfo->{bmc_device_id} = $ret->[1];
	$hostinfo->{bmc_ip_address} = $ret->[2];

	# The primary interface will be something with valid LLDP information

	my $primary_iface;
	my $primary_iface_name = '';

	if (!(grep {
				exists($_->{lldp}) && $_->{lldp}->{device_name} &&
				$_->{interface_name} =~ /^eth\d+/
			} @{$hostinfo->{network_interfaces}})) {
		$primary_iface =
			(grep { (exists($_->{lldp}) && $_->{lldp}->{device_name}) }
			@{$hostinfo->{network_interfaces}})[0];

		if (defined($primary_iface)) {
			($primary_iface_name) = $primary_iface->{interface_name}
				=~ /^(.*\D)\d+$/;
		}
	}

	my $primary_iface_idx = 0;
	foreach my $iface (
		sort { $a->{interface_name} cmp $b->{interface_name} }
			grep { $_->{interface_name} } @{$hostinfo->{network_interfaces}} 
	) {
		my $iface_name = $iface->{interface_name};
		next if (!$iface_name);

		if ($primary_iface_name && 
				$iface_name =~ /^${primary_iface_name}\d+$/) {
			$iface_name = 'eth' . $primary_iface_idx;
			$primary_iface_idx++;
		}
		if (!defined($ret = runquery(
			description => 'performing network_interface insertion',
			request => $r,
			dbh => $dbh,
			query => q {
				SELECT
					network_interface_id,
					physical_port_id
				FROM
					device_provisioning.insert_network_interface(
						device_id := ?,
						network_interface_name := ?,
						network_interface_type := 'broadcast',
						mac_addr := ?,
						create_physical_port := true,
						remote_physical_port_id := ?)
			},
			args => [
				$hostinfo->{device_id},
				$iface_name,
				$iface->{mac_address},
				$iface->{other_physical_port_id}
			]
		))) {
			return Apache2::Const::OK;
		}
		if (!@$ret) {
			$response->{status} = 'error';
			$response->{message} = "network_interface insert seems to have failed";
			$r->log_error(sprintf('Rejecting provisioning request from %s: %s',
				$client_ip, $response->{message}));
			$r->print($json->encode($response));
			$dbh->rollback;
			return Apache2::Const::OK;
		}
		$iface->{network_interface_id} = $ret->[0];
		$iface->{physical_port_id} = $ret->[1];
	}

	#
	# We're provisioned ourselves, now run the inventory parts
	#
	
	if (!defined($ret = runquery(
		description => 'fetching provisioned device from database',
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
					netblock n USING (netblock_id)
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
				asset a USING (component_id) LEFT JOIN
				ip USING (device_id) LEFT JOIN
				device_type dt USING (device_type_id)
			WHERE
				device_id = ?
		},
		args => [
			$hostinfo->{device_id},
		]
	))) {
		return Apache2::Const::OK;
	}

	if (!(@$ret)) {
		$response->{status} = 'reject';
		$response->{message} = 'Inserted device not found (this should not ever happen)',
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
		$dbh->do('SAVEPOINT storage');
		if (!defined(Provisioning::InventoryCommon::RAIDInventory(
			request => $r,
			usererror => $usererror,
			dbh => $dbh,
			inventory => $inventory,
			device => $device,
			debug => $debug,
			force => $force
		))) {
#			$response->{status} = 'error';
#			$response->{message} = join(',', @$usererror);

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_ERR,
				APR::Const::SUCCESS,
				"error processing RAID data.  Rolling back RAID and aborting storage processing"
				);
			$dbh->do('ROLLBACK TO SAVEPOINT storage');
			goto STORAGEDONE;
		}

		$dbh->do('SAVEPOINT storage');
		if (!defined(Provisioning::InventoryCommon::OSDiskInventory(
			request => $r,
			usererror => $usererror,
			dbh => $dbh,
			inventory => $inventory,
			device => $device,
			debug => $debug,
			force => $force
		))) {
#			$response->{status} = 'error';
#			$response->{message} = join(',', @$usererror);

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_ERR,
				APR::Const::SUCCESS,
				"error processing OS partition data.  Rolling back and aborting storage processing"
				);
			$dbh->do('ROLLBACK TO SAVEPOINT storage');
			goto STORAGEDONE;
		}

		$dbh->do('SAVEPOINT storage');
		if (!defined(Provisioning::InventoryCommon::LVMInventory(
			request => $r,
			usererror => $usererror,
			dbh => $dbh,
			inventory => $inventory,
			device => $device,
			debug => $debug,
			force => $force
		))) {
#			$response->{status} = 'error';
#			$response->{message} = join(',', @$usererror);
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_ERR,
				APR::Const::SUCCESS,
				"error processing LVM data.  Rolling back LVM data"
				);
			$dbh->do('ROLLBACK TO SAVEPOINT storage');
			goto STORAGEDONE;
		}
	}
STORAGEDONE:
	$response->{status} = 'accept';
	$response->{server_information} = $hostinfo;

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
	} elsif ($dryrun) {
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

	$r->print($json->encode($response));

	$r->log_rerror(
		Apache2::Log::LOG_MARK, 
		Apache2::Const::LOG_NOTICE,
		APR::Const::SUCCESS, 
		sprintf("Request from %s %s a %s %s server with id %d, serial %s, device name %s, BMC address %s",
			$client_ip,
			$response->{status} eq 'accept' ?
				'provisioned' :
				'failed to provision',
			$hostinfo->{vendor},
			$hostinfo->{model},
			$hostinfo->{device_id},
			$hostinfo->{serial_number},
			$hostinfo->{device_name},
			$hostinfo->{bmc_ip_address}));

	if (open(my $lfh, ">", $json_logdir . "/" . $lf)) {
		print $lfh $json->pretty->encode({
			hostinfo => $hostinfo,
			inventory => $input->{inventory}
		});
		close $lfh;
	} else {
		$r->log_error(sprintf('Unable to write JSON log for %s (%s): %s, putting it here: %s',
			$client_ip, $hostinfo->{serial_number}, $!,
			$response->{message}));
	}

	my $authinfo = JazzHands::AppAuthAL::find_and_parse_auth($authapp);
	if (!$authinfo || !$authinfo->{dhcp_notifier}) {
		$r->log_error(sprintf(
			"Unable to find dhcp_notifier auth entry for %s\n",
			$authapp));
	}
	my $dhcp = $authinfo->{dhcp_notifier};
	if ($dhcp) {
		my $stomp;
		eval {
			$stomp = new Net::Stomp(
				{
					hostname => $dhcp->{Host},
					port => $dhcp->{Port}
				}
			);
			$stomp->connect( 
				{
					login => $dhcp->{Username},
					passcode => $dhcp->{Password}
				}
			);
			$stomp->send(
				{
					destination => $dhcp->{Topic},
					body => 'rebuild'
				}
			);
		};
		if ($@) {
			$r->log_error(sprintf(
				"Error notifying DHCP STOMP handler for rebuild: %s",
				$@));
		} else {
			$r->log_rerror(
				Apache2::Log::LOG_MARK, 
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS, 
				sprintf("Notified DHCP handler on %s",
					$dhcp->{Host})
			);
		}
		if ($stomp) {
			eval { $stomp->disconnect };
		}
	}
	return Apache2::Const::OK;
}

sub ValidateAddress {
	my $opt = &_options(@_);

	if (!$opt->{DBHandle} || !$opt->{ip_address}) {
		SetError($opt->{errors},
			"Invalid parameters passed to ValidateAddress");
		return undef;
	}

	my $dbh = $opt->{DBHandle};

	my ($q, $sth, $result);
	$q = q {
		SELECT
			n.ip_address,
			site_code
		FROM
			jazzhands.netblock_collection nc JOIN
			jazzhands.v_nblk_coll_netblock_expanded USING
				(netblock_collection_id) JOIN
			jazzhands.netblock n USING (netblock_id) LEFT JOIN
			v_site_netblock_expanded sne USING (netblock_id)
		WHERE
			netblock_collection_type = 'application-permissions' AND
			netblock_collection_name = 'DeviceProvisioning' AND
			n.ip_address >>= ?
	};
	if (!($sth = $dbh->prepare_cached($q))) {
		SetError($opt->{errors},
			sprintf(
				"Error preparing query to check IP address permissions: %s",
				$dbh->errstr
			));
		return undef;
	}

	if (!($sth->execute($opt->{ip_address}))) {
		SetError($opt->{errors},
			sprintf(
				"Error executing query to check IP address permissions: %s",
				$sth->errstr
			));
		return undef;
	}
	my $ret = $sth->fetch;
	$sth->finish;
	if (defined($ret)) {
		return $ret->[1];
	} else {
		SetError($opt->{errors}, sprintf("IP Address %s not authorized",
			$opt->{ip_address}));
		return 0;
	}
}

sub DoNameTranslation {
	my $opt = &_options(@_);

	if (!$opt->{template}) {
		SetError($opt->{errors},
			"template not passed to DoNameTranslation")
		return undef;
	}

	if (!$opt->{hostinfo} || !(ref($hostinfo))) {
		SetError($opt->{errors},
			"hostinfo not passed to DoNameTranslation")
		return undef;
	}
	
	my $hostname = $opt->{template};
	$hostname =~ s/%{model}/$hostinfo->{model}/g;
	$hostname =~ s/%{serial}/$hostinfo->{serial_number}/g;
	$hostname =~ s/%{translated_model}/$hostinfo->{prefix}/g;
	$hostname =~ s/%{site}/$hostinfo->{site_code}/g;
	$hostname =~ s%/%-%g;
	$hostname =~ s/[^0-9A-Za-z-]//g;
	return $hostname;
}
1;
