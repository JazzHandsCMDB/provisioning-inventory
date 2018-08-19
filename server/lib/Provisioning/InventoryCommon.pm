package Provisioning::InventoryCommon;
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
use DBI;
use Provisioning::Common;

use Data::Dumper;

sub RAIDInventory {
	my $opt = &_options(@_);
	my $r = $opt->{request};
	my $debug = $opt->{debug};
	my $dbh = $opt->{dbh};
	my $force = $opt->{force};
	my $inventory = $opt->{inventory};
	my $device = $opt->{device};

	#
	# Defer constraints until later, because we play fast and loose with
	# moving some things around
	#

	$dbh->do('SET CONSTRAINTS ALL DEFERRED');

	###
	### Pull storage adapters for this client out of the database
	### Adapters will be directly attached to the top-level component
	###

	my $ret;
	if (!defined($ret = runquery(
		description => 'fetching storage controllers',
		request => $r,
		usererror => $opt->{usererror},
		debug => $debug,
		dbh => $dbh,
		allrows => 1,
		return_type => 'hashref',
		query => q {
			WITH x AS (
				SELECT
					component_type_id,
					array_agg(component_function) as functions
				FROM
					component_type_component_func
				GROUP BY
					component_type_id
			) SELECT
				c.component_id,
				s.slot_id,
				asset_id,
				serial_number,
				slot_name,
				model,
				vid.property_value AS pci_vendor_id,
				did.property_value AS pci_device_id,
				svid.property_value AS pci_subsystem_vendor_id,
				svid.property_value AS pci_subsystem_id
			FROM
				component c JOIN
				component_type ct USING (component_type_id) JOIN
				x USING (component_type_id) JOIN
				slot s ON (c.parent_slot_id = s.slot_id) LEFT JOIN
				asset a ON (c.component_id = a.component_id) LEFT JOIN
				component_property vid ON (
					vid.component_property_name = 'PCIVendorID' AND
					vid.component_property_type = 'PCI' AND
					vid.component_type_id = ct.component_type_id ) LEFT JOIN
				component_property did ON (
					did.component_property_name = 'PCIDeviceID' AND
					did.component_property_type = 'PCI' AND
					did.component_type_id = ct.component_type_id ) LEFT JOIN
				component_property svid ON (
					svid.component_property_name = 'PCISubsystemVendorID' AND
					svid.component_property_type = 'PCI' AND
					svid.component_type_id = ct.component_type_id ) LEFT JOIN
				component_property sid ON (
					sid.component_property_name = 'PCISubsystemID' AND
					sid.component_property_type = 'PCI' AND
					sid.component_type_id = ct.component_type_id )
			WHERE
				s.component_id = ? AND
				'PCI' = ANY(functions) AND
				'storage' = ANY(functions)
		},
		args => [
			$device->{component_id}
		]
	))) {
		return Apache2::Const::OK;
	}

	my $adapters;
#	$adapters->{probed} = $inventory->{disks}->{adapters};
	$adapters->{probed} = FindHashChild(
		hash => $inventory,
		key => 'class',
		value => 'storage'
	);
	$adapters->{database} = $ret;

	###
	### Determine if any adapters have been completely removed from the system.
	### Removed adapters will have all of the dependent disks and logical
	### volume information removed.
	###
	### Note: adapters which have moved slots will be detected as "removed",
	### but they should be rebuilt appropriately with the 'force' flag.
	###
	foreach my $db_adp (@{$adapters->{database}}) {
		if ($debug) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf(
					'Looking for storage adapter in slot %s from database in inventory request',
					$db_adp->{slot_name}
				)
			);
		}

		my $target = FindHashChild(
			hash => $adapters->{probed},
			key => "businfo",
			value => 'pci@' . $db_adp->{slot_name}
		);
		my $probed_adp;
		if (@$target) {
			$probed_adp = $target->[0];
		} else {
			#
			# This adapter seems to have disappeared
			#
			if (!$force) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_NOTICE,
					APR::Const::SUCCESS,
					sprintf(
						'Storage adapter in PCI slot %s for %s (device_id %d) seems to have disappeared',
						$db_adp->{slot_name},
						$device->{device_name},
						$device->{device_id}
					)
				);
			} else {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf(
						'Removing adapter from slot %s in the database (not present in inventory)',
						$db_adp->{slot_name}
					)
				);
				#
				# If force is run, clean out everything associated with this
				# controller
				#
				if (!defined(CleanStorageComponentInfo(
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						component => $db_adp))) {
					return undef;
				}
			}
			next;
		}
	}

	###
	### Detect added/replaced adapters
	###

	foreach my $adapter (@{$adapters->{probed}}) {
		#
		# The adapter will always have a PCI slot; this gets rid of bad
		# requests that can get sent from the client
		#
		next if (!$adapter->{businfo});

		my ($pci_slot) = $adapter->{businfo} =~ /pci@(.*)/;
		my $target = FindHashChild(
			hash => $adapters->{database},
			key => "slot_name",
			value => $pci_slot
		);
		my $db_adp;
		#
		# There can only be one here
		#
		if (@$target) {
			$db_adp = $target->[0];

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf('Found adapter for PCI slot %s in the database as component_id %d',
					$pci_slot,
					$db_adp->{component_id}
				));
		}
		#
		# If either the adapter does not exist in the database, or the
		# model (i.e. the tuple of PCI vendor, device, subsystem vendor,
		# and subsystem) or serial number has changed, then fetch a replacement
		# component
		#
		my $pci_dev = $inventory->{PCI}->{$pci_slot};
		next if !$pci_dev;

		my $megaraid_info = FindHashChild(
			hash => $inventory->{disks}->{adapters},
			key => 'pci_slot',
			value => $pci_slot
		)->[0];

		if (
			!$db_adp ||
			(
				hex($pci_dev->{vendor}->{id}),
				hex($pci_dev->{device}->{id}),
				defined($pci_dev->{subsystem_vendor}->{id}) ?
					hex($pci_dev->{subsystem_vendor}->{id}) :
					undef,
				defined($pci_dev->{subsystem_device}->{id}) ?
					hex($pci_dev->{subsystem_device}->{id}) :
					undef
			) ~~
			(
				defined($db_adp->{pci_vendor_id}) ?
					hex($db_adp->{pci_vendor_id}) :
					undef,
				defined($db_adp->{pci_device_id}) ?
					hex($db_adp->{pci_device_id}) :
					undef,
				defined($db_adp->{pci_subsystem_vendor_id}) ?
					hex($db_adp->{pci_subsystem_vendor_id}) :
					undef,
				defined($db_adp->{pci_subsystem_id}) ?
					hex($db_adp->{pci_subsystem_id}) :
					undef,
			) ||
			!($db_adp->{serial_number} ~~ $megaraid_info->{serial_number})
		) {

			my $model_name =
				(!$pci_dev->{subsystem_device}->{name} ||
					$pci_dev->{subsystem_device}->{name} eq 'Device') ?
					$pci_dev->{device}->{name} :
					$pci_dev->{subsystem_device}->{name};

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf(q{Inventoried adapter does not match database:
					DB Model:      %s
					PCI Device:    %s
					DB Serial:     %s
					Probed Serial: %s
				},
					$db_adp ? $db_adp->{model} || '' : '',
					$model_name || '',
					$db_adp ? $db_adp->{serial_number} || '' : '',
					$adapter->{serial_number} || '',
				));
			
			#
			# If we're replacing the adapter, clean out all of the old
			# crap.  We'll build all of the LV stuff up from scratch just
			# to keep everything clean.
			#
			if ($db_adp) {
				if (!defined(CleanStorageComponentInfo(
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						component => $db_adp))) {
					return undef;
				}
			}
			#
			# Fetch a component for the new adapter.  We have all of the
			# information that we need to insert the component_type if it
			# doesn't exist, so pass everything to the function to create
			# things
			#

			my $args =
					[
						hex($pci_dev->{vendor}->{id}),
						hex($pci_dev->{device}->{id}),
						defined($pci_dev->{subsystem_vendor}->{id}) ?
							hex($pci_dev->{subsystem_vendor}->{id}) :
							undef,
						defined($pci_dev->{subsystem_device}->{id}) ?
							hex($pci_dev->{subsystem_device}->{id}) :
							undef,
						$pci_dev->{vendor}->{name},
						$pci_dev->{device}->{name},
						$pci_dev->{subsystem_vendor}->{name},
						$pci_dev->{subsystem_device}->{name},
						$megaraid_info->{serial_number},
						['storage']
					];

			
			if (!defined($db_adp = runquery(
					description => 'inserting adapter component',
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						SELECT * FROM
							component_utils.insert_pci_component(
								pci_vendor_id := ?,
								pci_device_id := ?,
								pci_sub_vendor_id := ?,
								pci_subsystem_id := ?,
								pci_vendor_name := ?,
								pci_device_name := ?,
								pci_sub_vendor_name := ?,
								pci_sub_device_name := ?,
								serial_number := ?,
								component_function_list := ?
							)
					},
					args => $args
			))) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_ERR,
					APR::Const::SUCCESS,
					sprintf("Error inserting adapter: %s", 
						Dumper($pci_dev)
					));
				next;
			}

			if ($db_adp->{parent_slot_id}) {
				if (!$force) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_ERR,
						APR::Const::SUCCESS,
						sprintf("Adapter component_id %s for storage adapter %s %s in slot %s is already attached to slot_id %d",
							$db_adp->{component_id},
							$pci_dev->{vendor}->{name},
							$pci_dev->{device}->{name},
							$pci_slot,
							$db_adp->{parent_slot_id}
						));
					return undef;
				}
				if (!defined(CleanStorageComponentInfo(
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						component => $db_adp))) {
					return undef;
				}
			}

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS,
				sprintf("Retrieved component_id %s for new storage adapter %s %s in slot %s",
					$db_adp->{component_id},
					$pci_dev->{vendor}->{name},
					$pci_dev->{device}->{name},
					$pci_slot
				));
			my $slot;
			if (!defined($slot = runquery(
					description => 'attaching adapter component to parent slot',
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						SELECT * FROM
							component_utils.insert_component_into_parent_slot(
								parent_component_id := ?,
								component_id := ?,
								slot_name := ?,
								slot_function := 'PCI'
							)
					},
					args => [
						$device->{component_id},
						$db_adp->{component_id},
						$pci_slot
					]
			))) {
				return undef;
			}
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf("Attached component_id %d to slot_id %d of component %d",
					$db_adp->{component_id},
					$slot->{slot_id},
					$device->{component_id}
				));
		} else {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf('Probed adapter in PCI slot %s matches database',
					$pci_slot,
					$db_adp->{component_id}
				));
		}

		###
		### Inventory the disks attached to this adapter
		###

		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_DEBUG,
			APR::Const::SUCCESS,
			sprintf('Performing inventory of disks attached to adapter in PCI slot %s',
				$pci_slot
			));

		my $db_disks;
		if (!defined($db_disks = runquery(
			description => sprintf(
				'fetching disks for storage controller %s',
				$db_adp->{component_id}),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			query => q {
				WITH x AS (
					SELECT
						component_type_id,
						array_agg(component_function) as functions
					FROM
						component_type_component_func
					GROUP BY
						component_type_id
				) SELECT
					c.component_id,
					slot_id,
					asset_id,
					serial_number,
					slot_name,
					model
				FROM
					component c JOIN
					component_type ct USING (component_type_id) JOIN
					x USING (component_type_id) JOIN
					slot s ON (c.parent_slot_id = s.slot_id) LEFT JOIN
					asset a ON (c.component_id = a.component_id)
				WHERE
					s.component_id = ? AND
					'disk' = ANY(functions) AND
					'storage' = ANY(functions)
			},
			args => [
				$db_adp->{component_id}
			]
		))) {
			return undef;
		}

		###
		### Determine if any disks have been removed
		###

		my $disklist;
		if ($adapter->{configuration}->{driver} eq 'megaraid_sas') {
			$disklist = $megaraid_info->{adapter_disks};
		} else {
			$disklist = $adapter->{os_disks};
		}

		my $disklist;
		if ($adapter->{configuration}->{driver} eq 'megaraid_sas') {
			$disklist = $megaraid_info->{adapter_disks};
		} else {
			$disklist = $adapter->{os_disks};
		}

		foreach my $db_disk (@$db_disks) {
			
			my $target = [ grep
					{ ($_->{businfo} || $_->{physid}) eq $db_disk->{slot_name} }
					@{$disklist}
				];
			if (@$target && $#$target > 0) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_ERR,
					APR::Const::SUCCESS,
					sprintf("Multiple disks probed in slot %s of storage adapter in PCI slot %s of device_id %d.  This is definitely wrong",
						$db_disk->{slot_name},
						$db_adp->{slot_name},
						$device->{device_id}
					));
				next;
			} elsif (!@$target) {
				#
				# No disk was probed for this slot
				#
				if (!$force) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_NOTICE,
						APR::Const::SUCCESS,
						sprintf(
							'Disk in slot %s of storage adapter in PCI slot %s of device_id %d seems to have been removed',
							$db_disk->{slot_name},
							$db_adp->{slot_name},
							$device->{device_id}
						)
					);
				} else {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_INFO,
						APR::Const::SUCCESS,
						sprintf(
							'Removing disk %d from slot %s in the database (not present in inventory)',
							$db_disk->{component_id},
							$db_disk->{slot_name}
						)
					);
					if (!defined(CleanStorageComponentInfo(
							request => $r,
							usererror => $opt->{usererror},
							debug => $debug,
							dbh => $dbh,
							component => $db_disk))) {
						return undef;
					}
				}
			}
		}

		###
		### Determine if any disks have been added or replaced
		###

		foreach my $probed_disk (@$disklist) {
			my $slot_name = $probed_disk->{businfo} || $probed_disk->{physid};
			my $target = [ grep
					{ $_->{slot_name} eq $slot_name }
					@$db_disks
				];
			if (!($probed_disk->{size})) {
				next;
			}
			my $db_disk;
			if (@$target && $#$target == 0) {
				$db_disk = $target->[0];
				$probed_disk->{component_id} = $db_disk->{component_id};
			} elsif ($#$target > 0) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_ERR,
					APR::Const::SUCCESS,
					sprintf("Multiple disks found in slot %s of storage adapter in PCI slot %s of device_id %d in the database.  This is definitely wrong",
						$slot_name,
						$db_adp->{slot_name},
						$device->{device_id}
					));
				next;
			}

			if (
					!$db_disk ||
					($probed_disk->{product} ne $db_disk->{model}) ||
					!($probed_disk->{serial} ~~
						$db_disk->{serial_number})
					) {
				#
				# If the model is the same, but the serial number changed,
				# then just swap out the disk silently after we
				# allocate the new one
				#
				my $swapped_disk;
				if ($db_disk) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_DEBUG,
						APR::Const::SUCCESS,
						sprintf(q{Inventoried disk does not match database:
							DB Model:      %s
							Probed Model:  %s
							DB Serial:     %s
							Probed Serial: %s
						},
							$db_disk ? $db_disk->{model} || '' : '',
							$probed_disk->{product} || '',
							$db_disk ? $db_disk->{serial_number} || '' : '',
							$probed_disk->{serial} || '',
						));
					
					if ($probed_disk->{product} eq $db_disk->{model}) {
						#
						# Things don't need to be cleaned up here,
						# since we'll take care of transferring things
						# below
						#
						$swapped_disk = $db_disk;
					} else {
						if (!defined(CleanStorageComponentInfo(
								request => $r,
								usererror => $opt->{usererror},
								debug => $debug,
								dbh => $dbh,
								component => $db_disk))) {
							return undef;
						}
					}
				}
				my $disk_proto = $probed_disk->{disk_proto};

				if (!$disk_proto) {
					if ($probed_disk->{description} eq 'ATA Disk') {
						$disk_proto = 'SATA';
					} elsif ($probed_disk->{description} eq 'SCSI Disk') {
						$disk_proto = 'SCSI';
					} else {
						next;
					}
				}

#				$r->log_rerror(
#					Apache2::Log::LOG_MARK,
#					Apache2::Const::LOG_NOTICE,
#					APR::Const::SUCCESS,
#					sprintf("Inserting disk component: %s",
#						Dumper($probed_disk)));

				if (!defined($db_disk = runquery(
						description => 'inserting disk component',
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							SELECT * FROM
								component_utils.insert_disk_component(
									model := ?,
									bytes := ?,
									protocol := ?,
									media_type := ?,
									serial_number := ?
								)
						},
						args => [
							$probed_disk->{product},
							$probed_disk->{size},
							$disk_proto,
							(defined($probed_disk->{media_type}) &&
									$probed_disk->{media_type}
									=~ /^Solid State'/ ?
								'Solid State' :
								'Rotational'),
							$probed_disk->{serial}
						]
				))) {
					next;
				}
				$db_disk->{serial_number} = $probed_disk->{serial};
				$probed_disk->{component_id} = $db_disk->{component_id};
			
				if ($db_disk->{parent_slot_id}) {
					if (!$force) {
						$r->log_rerror(
							Apache2::Log::LOG_MARK,
							Apache2::Const::LOG_ERR,
							APR::Const::SUCCESS,
							sprintf("Disk component_id %s with serial number %s in slot %s is already attached to slot_id %d",
								$db_disk->{component_id},
								$probed_disk->{serial},
								$slot_name,
								$db_disk->{parent_slot_id}
							));
						return undef;
					}
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_NOTICE,
						APR::Const::SUCCESS,
						sprintf("Removing disk component_id %s with serial number %s in slot %s from slot_id %d",
							$db_disk->{component_id},
							$probed_disk->{serial},
							$slot_name,
							$db_disk->{parent_slot_id}
						));
					if (!defined(CleanStorageComponentInfo(
							request => $r,
							usererror => $opt->{usererror},
							debug => $debug,
							dbh => $dbh,
							component => $db_disk))) {
						return undef;
					}
				}

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf("Inserted component_id %s for new %s %s disk with serial %s in slot %s",
						$db_disk->{component_id},
						$probed_disk->{product},
						(defined($probed_disk->{media_type}) &&
								$probed_disk->{media_type}
							=~ /^Solid State'/ ?
								'Solid State' :
								'Rotational'),
						$probed_disk->{serial},
						$slot_name
					));

				my $slot;

				if ($swapped_disk) {
					if (!defined(runquery(
							description => sprintf(
								'swapping disk component_id %d with %d',
								$swapped_disk->{component_id},
								$db_disk->{component_id}),
							request => $r,
							usererror => $opt->{usererror},
							debug => $debug,
							dbh => $dbh,
							return_type => 'hashref',
							report_error => 0,
							query => q {
								SELECT component_utils.replace_component(
									old_component_id := ?,
									new_component_id := ?
								)
							},
							args => [
								$swapped_disk->{component_id},
								$db_disk->{component_id}
							]
					))) {
						return undef;
					}
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_INFO,
						APR::Const::SUCCESS,
						sprintf("Replaced disk having component_id %d with component_id %d",
							$swapped_disk->{component_id},
							$db_disk->{component_id}
						));
				} else {
					if (!defined($slot = runquery(
							description =>
								'attaching disk component to parent slot',
							request => $r,
							usererror => $opt->{usererror},
							debug => $debug,
							dbh => $dbh,
							return_type => 'hashref',
							report_error => 0,
							query => q {
								SELECT * FROM
									component_utils.insert_component_into_parent_slot(
										parent_component_id := ?,
										component_id := ?,
										slot_name := ?,
										slot_type := ?,
										slot_function := 'disk'
									)
							},
							args => [
								$db_adp->{component_id},
								$db_disk->{component_id},
								$slot_name,
								$disk_proto eq 'SCSI' ?
									'SCSI' :
									'SAS',
							]
					))) {
						next;
					}
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_DEBUG,
						APR::Const::SUCCESS,
						sprintf("Attached component_id %d to slot %s (slot_id %d) of component %d",
							$db_disk->{component_id},
							$slot->{slot_name},
							$slot->{slot_id},
							$db_adp->{component_id},
						));
				}
			}
		}

		#
		# Re-fetch updated disk configs from the database, plus additional
		# crap for physicalish_volume
		#

		if (!defined($db_disks = runquery(
			description => sprintf(
				'fetching disks for storage controller %s',
				$db_adp->{component_id}),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			query => q {
				WITH x AS (
					SELECT
						component_type_id,
						array_agg(component_function) as functions
					FROM
						component_type_component_func
					GROUP BY
						component_type_id
				) SELECT
					c.component_id,
					slot_id,
					asset_id,
					serial_number,
					slot_name,
					model,
					pv.physicalish_volume_id,
					pv.physicalish_volume_name,
					pv.physicalish_volume_type,
					pv.device_id
				FROM
					component c JOIN
					component_type ct USING (component_type_id) JOIN
					x USING (component_type_id) JOIN
					slot s ON (c.parent_slot_id = s.slot_id) LEFT JOIN
					asset a ON (c.component_id = a.component_id) LEFT JOIN
					physicalish_volume pv ON (pv.component_id = c.component_id)
						LEFT JOIN
					volume_group_physicalish_vol USING (physicalish_volume_id)
						LEFT JOIN
					volume_group vg USING (volume_group_id)
				WHERE
					s.component_id = ? AND
					'disk' = ANY(functions) AND
					'storage' = ANY(functions)
			},
			args => [
				$db_adp->{component_id}
			]
		))) {
			return undef;
		}

		###
		### Determine that a correct physicalish_volume exists for all
		### disk components.  There is a one to one mapping of probed and
		### database components at this point
		###
	
		my $pv_type = ($adapter->{configuration}->{driver} eq 'megaraid_sas') ?
			'raid_disk' :
			'os_disk';

		my $phys_vol_ids = {};
		my $phys_vol_names = {};
		foreach my $db_disk (@$db_disks) {
			#
			# Insert a physicalish_volume for this component if it does not
			# exist
			#

			my $probed_disk = [ 
				grep { 
					($_->{businfo} || $_->{physid}) eq $db_disk->{slot_name} 
				} @{$disklist}
			]->[0];

			if (!$probed_disk) {
				#
				# The disk was apparently removed, so we need to get rid of
				# it
				#

				if (!defined($ret = runquery(
						description => sprintf(
							'removing physicalish_volume for disk component %d',
							$db_disk->{component_id}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						report_error => 0,
						query => q {
							SELECT * FROM lv_manip.delete_pv(
								physicalish_volume_list := ARRAY(
									SELECT
										physicalish_volume_id
									FROM
										physicalish_volume
									WHERE
										component_id = ?
								)
							)
						},
						args => [
							$db_disk->{component_id}
						]
				))) {
					return undef;
				};

				if (!defined($ret = runquery(
						description => sprintf(
							'removing disk component %d from device %d',
							$db_disk->{component_id},
							$device->{device_id}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						report_error => 0,
						query => q {
							UPDATE
								component
							SET
								parent_slot_id = NULL
							WHERE
								component_id = ?
						},
						args => [
							$db_disk->{component_id}
						]
				))) {
					return undef;
				};
			}
			#
			# Name the logical volume
			#
			my $pv_name = $probed_disk->{logicalname} ||
				$probed_disk->{businfo} ||
				$probed_disk->{physid};

			if (!$db_disk->{physicalish_volume_id}) {
				if (!defined($ret = runquery(
						description => sprintf(
							'inserting physicalish_volume for disk component %d',
							$db_disk->{component_id}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							INSERT INTO physicalish_volume (
								physicalish_volume_name,
								physicalish_volume_type,
								device_id,
								component_id
							) VALUES (
								?,
								?,
								?,
								?
							) RETURNING *
						},
						args => [
								$pv_name,
								$pv_type,
								$device->{device_id},
								$db_disk->{component_id}
						]
				))) {
					return undef;
				}
				map {
					$db_disk->{$_} = $ret->{$_};
				} ( qw(
						physicalish_volume_id
						physicalish_volume_name
						physicalish_volume_type
						device_id
					));

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_DEBUG,
					APR::Const::SUCCESS,
					sprintf("Inserted physicalish_volume_id %d for component_id %d",
						$db_disk->{physicalish_volume_id},
						$db_disk->{component_id}
					));
			} elsif (
				#
				# These presumably should not ever change, but just in case
				#
				$db_disk->{physicalish_volume_name} ne $pv_name ||
				$db_disk->{physicalish_volume_type} ne $pv_type ||
				$db_disk->{device_id} ne $device->{device_id}
			) {
				if (!defined($ret = runquery(
						description => sprintf(
							'updating physicalish_volume attributes for disk component %d',
							$db_disk->{component_id}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							UPDATE
								physicalish_volume
							SET
								physicalish_volume_name = ?,
								physicalish_volume_type = ?,
								device_id = ?
							WHERE
								component_id = ?
							RETURNING *
						},
						args => [
								$pv_name,
								$pv_type,
								$device->{device_id},
								$db_disk->{component_id}
						]
				))) {
					next;
				}
				map {
					$db_disk->{$_} = $ret->{$_};
				} ( qw(
						physicalish_volume_id
						physicalish_volume_name
						physicalish_volume_type
						device_id
					));

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_DEBUG,
					APR::Const::SUCCESS,
					sprintf("Updated physicalish_volume attributes for id %d",
						$db_disk->{physicalish_volume_id}
					));
			}
			$phys_vol_ids->{$db_disk->{physicalish_volume_id}} = $db_disk;
			$phys_vol_names->{$db_disk->{physicalish_volume_name}} = $db_disk;
		}

		##
		## The rest of this only applies to LSI things
		##

		if ($adapter->{configuration}->{driver} ne 'megaraid_sas') {
			next;
		}

		###
		### Process volume group/logical volume stuff for this adapter
		###
	
		#
		# We kind of have to do volume groups and logical volumes together
		# for the LSI adapters, because they don't have a separate exposed
		# "volume group" concept, even though there really is
		#

		my $db_vgs;
		my $db_lv_names;
		my $db_lv_ids;

		if (!defined($db_vgs = runquery(
			description => sprintf(
				'fetching volume groups for storage controller %s by name',
				$db_adp->{component_id}),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'volume_group_name',
			query => q {
				SELECT
					volume_group_id,
					device_id,
					volume_group_name,
					volume_group_type,
					volume_group_size_in_bytes,
					raid_type
				FROM
					volume_group vg
				WHERE
					(
						volume_group_type = 'LSI RAID' AND
						device_id = ? AND
						volume_group_name ~ ?
					) OR (
						volume_group_id IN (
							SELECT DISTINCT
								volume_group_id
							FROM
								volume_group_physicalish_vol
							WHERE
								physicalish_volume_id = ANY (?)
						)
					)
			},
			args => [
				$device->{device_id},
				'^' . $db_adp->{component_id} . '_',
				[ map { $_->{physicalish_volume_id} } @{$db_disks} ]
			]
		))) {
			return undef;
		}

		my $vgpvs;
		if (!defined($vgpvs = runquery(
			description => sprintf(
				'fetching volume group members for storage controller %s',
				$db_adp->{component_id}),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'volume_group_id',
			query => q {
				SELECT
					volume_group_id,
					array_agg(physicalish_volume_id)
				FROM
					volume_group_physicalish_vol
				WHERE
					volume_group_id = ANY (?)
				GROUP BY
					volume_group_id
			},
			args => [
				[ map { $_->{volume_group_id} } values %$db_vgs ]
			]
		))) {
			return undef;
		}

		my $pv_vginfo;
		if (!defined($pv_vginfo = runquery(
			description => sprintf(
				'fetching volume group members for storage controller %s',
				$db_adp->{component_id}),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'physicalish_volume_id',
			query => q {
				SELECT
					physicalish_volume_id,
					volume_group_id,
					device_id,
					volume_group_primary_pos,
					volume_group_relation
				FROM
					volume_group_physicalish_vol
				WHERE
					volume_group_id = ANY (?)
			},
			args => [
				[ map { $_->{volume_group_id} } values %$db_vgs ]
			]
		))) {
			return undef;
		}

		if (!defined($db_lv_ids = runquery(
			description => sprintf(
				'fetching logical volumes for storage controller %s',
				$db_adp->{component_id}),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'logical_volume_id',
			query => q {
				SELECT
					lv.logical_volume_id,
					lv.volume_group_id,
					lv.device_id,
					logical_volume_name,
					logical_volume_size_in_bytes,
					logical_volume_offset_in_bytes,
					lv.filesystem_type,
					si.logical_volume_property_value scsi_id
				FROM
					logical_volume lv LEFT JOIN
					logical_volume_property si ON
						(lv.logical_volume_id = si.logical_volume_id AND
						 si.logical_volume_property_name = 'SCSI_Id' AND
						 si.filesystem_type = 'physicalish_volume')
				WHERE
					volume_group_id = ANY (?)
			},
			args => [
				[ map { $_->{volume_group_id} } values %$db_vgs ]
			]
		))) {
			return undef;
		}

		if (!defined($db_lv_names = runquery(
			description => sprintf(
				'fetching logical volumes for storage controller %s',
				$db_adp->{component_id}),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'logical_volume_name',
			query => q {
				SELECT
					lv.logical_volume_id as logical_volume_id,
					lv.volume_group_id,
					lv.device_id,
					logical_volume_name,
					logical_volume_size_in_bytes,
					logical_volume_offset_in_bytes,
					lv.filesystem_type,
					si.logical_volume_property_value scsi_id
				FROM
					logical_volume lv JOIN
					volume_group vg USING (volume_group_id) LEFT JOIN
					logical_volume_property si ON
						(lv.logical_volume_id = si.logical_volume_id AND
						 si.logical_volume_property_name = 'SCSI_Id' AND
						 si.filesystem_type = 'physicalish_volume')
				WHERE
					lv.device_id = ? AND
					vg.volume_group_type = 'LSI RAID'
			},
			args => [
				$device->{device_id}
			]
		))) {
			return undef;
		}

#		$db_lv_names =
#			{ map { ($_->{logical_volume_name}, $_) } values %$db_lv_ids };

		my $probed_lvs = $megaraid_info->{logical_disks};

		foreach my $lv (@$probed_lvs) {
			$lv->{os_disk} = (
					grep { 
						$_->{physid} && 
						$_->{physid} =~ /\d+\.([0-9a-f]+)\./ &&
						$lv->{scsi_id} == hex($1)
					}
						@{$adapter->{os_disks}}
				)[0];

			if (!$lv->{os_disk}) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_ERR,
					APR::Const::SUCCESS,
					sprintf('Unable to locate OS disk information for logical volume with SCSI id %s in request',
						$lv->{scsi_id})
				);
				return undef;
			}
		}

		##
		## Process volume groups and physicalish volume membership
		##

		foreach my $lv (@$probed_lvs) {
			if (!@{$lv->{physical_disks}}) {
				#
				# This should never happen
				#
				next;
			}
			my $vg_name =
				$db_adp->{component_id} . '_' .
				$lv->{physical_disks}->[0]->{drive_position}->{disk_group};

			my $vg = $db_vgs->{$vg_name};

			if ($vg && $vg->{handled}) {
				#
				# Store this for later, so we don't have to find it again
				#
				$lv->{volume_group_id} = $vg->{volume_group_id};
				next;
			}
			if (!$vg) {
				#
				# Volume group does not exist in the database, so create
				# and populate it
				#

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_DEBUG,
					APR::Const::SUCCESS,
					Data::Dumper->Dump([$lv], ['$lv'])
					);
				my $raid_type = 'RAID' . $lv->{raid_level}->{primary};
				if ($lv->{raid_level}->{primary} == 1 ||
						$lv->{raid_level}->{secondary}) {
					$raid_type .= "+" . $lv->{raid_level}->{secondary};
				}
				if (!defined($vg = runquery(
					description => sprintf(
						'creating volume group %s for storage controller %d',
						$vg_name,
						$db_adp->{component_id}),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						INSERT INTO volume_group (
							device_id,
							volume_group_name,
							volume_group_type,
							volume_group_size_in_bytes,
							raid_type
						) VALUES (
							?,
							?,
							'LSI RAID',
							0,
							?
						)
						RETURNING *
					},
					args => [
						$device->{device_id},
						$vg_name,
						$raid_type
					]
				))) {
					return undef;
				}
				$db_vgs->{$vg_name} = $vg;

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Inserted volume_group "%s", raid_type %s (volume_group_id %d) for storage controller %d',
						$vg_name,
						$raid_type,
						$vg->{volume_group_id},
						$db_adp->{component_id}
					));
			}
			#
			# Mark this as done already so that we don't process it multiple
			# times
			#
			$vg->{handled} = 1;

			#
			# Store the VG mapping for later
			#
			$lv->{volume_group_id} = $vg->{volume_group_id};
			
			my $offset = 0;

			foreach my $disk (@{$lv->{physical_disks}}) {

				#
				# Skip these temporarily while we get rid of bad clients
				#
				if ($disk->{physid} eq ':') {
					next;
				}

				my $pv = $phys_vol_names->{$disk->{physid}};
				if (!$pv) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_ERR,
						APR::Const::SUCCESS,
						sprintf(
							'physicalish_volume for disk in slot %s not found.  This should not be possible',
							$disk->{physid}
						)
					);
					return undef;
				}
				my $db_disk = $pv_vginfo->{$pv->{physicalish_volume_id}};

				if (!$db_disk) {
					if (!defined($db_disk = runquery(
						description => sprintf(
							'assigning physicalish_volume %d to volume_group %s',
							$pv->{physicalish_volume_id},
							$vg->{volume_group_id}),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						query => q {
							INSERT INTO volume_group_physicalish_vol (
								physicalish_volume_id,
								volume_group_id,
								device_id,
								volume_group_primary_pos,
								volume_group_relation
							) VALUES (
								?,
								?,
								?,
								?,
								'member_disk'
							)
							RETURNING *
						},
						args => [
							$pv->{physicalish_volume_id},
							$vg->{volume_group_id},
							$device->{device_id},
							$offset
						]
					))) {
						return undef;
					}
					$pv_vginfo->{$pv->{physicalish_volume_id}} =
						$db_disk;
				}
				if ($db_disk->{volume_group_primary_pos} != $offset ||
						$db_disk->{volume_group_id} != $vg->{volume_group_id}) {

					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_INFO,
						APR::Const::SUCCESS,
						sprintf('Updating physicalish volume %s (%d) to offset %d of volume group %s (%d), was vg_id %d, offset %d',
							$pv->{physicalish_volume_name},
							$pv->{physicalish_volume_id},
							$offset,
							$vg->{volume_group_name},
							$vg->{volume_group_id},
							$db_disk->{volume_group_id},
							$db_disk->{volume_group_primary_pos}
							));

					if (!defined($ret = runquery(
						description => sprintf(
							'setting volume_group and offset of physicalish_volume %d',
							$pv->{physicalish_volume_id}),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						query => q {
							UPDATE volume_group_physicalish_vol
							SET
								volume_group_id = ?,
								volume_group_primary_pos = ?
							WHERE physicalish_volume_id = ?
						},
						args => [
							$vg->{volume_group_id},
							$offset,
							$pv->{physicalish_volume_id}
						]
					))) {
						return undef;
					}


				}
				$db_disk->{handled} = 1;
			} continue {
				$offset += 1;
			}
		}
		#
		# Remove any physicalish_volumes that were previously assigned but
		# not referenced from any volume_groups
		#
		my $orphaned_pvs = [
			grep { !(defined($pv_vginfo->{$_}->{handled})) } keys %$pv_vginfo
		];
		if (@$orphaned_pvs) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS,
				sprintf(
					'Removing unreferenced physicalish_volume(s) %s from volume groups',
					(join(',', @$orphaned_pvs))
				)
			);
			if (!defined($ret = runquery(
				description => sprintf(
					'removing unreferenced physicalish_volume(s) from volume_groups for controller %d',
					$db_adp->{component_id}),
				request => $r,
				usererror => $opt->{usererror},
				debug => $debug,
				dbh => $dbh,
				return_type => 'hashref',
				query => q {
					SELECT * FROM
						lv_manip.remove_pv_membership(
							physicalish_volume_list := ?,
							purge_orphans := true
						)
				},
				args => [
					$orphaned_pvs
				]
			))) {
				return undef;
			}
		}

		###
		### At this point, volume groups and volume group membership
		### are correct.  Process logical volumes
		###

		#
		# Loop through, delete any logical volumes which no longer exist
		# for those that do
		#
		foreach my $lv (values %$db_lv_ids) {
			#
			# We're keying the specific logical volume on the serial
			# number of the "disk" presented to the OS.  This is supposed
			# to be unique and will change if the volume gets deleted and
			# recreated
			#
			my $probed_lv = (grep {
					$_->{os_disk}->{serial} eq $lv->{logical_volume_name}
				} @$probed_lvs)[0];

			if (!defined($probed_lv)) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_NOTICE,
					APR::Const::SUCCESS,
					sprintf('RAID logical volume %s (id %d) has disappeared',
						$lv->{logical_volume_name},
						$lv->{logical_volume_id}
					));

				if (!defined($ret = runquery(
					description => sprintf('deleting logical volume %d',
						$lv->{logical_volume_id}),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					hashkey => 'logical_volume_id',
					query => q {
						SELECT * FROM
						lv_manip.delete_lv(
							logical_volume_id := ?,
							purge_orphans := true
						)
					},
					args => [
						$lv->{logical_volume_id}
					]
				))) {
					return undef;
				}
				delete $db_lv_names->{$lv->{logical_volume_name}};
				delete $db_lv_ids->{$lv->{logical_volume_id}};
				next;
			}

			#
			# Mark that we've already seen this logical volume, so
			# we don't need to insert it later
			#
			$probed_lv->{handled} = 1;
			$probed_lv->{logical_volume_id} = $lv->{logical_volume_id};
			$lv->{probed_lv} = $probed_lv;

			#
			# If the volume_group changed for whatever reason, update
			# the database
			#
			if ($lv->{volume_group_id} != $probed_lv->{volume_group_id}) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Moving RAID logical_volume %d (%s) from volume_group %d to volume_group %d',
						$lv->{logical_volume_id},
						$lv->{logical_volume_name},
						$lv->{volume_group_id},
						$probed_lv->{volume_group_id}
					));

				if (!defined($ret = runquery(
					description => sprintf(
						'changing volume group for logical volume %d',
						$lv->{logical_volume_id}),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					hashkey => 'logical_volume_id',
					query => q {
						UPDATE
							logical_volume
						SET
							volume_group_id = ?
						WHERE
							logical_volume_id = ?
					},
					args => [
						$probed_lv->{volume_group_id},
						$lv->{logical_volume_id}
					]
				))) {
					return undef;
				}
			}

			#
			# It should not be possible for the SCSI id of the LV to change,
			# but just in case someone did something stupid, we handle that
			#

			if (!($lv->{scsi_id} ~~ $probed_lv->{scsi_id})) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Setting SCSI ID of logical volume %s (%d) to %d (was: %s)',
						$lv->{logical_volume_name},
						$lv->{logical_volume_id},
						$probed_lv->{scsi_id},
						defined($lv->{scsi_id}) ? $lv->{scsi_id} : 'not set'
					));

				my $q;
				if (!defined($lv->{scsi_id})) {
					$q = q {
						INSERT INTO logical_volume_property (
							filesystem_type,
							logical_volume_property_name,
							logical_volume_property_value,
							logical_volume_id
						) VALUES (
							'physicalish_volume',
							'SCSI_Id',
							?,
							?
						)
					};
				} else {
					$q = q {
						UPDATE
							logical_volume_property
						SET
							logical_volume_property_value = ?
						WHERE
							filesystem_type = 'physicalish_volume' AND
							logical_volume_property_name = 'SCSI_Id' AND
							logical_volume_id = ?
					};
				}

				if (!defined($ret = runquery(
					description => sprintf('setting logical volume %d SCSI id',
						$lv->{logical_volume_id}),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					hashkey => 'logical_volume_id',
					query => $q,
					args => [
						$probed_lv->{scsi_id},
						$lv->{logical_volume_id}
					]
				))) {
					return undef;
				}
			}
		}
		#
		# Create new logical volumes for everything that was not handled
		# above
		#

		foreach my $probed_lv (grep { !($_->{handled}) } @$probed_lvs) {
			if (exists($db_lv_names->{$probed_lv->{os_disk}->{serial}})) {
				my $lv = $db_lv_names->{$probed_lv->{os_disk}->{serial}};
				#
				# If another RAID logical volume exists with this name, then
				# it means we need to move it, because they are globally
				# unique.  There are a number of ways that this can happen,
				# all of them annoying
				#
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_DEBUG,
					APR::Const::SUCCESS,
					sprintf('Moving logical_volume %d (%s) to volume group %d for controller %d',
						$lv->{logical_volume_id},
						$lv->{logical_volume_name},
						$probed_lv->{volume_group_id},
						$db_adp->{component_id}
					));
				my $ret;
				if (!defined($ret = runquery(
					description => sprintf(
						'moving logical_volume %s into volume group %d',
						$probed_lv->{os_disk}->{serial},
						$probed_lv->{volume_group_id}),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						UPDATE
							logical_volume
						SET
							volume_group_id = ?
						WHERE
							logical_volume_id = ?
					},
					args => [
						$probed_lv->{volume_group_id},
						$lv->{logical_volume_id}
					]
				))) {
					return undef;
				}
				$db_lv_ids->{$lv->{logical_volume_id}} = $lv;
				$probed_lv->{logical_volume_id} = $lv->{logical_volume_id};
				$lv->{probed_lv} = $probed_lv;
			} else {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_DEBUG,
					APR::Const::SUCCESS,
					sprintf('Creating logical volume %s in volume group id %d for controller %d',
						$probed_lv->{os_disk}->{serial},
						$probed_lv->{volume_group_id},
						$db_adp->{component_id}
					));

				my $lv;
				if (!defined($lv = runquery(
					description => sprintf(
						'inserting logical volume %s into volume group %d',
						$probed_lv->{os_disk}->{serial},
						$probed_lv->{volume_group_id}),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						INSERT INTO logical_volume (
							volume_group_id,
							device_id,
							logical_volume_name,
							logical_volume_size_in_bytes,
							filesystem_type
						) VALUES (
							?,
							?,
							?,
							?,
							?
						)
						RETURNING *
					},
					args => [
						$probed_lv->{volume_group_id},
						$device->{device_id},
						$probed_lv->{os_disk}->{serial},
						$probed_lv->{os_disk}->{size},
						'physicalish_volume'
					]
				))) {
					return undef;
				}
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Created logical volume %s (id %d) in volume group id %d for controller %d',
						$probed_lv->{os_disk}->{serial},
						$lv->{logical_volume_id},
						$probed_lv->{volume_group_id},
						$db_adp->{component_id}
					));

				$db_lv_names->{$lv->{logical_volume_name}} = $lv;
				$db_lv_ids->{$lv->{logical_volume_id}} = $lv;
				$probed_lv->{logical_volume_id} = $lv->{logical_volume_id};
				$lv->{probed_lv} = $probed_lv;

				if (!defined($ret = runquery(
					description => sprintf('setting logical volume %d SCSI id',
						$lv->{logical_volume_id}),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					hashkey => 'logical_volume_id',
					query => q {
						INSERT INTO logical_volume_property (
							filesystem_type,
							logical_volume_property_name,
							logical_volume_property_value,
							logical_volume_id
						) VALUES (
							'physicalish_volume',
							'SCSI_Id',
							?,
							?
						)
					},
					args => [
						$probed_lv->{scsi_id},
						$lv->{logical_volume_id}
					]
				))) {
					return undef;
				}
			}
		}
		#
		# Ensure physicalish_volumes are created for OS disks
		#

		my $os_pv;
		if (!defined($os_pv = runquery(
				description => sprintf(
					'pulling OS physicalish_volumes for adapter component %d',
					$db_adp->{component_id}
				),
				request => $r,
				usererror => $opt->{usererror},
				debug => $debug,
				dbh => $dbh,
				allrows => 1,
				return_type => 'hashref',
				report_error => 0,
				query => q {
					SELECT
						physicalish_volume_id,
						physicalish_volume_name,
						physicalish_volume_type,
						pv.device_id,
						logical_volume_id,
						logical_volume_name
					FROM
						logical_volume lv LEFT JOIN
						physicalish_volume pv USING (logical_volume_id)
					WHERE
						logical_volume_id = ANY (?)
				},
				args => [
					[ keys %$db_lv_ids ]
				]
		))) {
			return undef;
		}
		
		#
		# There is the possibility here that a non-RAID OS disk exists which
		# has the same name in the database as one probed here.  This *should*
		# not be a problem since all of that should get cleaned up before
		# we start constructing/validating the partition stuff later, and
		# constraints are deferred.
		#
		foreach my $lv (@$os_pv) {
			my $probed_lv =
				$db_lv_ids->{$lv->{logical_volume_id}}->{probed_lv};

			if (!$lv->{physicalish_volume_id}) {
				if (!defined($ret = runquery(
						description => sprintf(
							'inserting new physicalish_volume %s for RAID logical_volume %d',
							$probed_lv->{os_disk}->{logicalname},
							$lv->{logical_volume_id}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							INSERT INTO physicalish_volume (
								physicalish_volume_name,
								physicalish_volume_type,
								device_id,
								logical_volume_id
							) VALUES (
								?,
								?,
								?,
								?
							)
							RETURNING *
						},
						args => [
							$probed_lv->{os_disk}->{logicalname},
							'os_disk',
							$device->{device_id},
							$lv->{logical_volume_id}
						]
				))) {
					return undef;
				}
				map { $lv->{$_} = $ret->{$_} } ( qw(
					physicalish_volume_id
					physicalish_volume_name
					physicalish_volume_type)
					);
				
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Created %s physicalish_volume %s (id %d) for RAID logical volume %s (id %d)',
						$lv->{physicalish_volume_type},
						$lv->{physicalish_volume_name},
						$lv->{physicalish_volume_id},
						$lv->{logical_volume_name},
						$lv->{logical_volume_id}
					));

				next;
			}
			#
			# Ensure that the values for these attributes didn't change
			#
			if ($lv->{physicalish_volume_type} ne 'os_disk' ||
					$lv->{physicalish_volume_name} ne
						$probed_lv->{os_disk}->{logicalname} ||
					$lv->{device_id} != $device->{device_id}
			) {
				
				if (!defined(runquery(
						description => sprintf(
							'correcting physicalish_volume attributes for %s (id %d)',
							$probed_lv->{os_disk}->{logicalname},
							$lv->{logical_volume_id}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							UPDATE physicalish_volume
							SET
								physicalish_volume_name = ?,
								physicalish_volume_type = ?,
								device_id = ?
							WHERE
								physicalish_volume_id = ?
						},
						args => [
							$probed_lv->{os_disk}->{logicalname},
							'os_disk',
							$device->{device_id},
							$lv->{physicalish_volume_id}
						]
				))) {
					return undef;
				}

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Set physicalish_volume id %d to name %s, type os_disk, device_id %d (was %s, %s, %d)',
						$lv->{physicalish_volume_id},
						$probed_lv->{os_disk}->{logicalname},
						$device->{device_id},
						$lv->{physicalish_volume_name},
						$lv->{physicalish_volume_type},
						$lv->{device_id}
					));

			}
		}
	}

	#
	# Clean up any empty RAID volume groups for this device
	#

	if (!defined(runquery(
			description => sprintf(
				'deleting unused LSI RAID volume groups from device %d',
				$device->{device_id}
			),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			return_type => 'hashref',
			report_error => 0,
			query => q {
				WITH x AS (
					SELECT
						volume_group_id
					FROM
						volume_group vg LEFT JOIN
						volume_group_physicalish_vol USING (volume_group_id)
							LEFT JOIN
						logical_volume USING (volume_group_id)
					WHERE
						logical_volume_id IS NULL AND
						physicalish_volume_id IS NULL AND
						volume_group_type = 'LSI RAID' AND
						vg.device_id = ?
				), y AS (
					DELETE FROM
						volume_group_purpose
					WHERE
						volume_group_id IN (SELECT volume_group_id FROM x)
				)
				DELETE FROM
					volume_group
				WHERE
					volume_group_id IN (SELECT volume_group_id FROM x)
			},
			args => [
				$device->{device_id},
			]
	))) {
		return undef;
	}

	return 1;
}

sub OSDiskInventory {
	my $opt = &_options(@_);
	my $r = $opt->{request};
	my $debug = $opt->{debug};
	my $dbh = $opt->{dbh};
	my $force = $opt->{force};
	my $inventory = $opt->{inventory};
	my $device = $opt->{device};

	my $ret;
	#
	# Defer constraints until later, because we play fast and loose with
	# moving some things around
	#

	$dbh->do('SET CONSTRAINTS ALL DEFERRED');

	my $os_pv;
	if (!defined($os_pv = runquery(
			description => sprintf(
				'pulling OS partitioning VG information for device %d',
				$device->{device_id}
			),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'physicalish_volume_name',
			report_error => 0,
			query => q {
				SELECT
					physicalish_volume_id,
					physicalish_volume_name,
					physicalish_volume_type,
					volume_group_id,
					volume_group_name,
					volume_group_type,
					volume_group_size_in_bytes,
					raid_type
				FROM
					physicalish_volume pv LEFT JOIN
					volume_group_physicalish_vol vgpv USING (physicalish_volume_id) LEFT JOIN
					volume_group vg USING (volume_group_id)
				WHERE
					pv.device_id = ? AND
					physicalish_volume_type = 'os_disk'
			},
			args => [
				$device->{device_id}
			]
	))) {
		return undef;
	}

	my $db_disk_parts;
	if (!defined($db_disk_parts = runquery(
			description => sprintf(
				'pulling OS partitioning LV information for device %d',
				$device->{device_id}
			),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'logical_volume_id',
			report_error => 0,
			query => q {
				SELECT
					physicalish_volume_id,
					volume_group_id,
					lv.logical_volume_id,
					logical_volume_name,
					logical_volume_size_in_bytes,
					logical_volume_offset_in_bytes,
					lv.filesystem_type,
					serial.logical_volume_property_value AS serial,
					serial.logical_volume_property_id AS serial_id,
					mp.logical_volume_property_value AS mount_point,
					mp.logical_volume_property_id AS mount_point_id,
					label.logical_volume_property_value AS label,
					label.logical_volume_property_id AS label_id
				FROM
					physicalish_volume pv LEFT JOIN
					volume_group_physicalish_vol vgpv USING (physicalish_volume_id) LEFT JOIN
					volume_group vg USING (volume_group_id) LEFT JOIN
					logical_volume lv USING (volume_group_id) LEFT JOIN
					logical_volume_property serial ON
						(lv.logical_volume_id = serial.logical_volume_id AND
						 serial.logical_volume_property_name = 'Serial')
						 LEFT JOIN
					logical_volume_property mp ON
						(lv.logical_volume_id = mp.logical_volume_id AND
						 mp.logical_volume_property_name = 'MountPoint')
						 LEFT JOIN
					logical_volume_property label ON
						(lv.logical_volume_id = label.logical_volume_id AND
						 label.logical_volume_property_name = 'Label')
				WHERE
					pv.device_id = ? AND
					physicalish_volume_type = 'os_disk' AND
					lv.logical_volume_id IS NOT NULL
			},
			args => [
				$device->{device_id}
			]
	))) {
		return undef;
	}

	if ($debug > 2 ) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_DEBUG,
			APR::Const::SUCCESS,
			Data::Dumper->Dump( [ $db_disk_parts ], [ '$db_disk_parts' ])
		);
	}

	#
	# Take the probed disks from all of the adapters and pull them into
	# one array
	#
	my $probed_disks = [
		map { @{$_->{os_disks}} }
			@{FindHashChild(
				hash => $inventory->{lshw_output},
				key => "os_disks"
			)}
	];

	#
	# We are assuming here that all physicalish_volumes have been inserted,
	# but we need to make sure that there aren't any that need to be removed
	#

	my $disk_list = [
		map {
			my $x = $_;
			if (!grep
					{ $_->{logicalname} eq $x->{physicalish_volume_name} }
					@$probed_disks) {
				delete $os_pv->{$_->{physicalish_volume_name}};
				($_->{physicalish_volume_id})
			} else {
				()
			}
		} values %$os_pv
	];

	if (@$disk_list) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			sprintf("Removing orphaned physicalish_volume(s) %s",
				join(',', @$disk_list))
		);

		if (!defined($ret = runquery(
			description => 'deleting orphaned os physicalish_volumes',
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			return_type => 'hashref',
			query => q {
				SELECT * FROM
				lv_manip.delete_pv(
					physicalish_volume_list := ?,
					purge_orphans := true
				)
			},
			args => [
				$disk_list
			]
		))) {
			return undef;
		}
	}

	foreach my $disk (@$probed_disks) {
		my $logicalname = (ref($disk->{logicalname}) eq 'ARRAY') ?
			$disk->{logicalname}->[0] : $disk->{logicalname};
	
		my $db_disk = (grep { $_->{physicalish_volume_name} eq
				$logicalname } values %$os_pv)[0];
		
		if (!$db_disk) {
			#
			# This should not be able to happen, but just in case
			#

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_NOTICE,
				APR::Const::SUCCESS,
				sprintf('database os_disk physicalish_volume for %s has disappeared.  Skipping.',
					$logicalname)
			);
			next;
		}

		#
		# If the disk has partitions, then add the new ones, otherwise
		# make sure the old ones are cleaned out
		#
		if (!$disk->{partitions} || !@{$disk->{partitions}}) {
			#
			# The disk has no partitions.  Remove the volume group and
			# everything underneath it if it exists
			#

			if ($db_disk->{volume_group_id}) {
				
				if (!defined($ret = runquery(
					description => sprintf('deleting partitioning volume group for %s (%d)',
						$db_disk->{volume_group_name},
						$db_disk->{volume_group_id}
						),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						SELECT * FROM
						lv_manip.delete_vg(
							volume_group_id := ?,
							purge_orphans := true
						)
					},
					args => [
						$db_disk->{volume_group_id}
					]
				))) {
					return undef;
				}

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Deleted empty partition volume group for %s (%d)',
						$db_disk->{volume_group_name},
						$db_disk->{volume_group_id}
					));

			}
			next;
		}

		if (!$db_disk->{volume_group_id}) {
			if (!defined($ret = runquery(
					description => sprintf(
						'inserting new partition table volume group for disk %s',
						$db_disk->{physicalish_volume_name}
					),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						INSERT INTO volume_group (
							volume_group_name,
							volume_group_type,
							device_id,
							volume_group_size_in_bytes
						) VALUES (
							?,
							?,
							?,
							?
						)
						RETURNING *
					},
					args => [
						$logicalname,
						'partitioned disk',
						$device->{device_id},
						$disk->{size}
					]
			))) {
				return undef;
			}

			map { $db_disk->{$_} = $ret->{$_} } ( qw(
				volume_group_id
				volume_group_name
				volume_group_type
				volume_group_size_in_bytes
				raid_type)
				);
			
			if (!defined($ret = runquery(
					description => sprintf(
						'inserting physicalish_volume into newly-created partition table volume group %d for disk %s',
						$db_disk->{volume_group_id},
						$db_disk->{physicalish_volume_name}
					),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						INSERT INTO volume_group_physicalish_vol (
							physicalish_volume_id,
							volume_group_id,
							device_id,
							volume_group_primary_pos,
							volume_group_relation
						) VALUES (
							?,
							?,
							?,
							?,
							?
						)
						RETURNING *
					},
					args => [
						$db_disk->{physicalish_volume_id},
						$db_disk->{volume_group_id},
						$device->{device_id},
						0,
						'member_disk'
					]
			))) {
				return undef;
			}

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS,
				sprintf('Created volume_group %s (id %d) for disk partitions for physicalish_volume %s (%d)',
					$db_disk->{volume_group_name},
					$db_disk->{volume_group_id},
					$db_disk->{physicalish_volume_name},
					$db_disk->{physicalish_volume_id},
				));

		} else {
			#
			# Make sure the volume group and volume group type are set
			# correctly
			#
			if (!(
				$db_disk->{volume_group_name} ~~ $logicalname &&
				$db_disk->{volume_group_type} ~~ 'partitioned disk' &&
				$db_disk->{volume_group_size_in_bytes} ~~ $disk->{size}
			)) {
				if (!defined($ret = runquery(
						description => sprintf(
							'updating partition table volume group for disk %s (%d)',
							$db_disk->{physicalish_volume_name},
							$db_disk->{volume_group_id}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							UPDATE volume_group SET
								volume_group_name = ?,
								volume_group_type = ?,
								volume_group_size_in_bytes = ?
							WHERE
								volume_group_id = ?
							RETURNING *
						},
						args => [
							$logicalname,
							'partitioned disk',
							$disk->{size},
							$db_disk->{volume_group_id}
						]
				))) {
					return undef;
				}
				map { $db_disk->{$_} = $ret->{$_} } ( qw(
					volume_group_id
					volume_group_name
					volume_group_type
					volume_group_size_in_bytes
					raid_type)
					);
				
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Updated attributes for volume_group %s (id %d) for disk partitions for physicalish_volume %s (%d)',
						$db_disk->{volume_group_name},
						$db_disk->{volume_group_id},
						$db_disk->{physicalish_volume_name},
						$db_disk->{physicalish_volume_id},
					));

			}
		}

		#
		# Volume group is okay, now process the partitions
		#
	
		foreach my $disk_part (@{$disk->{partitions}}) {
			#
			# Look for the partition based on serial first, and then
			# by logicalname if that doesn't exist
			#
			my $partition_name = ref($disk_part->{logicalname}) eq 'ARRAY' ?
				$disk_part->{logicalname}->[0] :
				$disk_part->{logicalname};

			#
			# It seems that some partitions do not always get logicalnames
			# (EFI partitions, for example), so try to work around this
			#
			if (!$partition_name) {
				$partition_name = 
					$disk_part->{serial} ||
					$disk_part->{businfo} ||
					"";
			}
			my $filesystem_type;
			#
			# Some versions of lshw are different
			#
			if (!defined($disk_part->{size})) {
				$disk_part->{size} = $disk_part->{capacity};
			}
			if ($disk_part->{capabilities}->{lvm2}) {
				$filesystem_type = 'LVM'
			} elsif ($disk_part->{capabilities}->{nofs}) {
				$filesystem_type = 'swap'
			} elsif (
				$disk_part->{configuration}->{filesystem} &&
				grep { $disk_part->{configuration}->{filesystem} eq $_ }
					(qw (ext2 ext3 ext4))
			) {
				$filesystem_type =
					$disk_part->{configuration}->{filesystem};
			} else {
				$filesystem_type = 'unknown'
			}

			my $db_disk_part;
			if ($disk_part->{serial}) {
				$db_disk_part = (grep {
					$_->{serial} eq $disk_part->{serial}
				} values %$db_disk_parts)[0];
			}
			if (!$db_disk_part) {
				$db_disk_part = (grep {
					$_->{logical_volume_name} eq $partition_name
				} values %$db_disk_parts)[0];
			}
			if (!$db_disk_part) {
				#
				# Create a new partition
				#

				if (!defined($ret = runquery(
						description => sprintf(
							'inserting new partition logical_volume for %s',
							$partition_name
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							INSERT INTO logical_volume (
								volume_group_id,
								device_id,
								logical_volume_name,
								logical_volume_size_in_bytes,
								filesystem_type
							) VALUES (
								?,
								?,
								?,
								?,
								?
							)
							RETURNING *
						},
						args => [
							$db_disk->{volume_group_id},
							$device->{device_id},
							$partition_name,
							$disk_part->{size},
							$filesystem_type
						]
				))) {
					return undef;
				}

				$db_disk_part = $ret;
			
				$db_disk_parts->{$ret->{logical_volume_id}} = $db_disk_part;

				if ($filesystem_type =~ /^ext/) {
					my $propmap = {
						'Serial' => $disk_part->{serial},
						'Label' => $disk_part->{configuration}->{label},
					};

					# If partition isn't mounted, logicalname will only be a single entry for the device path.
					# Otherwise, it will be an array of device path and mount point.
					if (ref($disk_part->{logicalname}) eq 'ARRAY') {
						$propmap->{MountPoint} = $disk_part->{logicalname}->[1];
					}

					foreach my $v (keys %$propmap) {
						if (!defined($ret = runquery(
								description => sprintf(
									'inserting new partition logical_volume properties for %s (%d)',
									$partition_name,
									$db_disk_part->{logical_volume_id}
								),
								request => $r,
								usererror => $opt->{usererror},
								debug => $debug,
								dbh => $dbh,
								return_type => 'hashref',
								report_error => 0,
								query => q {
									INSERT INTO logical_volume_property (
										logical_volume_id,
										filesystem_type,
										logical_volume_property_name,
										logical_volume_property_value
									) VALUES (
										?,
										?,
										?,
										?
									)
								},
								args => [
									$db_disk_part->{logical_volume_id},
									$filesystem_type,
									$v,
									$propmap->{$v}
								]
						))) {
							return undef;
						}
					}
					$db_disk_part->{serial} = $disk_part->{serial};
					$db_disk_part->{label} =
						$disk_part->{configuration}->{label};

					if (ref($disk_part->{logicalname}) eq 'ARRAY') {
						$db_disk_part->{mount_point} = $disk_part->{logicalname}->[1];
					}

				}

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Created logical_volume %s (id %d) for disk partitions for physicalish_volume %s (%d)',
						$db_disk_part->{logical_volume_name},
						$db_disk_part->{logical_volume_id},
						$db_disk->{physicalish_volume_name},
						$db_disk->{physicalish_volume_id},
					));

				$db_disk_part->{seen} = 1;
				next;
			}

			$db_disk_part->{seen} = 1;

			if (!(
				$db_disk_part->{logical_volume_name} ~~ $partition_name &&
				$db_disk_part->{filesystem_type} ~~ $filesystem_type &&
				$db_disk_part->{logical_volume_size_in_bytes} ~~ $disk_part->{size}
			)) {
				if (!($db_disk_part->{filesystem_type} ~~ $filesystem_type)) {
					if (!defined($ret = runquery(
							description => sprintf(
								'removing partition logical volume properties for partition %s (%d)',
								$db_disk_part->{logical_volume_name},
								$db_disk->{logical_volume_id}
							),
							request => $r,
							usererror => $opt->{usererror},
							debug => $debug,
							dbh => $dbh,
							return_type => 'hashref',
							report_error => 0,
							query => q {
								DELETE FROM logical_volume_property
								WHERE
									logical_volume_id = ?
							},
							args => [
								$db_disk_part->{logical_volume_id}
							]
					))) {
						return undef;
					}

					map {
						$db_disk_part->{$_} = undef;
						$db_disk_part->{$_ . '_id'} = undef;
					} (qw(label serial mount_point));
				}
				if (!defined($ret = runquery(
						description => sprintf(
							'updating partition logical volume for partition %s (%d)',
							$db_disk_part->{logical_volume_name},
							$db_disk->{logical_volume_id}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							UPDATE logical_volume SET
								logical_volume_name = ?,
								filesystem_type = ?,
								logical_volume_size_in_bytes = ?
							WHERE
								logical_volume_id = ?
							RETURNING *
						},
						args => [
							$partition_name,
							$filesystem_type,
							$disk_part->{size},
							$db_disk_part->{logical_volume_id}
						]
				))) {
					return undef;
				}
				map { $db_disk->{$_} = $ret->{$_} } ( qw(
					logical_volume_name
					filesystem_type
					logical_volume_size_in_bytes
					));
				
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Updated attributes for volume_group %s (id %d) for disk partitions for physicalish_volume %s (%d)',
						$db_disk->{volume_group_name},
						$db_disk->{volume_group_id},
						$db_disk->{physicalish_volume_name},
						$db_disk->{physicalish_volume_id},
					));

			}

			if ($filesystem_type =~ /^ext/ || $filesystem_type eq 'xfs') {
				my $propmap = [
					{
						localkey => 'serial',
						dbkey => 'Serial',
						value => $disk_part->{serial}
					},
					{
						localkey => 'mount_point',
						dbkey => 'MountPoint',
						value => ref ($disk_part->{logicalname}) eq 'ARRAY' ?
							$disk_part->{logicalname}->[1] : 
							$disk_part->{logicalname}
					},
					{
						localkey => 'label',
						dbkey => 'Label',
						value => $disk_part->{configuration}->{label}
					}
				];
				foreach my $mapval (@$propmap) {
					next if ($db_disk_part->{$mapval->{localkey}} ~~
							$mapval->{value});

					my $q;
					if (!defined($db_disk_part->{$mapval->{localkey} . '_id'})) {
						$q = q {
							INSERT INTO logical_volume_property (
								logical_volume_property_value,
								filesystem_type,
								logical_volume_id,
								logical_volume_property_name
							) VALUES (
								?,
								?,
								?,
								?
							)
						};
					} else {
						$q = q {
							UPDATE logical_volume_property
							SET
								logical_volume_property_value = ?,
								filesystem_type = ?
							WHERE
								logical_volume_id = ? AND
								logical_volume_property_name = ?
						};
					}
								
					if (!defined($ret = runquery(
							description => sprintf(
								'updating partition logical_volume_property %s to %s for %s',
								$mapval->{dbkey},
								$mapval->{value},
								$db_disk_part->{logical_volume_name}
							),
							request => $r,
							usererror => $opt->{usererror},
							debug => $debug,
							dbh => $dbh,
							return_type => 'hashref',
							report_error => 0,
							query => $q,
							args => [
								$mapval->{value},
								$filesystem_type,
								$db_disk_part->{logical_volume_id},
								$mapval->{dbkey}
							]
					))) {
						return undef;
					}


					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_INFO,
						APR::Const::SUCCESS,
						sprintf(
								'updated partition logical_volume_property %s from %s, type %s to %s, type %s for %s (%d)',
								$mapval->{dbkey},
								$db_disk_part->{$mapval->{dbkey}} || '(null)',
								$db_disk_part->{filesystem_type},
								$mapval->{value},
								$filesystem_type,
								$db_disk_part->{logical_volume_name},
								$db_disk_part->{logical_volume_id}
						));

					$db_disk_part->{$mapval->{dbkey}} = $mapval->{value};
				}
			}
		}
	}

	foreach my $db_disk_part (
		grep { !exists($_->{seen}) } values %$db_disk_parts
	) {

		if (!defined($ret = runquery(
			description => sprintf('deleting partition logical volume for %s (%d)',
				$db_disk_part->{logical_volume_name},
				$db_disk_part->{logical_volume_id}
				),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			return_type => 'hashref',
			query => q {
				SELECT * FROM
				lv_manip.delete_lv(
					logical_volume_id := ?,
					purge_orphans := true
				)
			},
			args => [
				$db_disk_part->{logical_volume_id}
			]
		))) {
			return undef;
		}

		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			sprintf('Deleted removed partition %s (%d)',
				$db_disk_part->{logical_volume_name},
				$db_disk_part->{logical_volume_id}
			));

	}

	return 1;
}	


sub LVMInventory {
	my $opt = &_options(@_);
	my $r = $opt->{request};
	my $debug = $opt->{debug};
	my $dbh = $opt->{dbh};
	my $force = $opt->{force};
	my $inventory = $opt->{inventory};
	my $device = $opt->{device};

	my $ret;
	#
	# Defer constraints until later, because we play fast and loose with
	# moving some things around
	#

	$dbh->do('SET CONSTRAINTS ALL DEFERRED');

	#
	# If there is no LVM information, then remove anything associated with
	# an LVM type if force is passed.  I'm unsure whether this can actually
	# happen with things that have been cleaned out above, but just make sure
	#
	my $linux_lvm = $inventory->{disks}->{linux_lvm};
	if (!$linux_lvm) {
		my $lvm_pv;
		if (!defined($lvm_pv = runquery(
				description => sprintf(
					'pulling LVM VG information for device %d',
					$device->{device_id}
				),
				request => $r,
				usererror => $opt->{usererror},
				debug => $debug,
				dbh => $dbh,
				allrows => 1,
				return_type => 'hashref',
				hashkey => 'volume_group_id',
				report_error => 0,
				query => q {
					SELECT
						volume_group_id
					FROM
						volume_group vg
					WHERE
						device_id = ? AND
						volume_group_type = 'Linux LVM'
				},
				args => [
					$device->{device_id}
				]
		))) {
			return undef;
		}

		return 1 if (!%$lvm_pv);

		if (!$force) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_NOTICE,
				APR::Const::SUCCESS,
				sprintf('Not removing orphaned LVM information for %s (%d).  Run with force to override this',
					$device->{device_name},
					$device->{device_id}
				));

			return 1;
		}
		my $ret;

		if (!defined($ret = runquery(
			description => sprintf('deleting LVM volume groups for %s (%d)',
				$device->{device_name},
				$device->{device_id}
				),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			return_type => 'hashref',
			query => q {
				SELECT * FROM
				lv_manip.delete_vg(
					volume_group_list := ?,
					purge_orphans := true
				)
			},
			args => [
				[ keys %$lvm_pv ]
			]
		))) {
			return undef;
		}

		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			'Purged removed LVM volume groups',
			);

		return;
	}

	#
	# Deal with any additions or changes to the LVM physicalish_volumes.
	# We don't need to worry about deletes, since those would have been
	# handled from purging elsewhere
	#

	my $db_lvm_pvs;
	if (!defined($db_lvm_pvs = runquery(
			description => sprintf(
				'pulling LVM physicalish_volumes',
			),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			report_error => 0,
			query => q {
				SELECT
					physicalish_volume_id,
					physicalish_volume_name,
					physicalish_volume_type,
					pv.device_id,
					logical_volume_id,
					logical_volume_name
				FROM
					logical_volume lv LEFT JOIN
					physicalish_volume pv USING (logical_volume_id)
				WHERE
					pv.device_id = ? AND
					filesystem_type = 'LVM'
			},
			args => [
				$device->{device_id}
			]
	))) {
		return undef;
	}

	#
	# First, ensure that the attributes on what's currently in the database
	# are current
	#
	foreach my $lv (grep { $_->{physicalish_volume_id} } @$db_lvm_pvs) {
		if (
			$lv->{physicalish_volume_type} ne 'lvm_pv' ||
			$lv->{physicalish_volume_name} ne
				$lv->{logical_volume_name} ||
			$lv->{device_id} != $device->{device_id}
		) {
			if (!defined(runquery(
					description => sprintf(
						'correcting physicalish_volume attributes for %s (id %d)',
						$lv->{logical_volume_name},
						$lv->{physicalish_volume_id}

					),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						UPDATE physicalish_volume
						SET
							physicalish_volume_name = ?,
							physicalish_volume_type = ?,
							device_id = ?
						WHERE
							physicalish_volume_id = ?
					},
					args => [
						$lv->{logical_volume_name},
						'lvm_pv',
						$device->{device_id},
						$lv->{physicalish_volume_id}
					]
			))) {
				return undef;
			}

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS,
				sprintf('Set physicalish_volume id %d to name %s, type os_disk, device_id %d (was %s, %s, %d)',
					$lv->{physicalish_volume_id},
					$lv->{logical_volume_name},
					$device->{device_id},
					$lv->{physicalish_volume_name},
					$lv->{physicalish_volume_type},
					$lv->{device_id}
				));
		}
	}

	foreach my $lv (grep { !($_->{physicalish_volume_id}) } @$db_lvm_pvs) {

		if (!$lv->{physicalish_volume_id}) {
			if (!defined($ret = runquery(
					description => sprintf(
						'inserting new physicalish_volume %s for LVM logical_volume %d',
						$lv->{logical_volume_name},
						$lv->{logical_volume_id}
					),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						INSERT INTO physicalish_volume (
							physicalish_volume_name,
							physicalish_volume_type,
							device_id,
							logical_volume_id
						) VALUES (
							?,
							?,
							?,
							?
						)
						RETURNING *
					},
					args => [
						$lv->{logical_volume_name},
						'lvm_pv',
						$device->{device_id},
						$lv->{logical_volume_id}
					]
			))) {
				return undef;
			}
			map { $lv->{$_} = $ret->{$_} } ( qw(
				physicalish_volume_id
				physicalish_volume_name
				physicalish_volume_type)
				);
			
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS,
				sprintf('Created LVM %s physicalish_volume %s (id %d) for partition %s (id %d)',
					$lv->{physicalish_volume_type},
					$lv->{physicalish_volume_name},
					$lv->{physicalish_volume_id},
					$lv->{logical_volume_name},
					$lv->{logical_volume_id}
				));
			next;
		}
	}

	#
	# Get information for all physicalish_volumes that are of type
	# 'lvm_pv' or 'os_disk' and that either belong to a LVM volume group or
	# are not attached to anything yet (raw disks that have partition tables
	# on them can not be assigned as an LVM PV)
	#
	if (!defined($db_lvm_pvs = runquery(
			description => sprintf(
				'pulling LVM PV information for device %d',
				$device->{device_id}
			),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'physicalish_volume_id',
			report_error => 0,
			query => q {
				SELECT
					physicalish_volume_id,
					physicalish_volume_name,
					physicalish_volume_type,
					volume_group_id,
					volume_group_name
				FROM
					physicalish_volume pv LEFT JOIN
					volume_group_physicalish_vol vgpv USING (physicalish_volume_id) LEFT JOIN
					volume_group vg USING (volume_group_id)
				WHERE
					pv.device_id = ? AND
					physicalish_volume_type IN ('os_disk', 'lvm_pv') AND
					(volume_group_type IS NULL OR
					 volume_group_type = 'Linux LVM')
			},
			args => [
				$device->{device_id}
			]
	))) {
		return undef;
	}

	my $db_lvm_vgs;
	if (!defined($db_lvm_vgs = runquery(
			description => sprintf(
				'pulling LVM VG information for device %d',
				$device->{device_id}
			),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'volume_group_id',
			report_error => 0,
			query => q {
				SELECT
					volume_group_id,
					volume_group_name,
					volume_group_type,
					volume_group_size_in_bytes,
					raid_type
				FROM
					volume_group vg
				WHERE
					device_id = ? AND
					volume_group_type = 'Linux LVM'
			},
			args => [
				$device->{device_id}
			]
	))) {
		return undef;
	}

	my $db_lvm_lvs;
	if (!defined($db_lvm_lvs = runquery(
			description => sprintf(
				'pulling LVM LV information for device %d',
				$device->{device_id}
			),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			allrows => 1,
			return_type => 'hashref',
			hashkey => 'logical_volume_id',
			report_error => 0,
			query => q {
				SELECT
					lv.logical_volume_id,
					vg.volume_group_id,
					logical_volume_name,
					volume_group_name,
					logical_volume_size_in_bytes,
					lv.filesystem_type,
					serial.logical_volume_property_value AS serial,
					serial.logical_volume_property_id AS serial_id,
					mp.logical_volume_property_value AS mount_point,
					mp.logical_volume_property_id AS mount_point_id,
					label.logical_volume_property_value AS label,
					label.logical_volume_property_id AS label_id
				FROM
					logical_volume lv JOIN
					volume_group vg USING (volume_group_id) LEFT JOIN
					logical_volume_property serial ON
						(lv.logical_volume_id = serial.logical_volume_id AND
						 serial.logical_volume_property_name = 'Serial')
						 LEFT JOIN
					logical_volume_property mp ON
						(lv.logical_volume_id = mp.logical_volume_id AND
						 mp.logical_volume_property_name = 'MountPoint')
						 LEFT JOIN
					logical_volume_property label ON
						(lv.logical_volume_id = label.logical_volume_id AND
						 label.logical_volume_property_name = 'Label')
				WHERE
					lv.device_id = ? AND
					vg.volume_group_type = 'Linux LVM'
			},
			args => [
				$device->{device_id}
			]
	))) {
		return undef;
	}

	my $probed_disks = $linux_lvm->{physical_volumes};
	#
	# Remove any physicalish_volumes which are now not present but have a
	# volume group assigned in the database
	#
	my $pv_list = [
		map {
			my $x = $_;
			if (!grep {
					($_->{physical_volume} eq $x->{physicalish_volume_name}) &&
					$x->{volume_group_id}
				} @$probed_disks) {
				delete $db_lvm_pvs->{$_->{physicalish_volume_id}};
				($_->{physicalish_volume_id})
			} else {
				()
			}
		} values %$db_lvm_pvs
	];

#	if (@$pv_list) {
#		$r->log_rerror(
#			Apache2::Log::LOG_MARK,
#			Apache2::Const::LOG_INFO,
#			APR::Const::SUCCESS,
#			sprintf("Removing orphaned LVM physicalish_volume(s) %s",
#				join(',', @$pv_list))
#		);
#
#		if (!defined($ret = runquery(
#			description => 'deleting orphaned LVM physicalish_volumes',
#			request => $r,
#			usererror => $opt->{usererror},
#			debug => $debug,
#			dbh => $dbh,
#			return_type => 'hashref',
#			query => q {
#				SELECT * FROM
#				lv_manip.delete_pv(
#					physicalish_volume_list := ?,
#					purge_orphans := true
#				)
#			},
#			args => [
#				$pv_list
#			]
#		))) {
#			return undef;
#		}
#	}

	foreach my $disk (@$probed_disks) {
		my $db_disk = (grep { $_->{physicalish_volume_name} eq
				$disk->{physical_volume} } values %$db_lvm_pvs)[0];
	
		#
		# Ensure all of the volume groups are correct.  We can't store UUID
		# in the database to determine renames yet, so they'll get deleted and
		# repopulated for now if that changes
		#

		foreach my $vg (@{$linux_lvm->{volume_groups}}) {
			my $db_lvm_vg;

			if (!($db_lvm_vg = (grep {
					$_->{volume_group_name} eq $vg->{volume_group}
				} (values %{$db_lvm_vgs}))[0]))
			{
				#
				# VG does not exist, so create it.  This should be moved to
				# a stored procedure
				#
				if (!defined($db_lvm_vg = runquery(
						description => sprintf(
							'inserting new LVM volume group for %s',
							$vg->{volume_group}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							INSERT INTO volume_group (
								volume_group_name,
								volume_group_type,
								device_id,
								volume_group_size_in_bytes
							) VALUES (
								?,
								?,
								?,
								?
							)
							RETURNING *
						},
						args => [
							$vg->{volume_group},
							'Linux LVM',
							$device->{device_id},
							$vg->{size}
						]
				))) {
					return undef;
				}

				$db_lvm_vgs->{$db_lvm_vg->{volume_group_id}} = $db_lvm_vg;

				if (!defined($dbh->do("SAVEPOINT lvm"))) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_ERR,
						APR::Const::SUCCESS,
						'Error creating LVM savepoint'
					);
					return undef;
				}

				if (!defined($ret = runquery(
						description => sprintf(
							'executing local hooks for inserted LVM volume group %s (%d)',
							$db_lvm_vg->{volume_group_name},
							$db_lvm_vg->{volume_group_id},
						),
						usererror => $opt->{usererror},
						dbh => $dbh,
						query => q {
							SELECT * FROM device_provisioning_local_hooks.post_create_volume_group (
								volume_group_id := ?
							)
						},
						args => [ $db_lvm_vg->{volume_group_id} ]
				))) {
					#
					# Look for 'invalid_schema_name' or 'undefined_function'
					# errors
					#
					if ($dbh->state eq '3F000' || $dbh->state eq '42883') {

						$r->log_rerror(
							Apache2::Log::LOG_MARK,
							Apache2::Const::LOG_DEBUG,
							APR::Const::SUCCESS,
							'device_provisioning_local_hooks.post_create_volume_group not found; skipping'
						);
						if (!defined($dbh->do("ROLLBACK TO SAVEPOINT lvm"))) {
							$r->log_rerror(
								Apache2::Log::LOG_MARK,
								Apache2::Const::LOG_ERR,
								APR::Const::SUCCESS,
								'Error rolling back LVM savepoint'
							);
							return undef;
						}
					} else {
						$r->log_rerror(
							Apache2::Log::LOG_MARK,
							Apache2::Const::LOG_ERR,
							APR::Const::SUCCESS,
							sprintf('Error executing LVM local hooks: %s',
								$dbh->errstr)
						);
						return undef;
					}
					if (!defined($dbh->do("RELEASE SAVEPOINT lvm"))) {
						$r->log_rerror(
							Apache2::Log::LOG_MARK,
							Apache2::Const::LOG_ERR,
							APR::Const::SUCCESS,
							'Error releasing LVM savepoint'
						);
						return undef;
					}
				}

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Created LVM volume_group %s (id %d)',
						$db_lvm_vg->{volume_group_name},
						$db_lvm_vg->{volume_group_id}
					));
			} else {
				#
				# Make sure the volume group and volume group type are set
				# correctly
				#
				if (!(
					$db_lvm_vg->{volume_group_name} ~~ $vg->{volume_group} &&
					$db_lvm_vg->{volume_group_type} ~~ 'Linux LVM' &&
					$db_lvm_vg->{volume_group_size_in_bytes} ~~ $vg->{size}
				)) {
					if (!defined($ret = runquery(
							description => sprintf(
								'updating LVM volume group for disk %s (%d)',
								$vg->{volume_group},
								$db_lvm_vg->{volume_group_id}
							),
							request => $r,
							usererror => $opt->{usererror},
							debug => $debug,
							dbh => $dbh,
							return_type => 'hashref',
							report_error => 0,
							query => q {
								UPDATE volume_group SET
									volume_group_name = ?,
									volume_group_type = ?,
									volume_group_size_in_bytes = ?
								WHERE
									volume_group_id = ?
								RETURNING *
							},
							args => [
								$vg->{volume_group},
								'Linux LVM',
								$vg->{size},
								$db_lvm_vg->{volume_group_id}
							]
					))) {
						return undef;
					}
					map { $db_lvm_vg->{$_} = $ret->{$_} } ( qw(
						volume_group_id
						volume_group_name
						volume_group_type
						volume_group_size_in_bytes
						raid_type)
						);
					
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_INFO,
						APR::Const::SUCCESS,
						sprintf(
							'Updated attributes for LVM volume_group %s (id %d)',
							$db_lvm_vg->{volume_group_name},
							$db_lvm_vg->{volume_group_id}
						));

				}
			}

			if (!defined($dbh->do("SAVEPOINT lvm"))) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_ERR,
					APR::Const::SUCCESS,
					'Error creating LVM savepoint'
				);
				return undef;
			}

			if (!defined($ret = runquery(
				dbh => $dbh,
				query => q {
					SELECT * FROM device_provisioning_local_hooks.should_manage_volume_group (
						 volume_group_id := ?
					)
				},
				args => [ $db_lvm_vg->{volume_group_id} ]
			))) {
				#
				# Look for 'invalid_schema_name' or 'undefined_function'
				# errors
				#
				if ($dbh->state eq '3F000' || $dbh->state eq '42883') {

					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_DEBUG,
						APR::Const::SUCCESS,
						'device_provisioning_local_hooks.post_create_volume_group not found; skipping'
					);
					if (!defined($dbh->do("ROLLBACK TO SAVEPOINT lvm"))) {
						$r->log_rerror(
							Apache2::Log::LOG_MARK,
							Apache2::Const::LOG_ERR,
							APR::Const::SUCCESS,
							'Error rolling back LVM savepoint'
						);
						return undef;
					}
				} else {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_ERR,
						APR::Const::SUCCESS,
						sprintf('Error executing LVM local hooks: %s',
							$dbh->errstr)
					);
					return undef;
				}
				if (!defined($dbh->do("RELEASE SAVEPOINT lvm"))) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_ERR,
						APR::Const::SUCCESS,
						'Error releasing LVM savepoint'
					);
					return undef;
				}
			}
			$db_lvm_vg->{manage_vg} = $ret ? $ret->[0] : '1';
		}

		#
		# Remove any volume groups which are now not present on the system
		# but which are in the database
		#
		my $vg_list = [
			map {
				my $x = $_;
				if (!grep {
						$_->{volume_group} eq $x->{volume_group_name}
					} @{$linux_lvm->{volume_groups}})
				{
					delete $db_lvm_vgs->{$_->{volume_group_id}};
					($_->{volume_group_id});
				} else {
					()
				}
			} values %$db_lvm_vgs
		];

		if (@$vg_list) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS,
				sprintf("Removing deleting LVM volume_group(s) %s",
					join(',', @$vg_list))
			);

			if (!defined($ret = runquery(
				description => 'deleting orphaned LVM volume_group',
				request => $r,
				usererror => $opt->{usererror},
				debug => $debug,
				dbh => $dbh,
				return_type => 'hashref',
				query => q {
					SELECT * FROM
					lv_manip.delete_vg(
						volume_group_list := ?,
						purge_orphans := true
					)
				},
				args => [
					$vg_list
				]
			))) {
				return undef;
			}
		}

		##
		## Validate volume group membership
		##

		#
		# physicalish_volumes are correct, so we just need to go through
		# them and make sure they match
		#

		foreach my $pv (values %$db_lvm_pvs) {
			my $os_pv = grep {
				$_->{physical_volume} eq $pv->{physicalish_volume_name}
			} @$db_lvm_pvs;
			#
			# If this is an os_disk and it's not listed as a physical volume
			# in LVM, or the physical_volume does not have a volume_group
			# assigned, make sure it's cleared
			#
			if ((!$os_pv && $pv->{physicalish_volume_type} eq 'os_disk') ||
					($os_pv && !$os_pv->{volume_group})) {
				if (!$pv->{volume_group_id}) {
					next;
				}

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf("Removing LVM physicalish_volume %s (%d) from volume_group %s (%d)",
						$pv->{physicalish_volume_name},
						$pv->{physicalish_volume_id},
						$pv->{volume_group_name},
						$pv->{volume_group_id}
					)
				);

				if (!defined($ret = runquery(
					description => 'removing LVM physicalish_volume from volume_group',
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						DELETE FROM
							volume_group_physicalish_vol
						WHERE
							volume_group_id = ? AND
							physicalish_volume_id = ?
					},
					args => [
						$pv->{volume_group_id},
						$pv->{physicalish_volume_id}
					]
				))) {
					return undef;
				}
				next;
			}
			if (!$os_pv) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_ERR,
					APR::Const::SUCCESS,
					sprintf("Database LVM physicalish_volume %s (%d) not detected on system.  This should not happen",
						$pv->{physicalish_volume_name},
						$pv->{physicalish_volume_id}
					)
				);

				next;
			}

			my $new_vg = (grep {
				$_->{volume_group_name} eq $os_pv->{volume_group}
			} @$db_lvm_vgs)[0] ;

			if (!$new_vg) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_ERR,
					APR::Const::SUCCESS,
					sprintf("Volume group not found for %s.  This should not happen",
						$os_pv->{volume_group},
					)
				);

				return undef;
			}

			if (!$pv->{volume_group_id}) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf("Adding LVM physicalish_volume %s (%d) to volume group %s (%d)",
						$pv->{physicalish_volume_name},
						$pv->{physicalish_volume_id},
						$new_vg->{volume_group_name},
						$new_vg->{volume_group_id}
					)
				);

				if (!defined($ret = runquery(
					description => 'adding physicalish_volume to volume_group',
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						INSERT INTO volume_group_physicalish_vol (
							physicalish_volume_id,
							volume_group_id,
							device_id,
							volume_group_relation
						) VALUES (
							?,
							?,
							?,
							?
						)
					},
					args => [
						$pv->{physicalish_volume_id},
						$new_vg->{volume_group_id},
						$device->{device_id},
						'member_disk'
					]
				))) {
					return undef;
				}
				next;
			}

			#
			# If the physicalish_volume is assigned to a different volume_group,
			# move it
			#
			if ($os_pv->{volume_group} ne $pv->{volume_group_name}) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf("moving LVM physicalish_volume %s (%d) to new volume group %s (%d)",
						$pv->{physicalish_volume_name},
						$pv->{physicalish_volume_id},
						$new_vg->{volume_group_name},
						$new_vg->{volume_group_id}
					)
				);

				if (!defined($ret = runquery(
					description => 'moving physicalish_volume to new volume_group',
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						UPDATE
							volume_group_physicalish_vol
						SET
							volume_group_id = ?,
							volume_group_primary_pos = NULL
						WHERE
							physicalish_volume_id = ?
					},
					args => [
						$new_vg->{volume_group_id},
						$pv->{physicalish_volume_id}
					]
				))) {
					return undef;
				}
				next;
			}
		}

		#
		# Volume group is okay, now process the logical volumes
		#

		#
		# Do this for convenience
		#
		my $db_vgs_by_name = {
			map {
				$_->{volume_group_name} => $_
			} values %$db_lvm_vgs
		};

		foreach my $lv (@{$linux_lvm->{logical_volumes}}) {
			#
			# Look for the logical_volume based on name, because we
			# can't store UUID easily right now
			#
			my $filesystem_type = $lv->{filesystem_type} || 'unknown';

			#
			# Determine whether we need to manage volume groups for this
			# logical volume.  For now this is a stored procedure, but
			# we'll change this to a property at some point.
			#

			my $db_vg = $db_vgs_by_name->{$lv->{volume_group}};

			if (!$force && !($db_vg->{manage_vg})) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_DEBUG,
					APR::Const::SUCCESS,
					sprintf('Not managing LVM logical_volume %s',
						$lv->{logical_volume})
				);
				next;
			}
			my $db_lv;
#			if ($lv->{uuid}) {
#				$db_lv = (grep {
#					$_->{uuid} eq $lv->{uuid}
#				} values %$db_lvm_lvs)[0];
#			}
			if (!$db_lv) {
				$db_lv = (grep {
					$_->{logical_volume_name} eq $lv->{logical_volume} &&
					$_->{volume_group_name} eq $lv->{volume_group}
				} values %$db_lvm_lvs)[0];
			}
			if (!$db_lv) {
				#
				# Create a new logical_volume
				#

				if (!defined($ret = runquery(
						description => sprintf(
							'inserting new LVM logical_volume for %s-%s',
							$lv->{volume_group},
							$lv->{logical_volume}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							INSERT INTO logical_volume (
								volume_group_id,
								device_id,
								logical_volume_name,
								logical_volume_size_in_bytes,
								filesystem_type
							) VALUES (
								?,
								?,
								?,
								?,
								?
							)
							RETURNING *
						},
						args => [
							$db_vgs_by_name->{$lv->{volume_group}}->
								{volume_group_id},
							$device->{device_id},
							$lv->{logical_volume},
							$lv->{size},
							$filesystem_type
						]
				))) {
					return undef;
				}

				$db_lv = $ret;
				$db_lv->{volume_group_name} = $lv->{volume_group};
			
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_DEBUG,
					APR::Const::SUCCESS,
					Data::Dumper->Dump( [$db_lv], ['$db_lv'])
					);

				$db_lvm_lvs->{$ret->{logical_volume_id}} = $db_lv;

				if ($filesystem_type =~ /^ext/ || $filesystem_type eq 'xfs') {
					my $propmap = [
						{
							localkey => 'serial',
							dbkey => 'Serial',
							value => $lv->{uuid}
						},
						{
							localkey => 'mount_point',
							dbkey => 'MountPoint',
							value => $lv->{mount_point}
						},
					];

					foreach my $v (@$propmap) {
						if (!defined($ret = runquery(
								description => sprintf(
									'inserting new LVM logical_volume properties for %s-%s (%d)',
									$db_lv->{volume_group_name},
									$db_lv->{logical_volume_name},
									$db_lv->{logical_volume_id}
								),
								request => $r,
								usererror => $opt->{usererror},
								debug => $debug,
								dbh => $dbh,
								return_type => 'hashref',
								report_error => 0,
								query => q {
									INSERT INTO logical_volume_property (
										logical_volume_id,
										filesystem_type,
										logical_volume_property_name,
										logical_volume_property_value
									) VALUES (
										?,
										?,
										?,
										?
									)
									RETURNING *
								},
								args => [
									$db_lv->{logical_volume_id},
									$filesystem_type,
									$v->{dbkey},
									$v->{value}
								]
						))) {
							return undef;
						}
						$db_lv->{$v->{localkey}} =
							$ret->{logical_volume_property_value};
						
						$db_lv->{$v->{localkey} . '_id'} =
							$ret->{logical_volume_property_id};
					}
				}

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Created LVM logical_volume %s (id %d), filesystem_type %s for volume group %s (%d)',
						$db_lv->{logical_volume_name},
						$db_lv->{logical_volume_id},
						$db_lv->{filesystem_type},
						$db_lv->{volume_group_name},
						$db_lv->{volume_group_id},
					));

				if (!defined($ret = runquery(
						description => sprintf(
							'executing local hooks for inserted LVM logical_volume %s-%s (%d)',
							$db_lv->{volume_group_name},
							$db_lv->{logical_volume_name},
							$db_lv->{logical_volume_id},
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => sprintf(q {
							DO $$
								BEGIN
									PERFORM device_provisioning_local_hooks.post_create_logical_volume (
										logical_volume_id := %s
									);
								EXCEPTION WHEN invalid_schema_name OR
										undefined_function THEN
									NULL;
								END;
							$$ language plpgsql
						}, 
							$db_lv->{logical_volume_id}
						)
				))) {
					return undef;
				}

				$db_lv->{seen} = 1;
				next;
			}

			$db_lv->{seen} = 1;

			if (
				!($db_lv->{logical_volume_name} ~~ $lv->{logical_volume}) ||
				($filesystem_type ne 'unknown' &&
					!($db_lv->{filesystem_type} ~~ $filesystem_type)) ||
				!($db_lv->{logical_volume_size_in_bytes} ~~ $lv->{size})
			) {
				if (!($db_lv->{filesystem_type} ~~ $filesystem_type)) {
					if (!defined($ret = runquery(
							description => sprintf(
								'removing LVM logical volume properties for partition %s-%s (%d)',
								$db_lv->{volume_group_name},
								$db_lv->{logical_volume_name},
								$db_lv->{logical_volume_id}
							),
							request => $r,
							usererror => $opt->{usererror},
							debug => $debug,
							dbh => $dbh,
							return_type => 'hashref',
							report_error => 0,
							query => q {
								DELETE FROM logical_volume_property
								WHERE
									logical_volume_id = ? AND
									filesystem_type = ?
							},
							args => [
								$db_lv->{logical_volume_id},
								$db_lv->{filesystem_type}
							]
					))) {
						return undef;
					}

					map {
						$db_lv->{$_} = undef;
						$db_lv->{$_ . '_id'} = undef;
					} (qw(label serial mount_point));
				}
				if (!defined($ret = runquery(
						description => sprintf(
							'updating LVM logical volume for partition %s-%s (%d)',
							$db_lv->{volume_group_name},
							$db_lv->{logical_volume_name},
							$db_lv->{logical_volume_id}
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => q {
							UPDATE logical_volume SET
								logical_volume_name = ?,
								filesystem_type = ?,
								logical_volume_size_in_bytes = ?
							WHERE
								logical_volume_id = ?
							RETURNING *
						},
						args => [
							$lv->{logical_volume},
							$filesystem_type,
							$lv->{size},
							$db_lv->{logical_volume_id}
						]
				))) {
					return undef;
				}
				map { $db_lv->{$_} = $ret->{$_} } ( qw(
					logical_volume_name
					filesystem_type
					logical_volume_size_in_bytes
					));
				
				if (!defined($ret = runquery(
						description => sprintf(
							'executing local hooks for updated LVM logical_volume %s-%s (%d)',
							$db_lv->{logical_volume_name},
							$db_lv->{volume_group_name},
							$db_lv->{logical_volume_name},
						),
						request => $r,
						usererror => $opt->{usererror},
						debug => $debug,
						dbh => $dbh,
						return_type => 'hashref',
						report_error => 0,
						query => sprintf(q {
							DO $$
								BEGIN
									PERFORM device_provisioning_local_hooks.post_update_logical_volume (
										logical_volume_id := %s
									);
								EXCEPTION WHEN invalid_schema_name OR
										undefined_function THEN
									NULL;
								END;
							$$ language plpgsql
						}, $db_lv->{logical_volume_id})
				))) {
					return undef;
				}

				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_INFO,
					APR::Const::SUCCESS,
					sprintf('Updated attributes for LVM logical_volume %s-%s (%d)',
						$db_lv->{volume_group_name},
						$db_lv->{logical_volume_name},
						$db_lv->{logical_volume_id},
					));

			}

			if ($filesystem_type =~ /^ext/ || $filesystem_type eq 'xfs') {
				my $propmap = [
					{
						localkey => 'serial',
						dbkey => 'Serial',
						value => $lv->{uuid}
					},
					{
						localkey => 'mount_point',
						dbkey => 'MountPoint',
						value => $lv->{mount_point}
					},
				];
				foreach my $mapval (@$propmap) {
					next if ($db_lv->{$mapval->{localkey}} ~~
							$mapval->{value});

					my $q;
					if (!defined($db_lv->{$mapval->{localkey} . '_id'})) {
						$q = q {
							INSERT INTO logical_volume_property (
								logical_volume_property_value,
								filesystem_type,
								logical_volume_id,
								logical_volume_property_name
							) VALUES (
								?,
								?,
								?,
								?
							)
							RETURNING *
						};
					} else {
						$q = q {
							UPDATE logical_volume_property
							SET
								logical_volume_property_value = ?,
								filesystem_type = ?
							WHERE
								logical_volume_id = ? AND
								logical_volume_property_name = ?
							RETURNING *
						};
					}
								
					if (!defined($ret = runquery(
							description => sprintf(
								'updating LVM logical_volume_property %s to %s for %s',
								$mapval->{dbkey},
								$mapval->{value},
								$db_lv->{logical_volume_name}
							),
							request => $r,
							usererror => $opt->{usererror},
							debug => $debug,
							dbh => $dbh,
							return_type => 'hashref',
							report_error => 0,
							query => $q,
							args => [
								$mapval->{value},
								$db_lv->{filesystem_type},
								$db_lv->{logical_volume_id},
								$mapval->{dbkey}
							]
					))) {
						return undef;
					}

					$db_lv->{$mapval->{localkey} . '_id'} =
						$ret->{logical_volume_property_id};
					$db_lv->{$mapval->{localkey}} = $mapval->{value};
					
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_INFO,
						APR::Const::SUCCESS,
						sprintf(
								'updated LVM logical_volume_property %s to %s for %s-%s (%d)',
								$mapval->{dbkey},
								$mapval->{value},
								$db_lv->{volume_group_name},
								$db_lv->{logical_volume_name},
								$db_lv->{logical_volume_id}
						));
				}
			}
		}
	}

	foreach my $db_lv (
		grep { 
			(!exists($_->{seen})) && 
			($force ||
				$db_lvm_vgs->{$_->{volume_group_id}}->{manage_vg} == 1)
		} values %$db_lvm_lvs
	) {
		
		if (!defined($ret = runquery(
			description => sprintf('deleting removed LVM logical volume for %s-%s (%d)',
				$db_lv->{volume_group_name},
				$db_lv->{logical_volume_name},
				$db_lv->{logical_volume_id}
				),
			request => $r,
			usererror => $opt->{usererror},
			debug => $debug,
			dbh => $dbh,
			return_type => 'hashref',
			query => q {
				SELECT * FROM
				lv_manip.delete_lv(
					logical_volume_id := ?,
					purge_orphans := true
				)
			},
			args => [
				$db_lv->{logical_volume_id}
			]
		))) {
			return undef;
		}

		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			sprintf('Deleted removed LVM logical_volume %s-%s (%d)',
				$db_lv->{volume_group_name},
				$db_lv->{logical_volume_name},
				$db_lv->{logical_volume_id}
			));

	}

	return 1;
}	

sub MemoryInventory {
	my $opt = &_options(@_);
	my $r = $opt->{request};
	my $debug = $opt->{debug};
	my $dbh = $opt->{dbh};
	my $force = $opt->{force};
	my $inventory = $opt->{inventory};
	my $device = $opt->{device};

	#
	# Defer constraints until later, because we play fast and loose with
	# moving some things around
	#

	$dbh->do('SET CONSTRAINTS ALL DEFERRED');

	###
	### Pull memory for this client out of the database
	###

	my $ret;
	my $db_memory;
	if (!defined($db_memory = runquery(
		description => 'fetching memory',
		request => $r,
		usererror => $opt->{usererror},
		debug => $debug,
		dbh => $dbh,
		allrows => 1,
		return_type => 'hashref',
		query => q {
			WITH x AS (
				SELECT
					component_type_id,
					array_agg(component_function) as functions
				FROM
					component_type_component_func
				GROUP BY
					component_type_id
			) SELECT
				c.component_id,
				s.slot_id,
				asset_id,
				serial_number,
				slot_name,
				model
			FROM
				component c JOIN
				component_type ct USING (component_type_id) JOIN
				x USING (component_type_id) JOIN
				slot s ON (c.parent_slot_id = s.slot_id) LEFT JOIN
				asset a ON (c.component_id = a.component_id)
			WHERE
				s.component_id = ? AND
				'memory' = ANY(functions)
		},
		args => [
			$device->{component_id}
		]
	))) {
		return Apache2::Const::OK;
	}

	my $probed_memory = $inventory->{system}->{memory};

	foreach my $db_mem (@{$db_memory}) {
		if ($debug) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf(
					'Looking for memory in slot %s from database in inventory request',
					$db_mem->{slot_name}
				)
			);
		}

		my $target = FindHashChild(
			hash => $probed_memory,
			key => "locator",
			value => $db_mem->{slot_name}
		);
		my $memory;
		if (@$target) {
			$memory = $target->[0];
		}
		if (!$memory || $memory->{part_number} eq 'NO DIMM') {
			#
			# This memory seems to have disappeared
			#
			if (!$force) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_NOTICE,
					APR::Const::SUCCESS,
					sprintf(
						'Memory in slot %s for %s (device_id %d) seems to have disappeared',
						$db_mem->{slot_name},
						$device->{device_name},
						$device->{device_id}
					)
				);
			} else {
				if ($debug) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_INFO,
						APR::Const::SUCCESS,
						sprintf(
							'Removing memory from slot %s in the database (not present in inventory)',
							$db_mem->{slot_name}
						)
					);
				}
				#
				# If force is run, clean out everything associated with this
				# controller
				#

				if (!defined($ret = runquery(
					description => sprintf(
						'shelving memory component %d',
						$db_mem->{component_id}
						),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						SELECT * FROM
						component_utils.remove_component_hier(
							component_id := ?
						)
					},
					args => [
						$db_mem->{component_id}
					]
				))) {
					return undef;
				}
			}
			next;
		}
	}

	###
	### Detect added/replaced memory
	###

	my $slots_seen = {};
	foreach my $dmi_mem (@{$probed_memory}) {
		next if (
			!defined($dmi_mem->{memory_size}) ||
			$dmi_mem->{memory_size} eq 'No Module Installed'
		);

		if ($dmi_mem->{memory_type} ~~ '<OUT OF SPEC>') {
			$dmi_mem->{memory_type} = 'DDR3';
		}

		#
		# Some memory overloads the vendor field with manufacture date
		# information
		#
		$dmi_mem->{vendor} =~ s/\(.*//;
		my ($slot_name) = $dmi_mem->{locator};

		next if (exists($slots_seen->{$slot_name}));
		$slots_seen->{$slot_name} = $dmi_mem;
		my $memory = FindHashChild(
			hash => FindHashChild(
				hash => $inventory->{lshw_output},
				key => 'class',
				value => 'memory'
			),
			key => 'slot',
			value => $slot_name
		);

		if (!$memory) {
			next;
		} else {
			$memory = $memory->[0];
		}

		my $target = FindHashChild(
			hash => $db_memory,
			key => "slot_name",
			value => $slot_name
		);
		my $db_mem;
		#
		# Do some things to make things consistent, because lots of things
		# suck
		#
		if (!exists($memory->{serial})) {
			$memory->{serial} = $memory->{serial_number};
		}
		if (!exists($memory->{product})) {
			$memory->{product} = $memory->{part_number};
		}
		#
		# There can only be one here
		#
		if (@$target) {
			$db_mem = $target->[0];

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf('Found memory for slot %s in the database as component_id %d',
					$slot_name,
					$db_mem->{component_id}
				));
		}
		#
		# If either the memory does not exist in the database, or the
		# model or serial number has changed, then fetch a replacement
		# component
		#

		if (
			!$db_mem ||
			!($db_mem->{serial_number} ~~ $memory->{serial}) ||
			!($db_mem->{model} ~~ $memory->{product})
		) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf(q{Inventoried memory does not match database:
					DB Model:      %s
					Probed model:  %s
					DB Serial:     %s
					Probed Serial: %s
				},
					$db_mem ? $db_mem->{model} || '' : '',
					$memory->{product} || '',
					$db_mem ? $db_mem->{serial_number} || '' : '',
					$memory->{serial} || '',
				));

			
			#
			# If we're replacing the memory, shelve the old one first
			#
			if ($db_mem) {
				if (!defined($ret = runquery(
					description => sprintf(
						'shelving memory component %d',
						$db_mem->{component_id}
						),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						SELECT * FROM
						component_utils.remove_component_hier(
							component_id := ?
						)
					},
					args => [
						$db_mem->{component_id}
					]
				))) {
					return undef;
				}
			}
			#
			# Fetch a component for the memory doober
			#

			if (!defined($db_mem = runquery(
					description => 'inserting memory component',
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						SELECT * FROM
							component_utils.insert_memory_component(
								model := ?,
								memory_size := ?,
								memory_speed := ?,
								memory_type := ?,
								vendor_name := ?,
								serial_number := ?
							)
					},
					args => [
						$memory->{product},
						$memory->{size} / 1048576,
						$memory->{clock} / 1000000,
						$dmi_mem->{memory_type} || 'DDR3',
						$memory->{vendor},
						$memory->{serial}
					]
			))) {
				return undef;
			}

			if ($db_mem->{parent_slot_id}) {
				if (!$force) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_ERR,
						APR::Const::SUCCESS,
						sprintf("Memory component_id %s in slot %s is already attached to slot_id %d",
							$db_mem->{component_id},
							$slot_name,
							$db_mem->{parent_slot_id}
						));
					return undef;
				}
			}

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS,
				sprintf("Retrieved component_id %s for new %s %s %sMB %s memory with serial %s in slot %s",
					$db_mem->{component_id},
					$memory->{vendor} || '',
					$memory->{product} || '',
					$memory->{size} / 1048576,
					$dmi_mem->{memory_type} || 'DDR3',
					$memory->{serial} || '',
					$slot_name
				));
			my $slot;
			if (!defined($slot = runquery(
					description => 'attaching memory component to parent slot',
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						SELECT * FROM
							component_utils.insert_component_into_parent_slot(
								parent_component_id := ?,
								component_id := ?,
								slot_name := ?,
								slot_type := ?,
								slot_function := 'memory'
							)
					},
					args => [
						$device->{component_id},
						$db_mem->{component_id},
						$slot_name,
						$dmi_mem->{memory_type} || 'DDR3'
					]
			))) {
				return undef;
			}
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf("Attached component_id %d to slot_id %d of component %d",
					$db_mem->{component_id},
					$slot->{slot_id},
					$device->{component_id}
				));
		} else {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf('Probed memory in slot %s matches database',
					$slot_name,
				));
		}
	}
	return 1;
}

sub CPUInventory {
	my $opt = &_options(@_);
	my $r = $opt->{request};
	my $debug = $opt->{debug};
	my $dbh = $opt->{dbh};
	my $force = $opt->{force};
	my $inventory = $opt->{inventory};
	my $device = $opt->{device};

	#
	# Defer constraints until later, because we play fast and loose with
	# moving some things around
	#

	$dbh->do('SET CONSTRAINTS ALL DEFERRED');

	###
	### Pull processors for this client out of the database
	###

	my $ret;
	my $db_cpus;
	if (!defined($db_cpus = runquery(
		description => 'fetching processors',
		request => $r,
		usererror => $opt->{usererror},
		debug => $debug,
		dbh => $dbh,
		allrows => 1,
		return_type => 'hashref',
		query => q {
			WITH x AS (
				SELECT
					component_type_id,
					array_agg(component_function) as functions
				FROM
					component_type_component_func
				GROUP BY
					component_type_id
			) SELECT
				c.component_id,
				s.slot_id,
				asset_id,
				serial_number,
				slot_name,
				model
			FROM
				component c JOIN
				component_type ct USING (component_type_id) JOIN
				x USING (component_type_id) JOIN
				slot s ON (c.parent_slot_id = s.slot_id) LEFT JOIN
				asset a ON (c.component_id = a.component_id)
			WHERE
				s.component_id = ? AND
				'CPU' = ANY(functions)
		},
		args => [
			$device->{component_id}
		]
	))) {
		return Apache2::Const::OK;
	}

	my $probed_cpu = $inventory->{system}->{processor};

	foreach my $db_cpu (@{$db_cpus}) {
		if ($debug) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf(
					'Looking for CPU in slot %s from database in inventory request',
					$db_cpu->{slot_name}
				)
			);
		}

		my $target = FindHashChild(
			hash => $probed_cpu,
			key => "socket",
			value => $db_cpu->{slot_name}
		);

		my $cpu;
		if (@$target) {
			$cpu = $target->[0];
		}

		if ($cpu->{model} eq 'Not Specified') {
			$cpu = undef;
		}

		$cpu->{model} =~ s/^\s*(.*\S*)\s*$/$1/;


		if (!$cpu) {
			#
			# This CPU seems to have disappeared
			#
			if (!$force) {
				$r->log_rerror(
					Apache2::Log::LOG_MARK,
					Apache2::Const::LOG_NOTICE,
					APR::Const::SUCCESS,
					sprintf(
						'CPU in slot %s for %s (device_id %d) seems to have disappeared',
						$db_cpu->{slot_name},
						$device->{device_name},
						$device->{device_id}
					)
				);
			} else {
				if ($debug) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_INFO,
						APR::Const::SUCCESS,
						sprintf(
							'Removing CPU from slot %s in the database (not present in inventory)',
							$db_cpu->{slot_name}
						)
					);
				}

				if (!defined($ret = runquery(
					description => sprintf(
						'shelving CPU component %d',
						$db_cpu->{component_id}
						),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						SELECT * FROM
						component_utils.remove_component_hier(
							component_id := ?
						)
					},
					args => [
						$db_cpu->{component_id}
					]
				))) {
					return undef;
				}
			}
			next;
		}
	}

	###
	### Detect added/replaced CPU
	###

	if (ref($inventory->{system}->{processor}) ne 'ARRAY') {
		$inventory->{system}->{processor} = 
			[ $inventory->{system}->{processor} ];
	}

	foreach my $cpu (@{$inventory->{system}->{processor}}) {
		my ($slot_name) = $cpu->{socket};

		if (!$cpu->{model} || $cpu->{model} eq 'Not Specified') {
			next;
		}

		if (!$cpu->{socket_type} || $cpu->{socket_type} ~~ '<OUT OF SPEC>') {
			$cpu->{socket_type} = 'Other';
		}


		$cpu->{model} =~ s/^\s*(.*\S*)\s*$/$1/;

		my $target = FindHashChild(
			hash => $db_cpus,
			key => "slot_name",
			value => $slot_name
		);
		my $db_cpu;
		#
		# There can only be one here
		#
		if (@$target) {
			$db_cpu = $target->[0];

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf('Found CPU for slot %s in the database as component_id %d',
					$slot_name,
					$db_cpu->{component_id}
				));
		}
		#
		# If either the CPU does not exist in the database, or the
		# model has changed, then fetch a replacement component
		#

		if (
			!$db_cpu ||
			!($db_cpu->{model} ~~ $cpu->{model})
		) {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf(q{Inventoried CPU does not match database:
					DB Model:      %s
					Probed model:  %s
				},
					$db_cpu ? $db_cpu->{model} || '' : '',
					$cpu->{model} || '',
				));

			#
			# If we're replacing the CPU, shelve the old one first
			#
			if ($db_cpu) {
				if (!defined($ret = runquery(
					description => sprintf(
						'shelving CPU component %d',
						$db_cpu->{component_id}
						),
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					query => q {
						SELECT * FROM
						component_utils.remove_component_hier(
							component_id := ?
						)
					},
					args => [
						$db_cpu->{component_id}
					]
				))) {
					return undef;
				}
			}
			#
			# Fetch a component for the CPU
			#

			if (!defined($db_cpu = runquery(
					description => 'inserting CPU component',
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						SELECT * FROM
							component_utils.insert_cpu_component(
								model := ?,
								processor_speed := ?,
								processor_cores := ?,
								socket_type := ?,
								vendor_name := ?
							)
					},
					args => [
						$cpu->{model},
						0,
						$cpu->{cores},
						$cpu->{socket_type},
						$cpu->{vendor},
					]
			))) {
				return undef;
			}

			if ($db_cpu->{parent_slot_id}) {
				if (!$force) {
					$r->log_rerror(
						Apache2::Log::LOG_MARK,
						Apache2::Const::LOG_ERR,
						APR::Const::SUCCESS,
						sprintf("CPU component_id %s in slot %s is already attached to slot_id %d",
							$db_cpu->{component_id},
							$slot_name,
							$db_cpu->{parent_slot_id}
						));
					return undef;
				}
			}

			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_INFO,
				APR::Const::SUCCESS,
				sprintf("Retrieved component_id %s (%s) for %s CPU in slot %s",
					$db_cpu->{component_id},
					$cpu->{model},
					$slot_name
				));
			my $slot;
			if (!defined($slot = runquery(
					description => 'attaching CPU component to parent slot',
					request => $r,
					usererror => $opt->{usererror},
					debug => $debug,
					dbh => $dbh,
					return_type => 'hashref',
					report_error => 0,
					query => q {
						SELECT * FROM
							component_utils.insert_component_into_parent_slot(
								parent_component_id := ?,
								component_id := ?,
								slot_name := ?,
								slot_type := ?,
								slot_function := 'CPU'
							)
					},
					args => [
						$device->{component_id},
						$db_cpu->{component_id},
						$slot_name,
						$cpu->{socket_type}
					]
			))) {
				return undef;
			}
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf("Attached component_id %d to slot_id %d of component %d",
					$db_cpu->{component_id},
					$slot->{slot_id},
					$device->{component_id}
				));
		} else {
			$r->log_rerror(
				Apache2::Log::LOG_MARK,
				Apache2::Const::LOG_DEBUG,
				APR::Const::SUCCESS,
				sprintf('Probed CPU in slot %s matches database',
					$slot_name,
				));
		}
	}
	return 1;
}


sub ChassisInventory {
	my $opt = &_options(@_);
	my $r = $opt->{request};
	my $debug = $opt->{debug};
	my $dbh = $opt->{dbh};
	my $force = $opt->{force};
	my $inventory = $opt->{inventory};
	my $hostinfo = $opt->{hostinfo};
	my $device = $opt->{device};

	#
	# This shouldn't happen, but...
	#
	return if (!$hostinfo);

	#
	# Defer constraints until later, because we play fast and loose with
	# moving some things around
	#

	$dbh->do('SET CONSTRAINTS ALL DEFERRED');

	my $dt;
	if (!defined($dt = runquery(
		description => 'looking for server device_type',
		request => $r,
		dbh => $dbh,
		debug => $debug,
		return_type => 'hashref',
		query => q {
			SELECT
				device_type_id,
				COALESCE(dt.model, dt.description) as model
			FROM
				jazzhands.device_type dt JOIN
				jazzhands.property p USING (company_id)
			WHERE
				p.property_name = 'DeviceVendorProbeString' AND
				p.property_type = 'DeviceProvisioning' AND
				p.property_value = ? AND
				dt.model = ?
		},
		args => [ 
			$hostinfo->{vendor},
			$hostinfo->{model}
		]
	))) {
		return Apache2::Const::OK;
	}

	if (!%$dt) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			sprintf(
				"Unable to locate an appropriate device_type for vendor string '%s', model '%s'",
				$hostinfo->{vendor},
				$hostinfo->{model}
			)
		);
		return Apache2::Const::OK;
	}

	$r->log_rerror(
		Apache2::Log::LOG_MARK,
		Apache2::Const::LOG_DEBUG,
		APR::Const::SUCCESS,
		sprintf(
			"Found matching device_type_id %d (%s) for vendor string '%s', model '%s'",
			$dt->{device_type_id},
			$dt->{model},
			$hostinfo->{vendor},
			$hostinfo->{model}
		)
	);
	if (!($device->{device_type_id} ~~ $dt->{device_type_id})) {
		#
		# Attempt to change the device type to match what the device reports.
		# All of the heavy lifting to do this is done in the database, so
		# the only issue that may come up here is if the device is plugged
		# into a chassis that requires the current underlying component_type.
		#
		# We try to deal with that situation below, though.
		#

		my $ret;
		if (!defined($ret = runquery(
			description => 'updating device_type_id',
			request => $r,
			dbh => $dbh,
			debug => $debug,
			query => q {
				UPDATE
					device
				SET
					device_type_id = ?
				WHERE
					device_id = ?
			},
			args => [ 
				$dt->{device_type_id},
				$device->{device_id}
			]
		))) {
			return Apache2::Const::OK;
		}

		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			sprintf(
				"Updated device_type_id for %s/%s (%d) from %d to %d",
				$device->{device_name},
				$device->{physical_label},
				$device->{device_id},
				$device->{device_type_id},
				$dt->{device_type_id}
			)
		);
	}
	return 1;
}

sub CleanStorageComponentInfo {
	my $opt = &_options(@_);
	my $r = $opt->{request};
	my $debug = $opt->{debug};
	my $dbh = $opt->{dbh};
	my $component = $opt->{component};

	if ($debug) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_DEBUG,
			APR::Const::SUCCESS,
			sprintf(
				'Shelving storage component %d in the database',
				$component->{component_id}
			)
		);
	}

	my $ret;

	if (!defined($ret = runquery(
		description => sprintf('deleting logical volume foo for component %d',
			$component->{component_id}),
		request => $r,
		usererror => $opt->{usererror},
		debug => $debug,
		dbh => $dbh,
		return_type => 'hashref',
		query => q {
			SELECT * FROM
			lv_manip.delete_pv(
				physicalish_volume_list := (
					SELECT ARRAY(SELECT
						physicalish_volume_id
					FROM
						physicalish_volume pv JOIN
						v_component_hier ch ON
							(pv.component_id =
								ch.child_component_id)
					WHERE
						ch.component_id = ?
				)),
				purge_orphans := true
			)
		},
		args => [
			$component->{component_id}
		]
	))) {
		return undef;
	}

	if (!defined($ret = runquery(
		description => sprintf(
			'shelving component %d and all attached components',
			$component->{component_id}
			),
		request => $r,
		usererror => $opt->{usererror},
		debug => $debug,
		dbh => $dbh,
		return_type => 'hashref',
		query => q {
			SELECT * FROM
			component_utils.remove_component_hier(
				component_id := ?
			)
		},
		args => [
			$component->{component_id}
		]
	))) {
		return undef;
	}

	return 1;
}


sub SetLLDPConnection {
	my $opt = &_options(@_);
	my $r = $opt->{request};
	my $debug = $opt->{debug};
	my $dbh = $opt->{dbh};

	#
	# Validate that parameters exist, but otherwise, we're just throwing
	# this over the wall and letting the database deal with things
	#
	if (!$opt->{device_id} || !$opt->{slot_name} || !$opt->{remote_slot_name}) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_ERR,
			APR::Const::SUCCESS,
			'device_id, slot_name, and remote_slot name must all be passed to SetLLDPConnection'
		);
		return undef;
	}
	
	if (!($opt->{remote_device_id} || $opt->{remote_device_name} ||
		$opt->{remote_host_id}
	)) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_ERR,
			APR::Const::SUCCESS,
			'one of remote_device_id, remote_device_name, or remote_host_id must all be passed to SetLLDPConnection'
		);
		return undef;
	}

	if ($debug) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_DEBUG,
			APR::Const::SUCCESS,
			sprintf(
				'Ensuring interface %s of device %s is connected to %s of device with %s of %s',
					$opt->{slot_name},
					$opt->{device_id},
					$opt->{remote_slot_name},
					$opt->{remote_device_id} ? 
						('device_id', $opt->{remote_device_id}) :
							($opt->{remote_host_id} ?
								('host_id', $opt->{remote_host_id}) :
								('device_name', $opt->{remote_device_name})
							)
						
			)
		);
	}

	my $ret;
	$dbh->do('SAVEPOINT SetLLDPConnection');

	if (!defined($ret = runquery(
		description =>
			sprintf(
				'connecting interface %s of device %s to %s of device with %s of %s',
					$opt->{slot_name},
					$opt->{device_id},
					$opt->{remote_device_id} ? 
						('device_id', $opt->{remote_device_id}) :
							($opt->{remote_host_id} ?
								('host_id', $opt->{remote_host_id}) :
								('device_name', $opt->{remote_device_name})
							)
				),
		request => $r,
		usererror => $opt->{usererror},
		debug => $debug,
		dbh => $dbh,
		return_type => 'hashref',
		query => q {
			SELECT * FROM
				component_connection_utils.create_inter_component_connection(
					device_id := ?,
					slot_name := ?,
					remote_slot_name := ?,
					remote_device_id := ?,
					remote_host_id := ?,
					remote_device_name := ?
				)
		},
		args => [
			$opt->{device_id},
			$opt->{slot_name},
			$opt->{remote_slot_name},
			$opt->{remote_device_id},
			$opt->{remote_host_id},
			$opt->{remote_device_name},
		]
	))) {
		$dbh->do('ROLLBACK TO SAVEPOINT SetLLDPConnection');
		return undef;
	}

	if ($ret->{changed}) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_INFO,
			APR::Const::SUCCESS,
			sprintf(
				'Interface %s of device %s connected to %s of device %s using inter_component_connection %d',
				$ret->{slot_name},
				$ret->{device_id},
				$ret->{remote_slot_name},
				$ret->{remote_device_id},
				$ret->{inter_component_connection_id}
			)
		);
	} elsif ($debug) {
		$r->log_rerror(
			Apache2::Log::LOG_MARK,
			Apache2::Const::LOG_DEBUG,
			APR::Const::SUCCESS,
			sprintf(
				'Interface %s of device %s already connected to %s of device %s using inter_component_connection %d',
				$ret->{slot_name},
				$ret->{device_id},
				$ret->{remote_slot_name},
				$ret->{remote_device_id},
				$ret->{inter_component_connection_id}
			)
		);
	}
	return(defined($ret->{inter_component_connection_id}));
}

1;
