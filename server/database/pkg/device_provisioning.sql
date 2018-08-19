\set ON_ERROR_STOP

DO $$
BEGIN
	CREATE SCHEMA device_provisioning;
EXCEPTION
	WHEN OTHERS THEN NULL;
END; $$;

-- this lingered in the jazzhands schema
DROP FUNCTION IF EXISTS insert_rack(rack_name text, site_code character varying, rack_type text, switch_type text, force boolean);

CREATE OR REPLACE FUNCTION device_provisioning.insert_switch(
		INOUT device_name	jazzhands.device.device_name%TYPE,
		site_code			jazzhands.site.site_code%TYPE,
		company_name		jazzhands.company.company_name%TYPE,
		model				jazzhands.device_type.model%TYPE,
		rack_id				jazzhands.rack.rack_id%TYPE DEFAULT NULL,
		rack_name			text DEFAULT NULL,
		rack_side			text DEFAULT 'FRONT',
		rack_u				integer DEFAULT NULL,
		device_function		text DEFAULT NULL,
		encapsulation_domain
							jazzhands.encapsulation_domain.encapsulation_domain%TYPE DEFAULT NULL,
		INOUT management_addr
							inet DEFAULT NULL,
		OUT device_id		jazzhands.device.device_id%TYPE
	) RETURNS RECORD AS $$
DECLARE
	rack_name_ary			text[];
	rack_loc_id				jazzhands.rack_location.rack_location_id%TYPE;
	mgmt_net_iface_name		text;
	mgmt_phys_port_name		text;
	mgmt_phys_port_id		jazzhands.physical_port.physical_port_id%TYPE;
	dns_rec					record;
	dns_netblock_id			jazzhands.netblock.netblock_id%TYPE;
	netblock_rec			record;
	rack_rec				record;
	dev_rec					record;
	device_netblock_id		jazzhands.netblock.netblock_id%TYPE;
	network_interface_id	jazzhands.network_interface.network_interface_id%TYPE;
	hostname_text			text[];
	zone_text				text[];
	domain_id				jazzhands.dns_domain.dns_domain_id%TYPE;
	netblock_list			integer[];
	ass_id					jazzhands.asset.asset_id%TYPE;
	dev_name				ALIAS FOR device_name;
	dev_id					ALIAS FOR device_id;
	ni_id					ALIAS FOR network_interface_id;
	encaps_domain			ALIAS FOR encapsulation_domain;
BEGIN
	IF encapsulation_domain IS NULL THEN
		encapsulation_domain := site_code;
	END IF;

	IF rack_id IS NOT NULL THEN
		SELECT * INTO rack_rec FROM jazzhands.rack r
			WHERE r.rack_id = insert_switch.rack_id;
		IF NOT FOUND THEN
			RAISE EXCEPTION 'Rack with rack_id % not found', rack_id;
		END IF;
		site_code := rack_rec.site_code;
	ELSE
		IF rack_name IS NULL OR site_code IS NULL THEN
			RAISE EXCEPTION 'rack_id or both rack_name and site_code must be specified';
		END IF;

		rack_name_ary := regexp_split_to_array(rack_name, '-');

		IF array_length(rack_name_ary, 1) = 1 THEN
			rack_name_ary := array_prepend(NULL, rack_name_ary);
		END IF;

		IF array_length(rack_name_ary, 1) = 2 THEN
			rack_name_ary := array_prepend(NULL, rack_name_ary);
		END IF;

		IF array_length(rack_name_ary, 1) = 3 THEN
			rack_name_ary := ARRAY[
				rack_name_ary[1],
				NULL,
				rack_name_ary[2],
				rack_name_ary[3]];
		END IF;

		SELECT
			r.rack_id INTO insert_switch.rack_id
		FROM
			jazzhands.rack r
		WHERE
			room IS NOT DISTINCT FROM rack_name_ary[1] AND
			sub_room IS NOT DISTINCT FROM rack_name_ary[2] AND
			rack_row IS NOT DISTINCT FROM rack_name_ary[3] AND
			r.rack_name IS NOT DISTINCT FROM rack_name_ary[4] AND
			r.site_code = insert_switch.site_code;

		IF NOT FOUND THEN
			RAISE EXCEPTION 'Rack with name % not found in site %',
				rack_name, site_code;
		END IF;
	END IF;

	RAISE DEBUG 'rack_id is %', rack_id;

	mgmt_phys_port_name :=
		CASE
			WHEN model ~ 'EX2200' THEN NULL
			WHEN model ~ 'EX4200' THEN 'vme-re0'
			ELSE 'Management1'
		END;

	mgmt_net_iface_name :=
		CASE
			WHEN model ~ 'EX2200' THEN 'vlan.10'
			WHEN model ~ 'EX4200' THEN 'vme.0'
			ELSE 'Management1'
		END;

	---
	--- Determine if this switch has already been inserted
	---
	SELECT
		d.device_id,
		ni.network_interface_id,
		n.netblock_id,
		n.ip_address
	INTO dev_rec
	FROM
		device d LEFT JOIN
		network_interface ni USING (device_id) LEFT JOIN
		network_interface_netblock nin USING (network_interface_id) LEFT JOIN
		netblock n USING (netblock_id)
	WHERE
		d.device_name = dev_name AND
		ni.network_interface_name = mgmt_net_iface_name;

	IF FOUND THEN
		dev_id := dev_rec.device_id;
		IF dev_rec.ip_address IS NOT NULL THEN
			management_addr := dev_rec.ip_address;
			RETURN;
		END IF;
		IF dev_rec.network_interface_id IS NOT NULL THEN
			ni_id := dev_rec.network_interface_id;
		END IF;
	END IF;

	IF management_addr IS NULL THEN
		SELECT
			array_agg(netblock_id) INTO netblock_list
		FROM
			netblock_collection nc JOIN
			netblock_collection_netblock ncn USING
				(netblock_collection_id) JOIN
			v_site_netblock_expanded sne USING (netblock_id)
		WHERE
			netblock_collection_type = 'NetblockAllocationPool' AND
			netblock_collection_name = 'ManagementNetworks' AND
			sne.site_code = insert_switch.site_code;

		IF NOT FOUND OR array_length(netblock_list, 1) IS NULL THEN
			RAISE EXCEPTION
				'No netblocks found for automatic allocation for site %.  Netblocks must be in the ''ManagementNetworks'' netblock_collection with a netblock_collection_type of ''NetblockAllocationPool''',
				site_code;
		END IF;

		SELECT
			* INTO netblock_rec
		FROM
			netblock_manip.allocate_netblock(
				parent_netblock_list := netblock_list,
				address_type := 'single',
				netblock_status := 'Allocated'
			);
		IF NOT FOUND THEN
			RAISE EXCEPTION
				'No addresses available to automatically assign to % from netblock_collection ''NetblockAllocationPool:ManagementNetworks''',
				device_name;
		END IF;
		management_addr := netblock_rec.ip_address;
		device_netblock_id := netblock_rec.netblock_id;
	ELSE
		SELECT
			dns_name, soa_name, netblock_id, z.dns_domain_id INTO dns_rec
		FROM
			jazzhands.dns_record r JOIN
			jazzhands.dns_domain z USING (dns_domain_id) JOIN
			jazzhands.netblock n USING (netblock_id)
		WHERE
			dns_type = 'A' AND
			host(ip_address) = host(management_addr);

		IF FOUND THEN
			device_netblock_id := dns_rec.netblock_id;
			dns_netblock_id := dns_rec.netblock_id;
		ELSE
			SELECT
				* INTO netblock_rec
			FROM
				jazzhands.netblock n
			WHERE
				is_single_address = 'Y' AND
				ip_universe_id = 0 AND
				netblock_type = 'default' AND
				host(ip_address) = host(management_addr);

			IF FOUND AND netblock_rec.description IS DISTINCT FROM device_name
			THEN
				RAISE EXCEPTION 'Netblock % already exists', management_addr;
			END IF;

			IF netblock_rec.netblock_id IS NOT NULL THEN
				IF netblock_rec.netblock_status = 'Reserved' THEN
					UPDATE netblock n SET netblock_status = 'Allocated' WHERE
					n.netblock_id = netblock_rec.netblock_id;
				END IF;
				device_netblock_id := netblock_rec.netblock_id;
			ELSE
				INSERT INTO netblock (
					ip_address,
					netblock_type,
					is_single_address,
					can_subnet,
					netblock_status
				) VALUES (
					management_addr,
					'default',
					'Y',
					'N',
					'Allocated'
				) RETURNING netblock_id INTO device_netblock_id;
			END IF;
		END IF;

		IF (dns_netblock_ic IS NOT NULL AND
				concat_ws('.', dns_rec.dns_name, dns_rec.soa_name)
				IS DISTINCT FROM device_name) THEN
			RAISE EXCEPTION
				'A DNS record for this netblock already exists for %',
				concat_ws('.', dns_rec.dns_name, dns_rec.soa_name);
		END IF;
	END IF;

	zone_text := regexp_split_to_array(device_name, '\.');
	WHILE (array_upper(zone_text, 1) > 1) LOOP
		hostname_text := hostname_text || zone_text[1];
		zone_text := zone_text[2:array_upper(zone_text, 1)];
		SELECT dns_domain_id INTO domain_id FROM jazzhands.dns_domain d WHERE
			soa_name = array_to_string(zone_text, '.');
		IF FOUND THEN
			EXIT;
		END IF;
	END LOOP;

	IF domain_id IS NULL THEN
		RAISE 'DNS domain for % not found', device_name;
	END IF;
	SELECT
		rack_location_id INTO rack_loc_id
	FROM
		jazzhands.rack_location rl
	WHERE
		rl.rack_id = insert_switch.rack_id AND
		rl.rack_side = insert_switch.rack_side AND
		rack_u_offset_of_device_top IS NOT DISTINCT FROM rack_u;

	IF NOT FOUND THEN
		INSERT INTO jazzhands.rack_location (
			rack_id, rack_u_offset_of_device_top, rack_side
		) VALUES (
			insert_switch.rack_id,
			rack_u,
			insert_switch.rack_side
		) RETURNING rack_location_id INTO rack_loc_id;
	END IF;

	RAISE DEBUG 'rack_location_id is %', rack_loc_id;

	IF device_id IS NULL THEN
		-- Determine whether the various asset columns have been removed
		-- from the device table

		INSERT INTO device (
			device_type_id,
			device_name,
			physical_label,
			site_code,
			rack_location_id,
			device_status,
			service_environment_id,
			operating_system_id,
			is_monitored,
			should_fetch_config,
			date_in_service
		) VALUES (
			(SELECT device_type_id FROM jazzhands.device_type dt JOIN company c
				USING (company_id)
				WHERE
					c.company_name = insert_switch.company_name AND
					dt.model = insert_switch.model
			),
			device_name,
			device_name,
			site_code,
			rack_loc_id,
			'up',
			(select service_environment_id from service_environment where service_environment_name = 'production'),
			0,
			'N',
			'N',
			current_timestamp
		) RETURNING * INTO dev_rec;
		insert_switch.device_id := dev_rec.device_id;

		-- This is a hack
		IF device_function IS NULL THEN
			device_function := CASE
				WHEN device_name ~ 'rs\d' THEN 'rack_switch'
				WHEN device_name ~ 'oob\d' THEN 'oob_switch'
				WHEN device_name ~ 'ss\d' THEN 'spine_switch'
				WHEN device_name ~ 'ls\d' THEN 'leaf_switch'
				WHEN device_name ~ 'cs\d' THEN 'core_switch'
				WHEN device_name ~ 'ds\d' THEN 'distribution_switch'
				ELSE NULL END;
		END IF;

		IF device_function IS NOT NULL THEN
			INSERT INTO device_collection_device (
				device_id,
				device_collection_id
			) VALUES
				(device_id, (
					SELECT device_collection_id FROM device_collection
					WHERE device_collection_type = 'device-function' AND
						device_collection_name = insert_switch.device_function
				));
		END IF;

		RAISE DEBUG 'New device_id is %', device_id;

		PERFORM port_utils.setup_device_physical_ports(in_device_id := device_id);
	END IF;

	SELECT
		physical_port_id INTO mgmt_phys_port_id
	FROM
		jazzhands.physical_port pp
	WHERE
		pp.device_id = insert_switch.device_id AND
		port_name = mgmt_phys_port_name;

	IF network_interface_id IS NULL THEN
		INSERT INTO network_interface (
			device_id,
			network_interface_name,
			network_interface_type,
			physical_port_id,
			should_monitor
		) VALUES (
			device_id,
			mgmt_net_iface_name,
			'broadcast',
			mgmt_phys_port_id,
			'N'
		) RETURNING network_interface.network_interface_id INTO
			network_interface_id;

		INSERT INTO network_interface_netblock (
			device_id,
			network_interface_id,
			netblock_id
		) VALUES (
			device_id,
			network_interface_id,
			device_netblock_id
		);

		INSERT INTO network_interface_purpose (
			device_id,
			network_interface_id,
			network_interface_purpose
		) VALUES
			(device_id, network_interface_id, 'api'),
			(device_id, network_interface_id, 'cloudapi'),
			(device_id, network_interface_id, 'ssh');
	END IF;

	IF dns_netblock_id IS NULL THEN
		INSERT INTO dns_record (
			dns_name,
			dns_domain_id,
			dns_type,
			netblock_id
		) VALUES (
			array_to_string(hostname_text, '.'),
			domain_id,
			'A',
			device_netblock_id
		);
	END IF;

	PERFORM * FROM encapsulation_domain ed WHERE
		ed.encapsulation_domain = encaps_domain AND
		ed.encapsulation_type = '802.1q';

	IF NOT FOUND THEN
		INSERT INTO encapsulation_domain (
			encapsulation_domain,
			encapsulation_type
		) VALUES (
			encaps_domain,
			'802.1q'
		);
	END IF;

	INSERT INTO device_encapsulation_domain(
		device_id,
		encapsulation_domain,
		encapsulation_type
	) VALUES (
		device_id,
		encapsulation_domain,
		'802.1q'
	);

	RETURN;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION device_provisioning.insert_server(
		device_name			jazzhands.device.device_name%TYPE,
		device_type_id		jazzhands.device_type.device_type_id%TYPE,
		serial_number		text,
		host_id				text,
		rack_id				jazzhands.rack.rack_id%TYPE,
		rack_u				integer DEFAULT NULL,
		bmc_mac				macaddr DEFAULT NULL,
		server_configuration_id
							cloudapi.server_configuration.id%TYPE DEFAULT NULL,
		OUT device_id		jazzhands.device.device_id%TYPE,
		OUT bmc_device_id	jazzhands.device.device_id%TYPE,
		OUT bmc_ip_address	inet
	) RETURNS RECORD AS $$
DECLARE
	scid					ALIAS FOR server_configuration_id;
	ass_id					jazzhands.asset.asset_id%TYPE;
	server_site_code		text;
	svc_env_id				jazzhands.service_environment.service_environment_id%TYPE;
	bmc_device_name			text;
	bmc_netblock_id			jazzhands.netblock.netblock_id%TYPE;
	dev_rec					RECORD;
	rack_rec				RECORD;
	netblock_rec			RECORD;
	rack_loc_id				jazzhands.rack_location.rack_location_id%TYPE;
	bmc_rack_loc_id			jazzhands.rack_location.rack_location_id%TYPE;
	network_interface_id	jazzhands.network_interface.network_interface_id%TYPE;
	hostname_text			text[];
	zone_text				text[];
	domain_id				jazzhands.dns_domain.dns_domain_id%TYPE;
	netblock_list			integer[];
	dc_id					jazzhands.device_collection.device_collection_id%TYPE;
	dev_id					ALIAS FOR device_id;
BEGIN
--	IF rack_id IS NULL THEN
--		RAISE EXCEPTION 'rack_id must be specified';
--	END IF;

	SELECT * INTO rack_rec FROM jazzhands.rack r
		WHERE r.rack_id = insert_server.rack_id;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'Rack with rack_id % not found', rack_id;
	END IF;
	server_site_code := rack_rec.site_code;

	--
	-- For now, require a BMC MAC, because it's too much of a pain in the
	-- ass to provision it later if it's not probed correctly
	--
	IF bmc_mac IS NULL THEN
		RAISE EXCEPTION 'BMC MAC is NULL in insert_server for device %.  This is probably due to a bad probe on the client side',
			device_name;
	END IF;

	IF bmc_mac IS NOT NULL THEN
		bmc_device_name := 'bmc.' || device_name;
		--
		-- This needs to change to not pull from the cloud_jazz.subnet table
		--
		SELECT
			array_agg(netblock_id) INTO netblock_list
		FROM
			(
				SELECT
					netblock_id
				FROM
					netblock_collection nc JOIN
					netblock_collection_netblock ncn USING
						(netblock_collection_id) JOIN
					v_site_netblock_expanded sne USING (netblock_id) JOIN
					netblock n USING (netblock_id)
				WHERE
					netblock_collection_type = 'NetblockAllocationPool' AND
					netblock_collection_name = 'BMCNetworks' AND
					site_code = server_site_code
				ORDER BY
					ip_address
			) x;

		IF NOT FOUND OR array_length(netblock_list, 1) IS NULL THEN
			RAISE EXCEPTION
				'No netblocks found for BMC address for site %',
				server_site_code;
		END IF;

		SELECT
			* INTO netblock_rec
		FROM
			netblock_manip.allocate_netblock(
				parent_netblock_list := netblock_list,
				address_type := 'single',
				netblock_status := 'Allocated',
				description := bmc_device_name
			);
		IF NOT FOUND OR netblock_rec.netblock_id IS NULL THEN
			RAISE EXCEPTION
				'No addresses available to automatically assign to %',
				bmc_device_name;
		END IF;
		bmc_ip_address := netblock_rec.ip_address;
		bmc_netblock_id := netblock_rec.netblock_id;

		zone_text := regexp_split_to_array(bmc_device_name, '\.');
		WHILE (array_upper(zone_text, 1) > 1) LOOP
			hostname_text := hostname_text || zone_text[1];
			zone_text := zone_text[2:array_upper(zone_text, 1)];
			SELECT dns_domain_id INTO domain_id FROM jazzhands.dns_domain d
				WHERE soa_name = array_to_string(zone_text, '.');
			IF FOUND THEN
				EXIT;
			END IF;
		END LOOP;

		IF domain_id IS NULL THEN
			RAISE 'DNS domain for % not found', bmc_device_name;
		END IF;
	END IF;

	SELECT
		rack_location_id INTO rack_loc_id
	FROM
		jazzhands.rack_location rl
	WHERE
		rl.rack_id = insert_server.rack_id AND
		rl.rack_side = 'FRONT' AND
		rack_u_offset_of_device_top IS NOT DISTINCT FROM rack_u;

	IF NOT FOUND THEN
		INSERT INTO jazzhands.rack_location (
			rack_id, rack_u_offset_of_device_top, rack_side
		) VALUES (
			insert_server.rack_id,
			rack_u,
			'FRONT'
		) RETURNING rack_location_id INTO rack_loc_id;
	END IF;

	RAISE DEBUG 'rack_location_id is %', rack_loc_id;

	SELECT
		rack_location_id INTO bmc_rack_loc_id
	FROM
		jazzhands.rack_location rl
	WHERE
		rl.rack_id = insert_server.rack_id AND
		rl.rack_side = 'FRONT' AND
		rack_u_offset_of_device_top IS NULL;

	IF NOT FOUND THEN
		INSERT INTO jazzhands.rack_location (
			rack_id, rack_u_offset_of_device_top, rack_side
		) VALUES (
			insert_server.rack_id,
			NULL,
			'FRONT'
		) RETURNING rack_location_id INTO bmc_rack_loc_id;
	END IF;

	RAISE DEBUG 'rack_location_id is %', rack_loc_id;

	-- Insert a new asset for this device

	INSERT INTO jazzhands.asset (
		serial_number,
		ownership_status
	) VALUES (
		serial_number,
		'leased'
	) RETURNING asset_id INTO ass_id;

	-- Get the production service environemnt
	SELECT service_environment_id INTO svc_env_id FROM service_environment
		WHERE service_environment_name = 'production';

	IF NOT FOUND THEN
		RAISE EXCEPTION 'Service environment production not found';
	END IF;

	IF server_configuration_id IS NULL THEN
		SELECT
			sc.server_configuration_id INTO scid
		FROM
			cloudapi.server_configuration
		WHERE
			name = 'unclassified';
	END IF;

	-- Determine whether the various asset columns have been removed
	-- from the device table

	INSERT INTO jazzhands.device (
		device_type_id,
		device_name,
		physical_label,
		site_code,
		host_id,
		rack_location_id,
		device_status,
		service_environment_id,
		operating_system_id,
		is_monitored,
		should_fetch_config,
		date_in_service
	) VALUES (
		device_type_id,
		device_name,
		device_name,
		server_site_code,
		host_id,
		rack_loc_id,
		'up',
		svc_env_id,
		0,
		'N',
		'N',
		current_timestamp
	) RETURNING * INTO dev_rec;

	device_id := dev_rec.device_id;

	IF dev_rec.component_id IS NOT NULL THEN
		UPDATE
			asset
		SET
			component_id = dev_rec.component_id
		WHERE
			asset_id = ass_id;
	END IF;

	RAISE DEBUG 'New device_id is %', device_id;

	INSERT INTO cloud_jazz.server (
		device_id,
		server_configuration_id,
		active,
		comment,
		type,
		kickstart_profile
	) VALUES (
		device_id,
		server_configuration_id,
		0,
		'initial load',
		'baremetal',
		'stresslinux.cfg'
	);

	SELECT
		device_collection_id INTO dc_id
	FROM
		device_collection
	WHERE
		(device_collection_name, device_collection_type) =
			('default', 'ApplicationAllocation');

	IF FOUND THEN
		INSERT INTO device_collection_device (
			device_collection_id,
			device_id
		) VALUES (
			dc_id,
			device_id
		);
	END IF;

	SELECT
		device_collection_id INTO dc_id
	FROM
		device_collection
	WHERE
		(device_collection_name, device_collection_type) =
			('stresslinux.cfg', 'SystemInstallationProfile');

	IF FOUND THEN
		INSERT INTO device_collection_device (
			device_collection_id,
			device_id
		) VALUES (
			dc_id,
			device_id
		);
	END IF;

	IF bmc_mac IS NOT NULL THEN
		INSERT INTO jazzhands.device (
			device_type_id,
			device_name,
			physical_label,
			site_code,
			host_id,
			rack_location_id,
			device_status,
			service_environment_id,
			operating_system_id,
			is_monitored,
			should_fetch_config,
			date_in_service
		) VALUES (
			( 
				SELECT
					dt.device_type_id
				FROM
					device_type dt JOIN
					company c USING (company_id)
				WHERE
					company_name = 'BMC' AND
					model = 'BMC'
			),
			bmc_device_name,
			bmc_device_name,
			server_site_code,
			serial_number,
			rack_loc_id,
			'up',
			svc_env_id,
			0,
			'N',
			'N',
			current_timestamp
		) RETURNING device.device_id INTO insert_server.bmc_device_id;

		RAISE DEBUG 'New BMC device_id is %', device_id;

		INSERT INTO jazzhands.device_management_controller (
			manager_device_id,
			device_id,
			device_mgmt_control_type
		) VALUES (
			bmc_device_id,
			device_id,
			'bmc'
		);

		INSERT INTO jazzhands.network_interface (
			device_id,
			network_interface_type,
			mac_addr,
			should_monitor
		) VALUES (
			bmc_device_id,
			'broadcast',
			bmc_mac,
			'N'
		) RETURNING network_interface.network_interface_id INTO
			network_interface_id;

		INSERT INTO network_interface_purpose (
			device_id,
			network_interface_id,
			network_interface_purpose
		) VALUES
			(bmc_device_id, network_interface_id, 'cloudapi');

		INSERT INTO network_interface_netblock(
			device_id,
			network_interface_id,
			netblock_id
		) VALUES
			(bmc_device_id, network_interface_id, bmc_netblock_id);

		INSERT INTO device_collection_device (
			device_id,
			device_collection_id
		) VALUES
			(device_id, (
				SELECT device_collection_id FROM device_collection
				WHERE device_collection_type = 'device-function' AND
					device_collection_name = 'server')
			),
			(bmc_device_id, (
				SELECT device_collection_id FROM device_collection
				WHERE device_collection_type = 'device-function' AND
					device_collection_name = 'BMC')
			);

		INSERT INTO jazzhands.dns_record (
			dns_name,
			dns_domain_id,
			dns_type,
			netblock_id
		) VALUES (
			array_to_string(hostname_text, '.'),
			domain_id,
			'A',
			bmc_netblock_id
		);
	END IF;

	PERFORM cloud_jazz.refresh_cache_device_customer();
	PERFORM cloud_jazz.refresh_cache_device_customer_dedicated();
	PERFORM cloud_jazz.refresh_cache_device_kickstart_profile();

	IF rack_rec.rack_type = 'layer3' THEN
		UPDATE
			cloudapi.server s
		SET
			customer_id = 24
		WHERE
			s.device_id = dev_id;
	END IF;

	RETURN;
END
$$ LANGUAGE plpgsql SET search_path = jazzhands,cloud_jazz,cloudapi;

--
-- This is actually very similar to the insert_server above, except that
-- it doesn't do any cloudapi bits, mostly for things that we can't tie
-- to racks
--
CREATE OR REPLACE FUNCTION device_provisioning.insert_server_device(
		device_name			jazzhands.device.device_name%TYPE,
		device_type_id		jazzhands.device_type.device_type_id%TYPE,
		site_code			jazzhands.site.site_code%TYPE,
		serial_number		text,
		host_id				text,
		bmc_mac				macaddr DEFAULT NULL,
		OUT device_id		jazzhands.device.device_id%TYPE,
		OUT bmc_device_id	jazzhands.device.device_id%TYPE,
		OUT bmc_ip_address	inet
	) RETURNS RECORD AS $$
DECLARE
	ass_id					jazzhands.asset.asset_id%TYPE;
	scode					ALIAS FOR site_code;
	svc_env_id				jazzhands.service_environment.service_environment_id%TYPE;
	bmc_device_name			text;
	bmc_netblock_id			jazzhands.netblock.netblock_id%TYPE;
	netblock_rec			RECORD;
	network_interface_id	jazzhands.network_interface.network_interface_id%TYPE;
	hostname_text			text[];
	zone_text				text[];
	domain_id				jazzhands.dns_domain.dns_domain_id%TYPE;
	netblock_list			integer[];
	dev_rec					RECORD;
BEGIN
	IF scode IS NULL THEN
		RAISE EXCEPTION 'site_code must be specified';
	END IF;

	--
	-- For now, require a BMC MAC, because it's too much of a pain in the
	-- ass to provision it later if it's not probed correctly
	--
	IF bmc_mac IS NULL THEN
		RAISE EXCEPTION 'BMC MAC is NULL in insert_server for device %.  This is probably due to a bad probe on the client side',
			device_name;
	END IF;

	IF bmc_mac IS NOT NULL THEN
		bmc_device_name := 'bmc.' || device_name;
		--
		-- This needs to change to not pull from the cloud_jazz.subnet table
		--
		SELECT
			array_agg(netblock_id) INTO netblock_list
		FROM
			(
				SELECT
					netblock_id
				FROM
					netblock_collection nc JOIN
					netblock_collection_netblock ncn USING
						(netblock_collection_id) JOIN
					v_site_netblock_expanded sne USING (netblock_id) JOIN
					netblock n USING (netblock_id)
				WHERE
					netblock_collection_type = 'NetblockAllocationPool' AND
					netblock_collection_name = 'BMCNetworks' AND
					sne.site_code = scode
				ORDER BY
					ip_address
			) x;

		IF NOT FOUND OR array_length(netblock_list, 1) IS NULL THEN
			RAISE EXCEPTION
				'No netblocks found for BMC address for site %',
				scode;
		END IF;

		SELECT
			* INTO netblock_rec
		FROM
			netblock_manip.allocate_netblock(
				parent_netblock_list := netblock_list,
				address_type := 'single',
				netblock_status := 'Allocated',
				description := bmc_device_name
			);
		IF NOT FOUND OR netblock_rec.netblock_id IS NULL THEN
			RAISE EXCEPTION
				'No addresses available to automatically assign to %',
				bmc_device_name;
		END IF;
		bmc_ip_address := netblock_rec.ip_address;
		bmc_netblock_id := netblock_rec.netblock_id;

		zone_text := regexp_split_to_array(bmc_device_name, '\.');
		WHILE (array_upper(zone_text, 1) > 1) LOOP
			hostname_text := hostname_text || zone_text[1];
			zone_text := zone_text[2:array_upper(zone_text, 1)];
			SELECT dns_domain_id INTO domain_id FROM jazzhands.dns_domain d
				WHERE soa_name = array_to_string(zone_text, '.');
			IF FOUND THEN
				EXIT;
			END IF;
		END LOOP;

		IF domain_id IS NULL THEN
			RAISE 'DNS domain for % not found', bmc_device_name;
		END IF;
	END IF;

	-- Insert a new asset for this device

	INSERT INTO jazzhands.asset (
		serial_number,
		ownership_status
	) VALUES (
		serial_number,
		'leased'
	) RETURNING asset_id INTO ass_id;

	-- Get the production service environemnt
	SELECT service_environment_id INTO svc_env_id FROM service_environment
		WHERE service_environment_name = 'production';

	IF NOT FOUND THEN
		RAISE EXCEPTION 'Service environment production not found';
	END IF;

	INSERT INTO jazzhands.device (
		device_type_id,
		device_name,
		physical_label,
		site_code,
		host_id,
		device_status,
		service_environment_id,
		operating_system_id,
		is_monitored,
		should_fetch_config,
		date_in_service
	) VALUES (
		device_type_id,
		device_name,
		device_name,
		scode,
		host_id,
		'up',
		svc_env_id,
		0,
		'N',
		'N',
		current_timestamp
	) RETURNING * INTO dev_rec;

	device_id := dev_rec.device_id;

	RAISE DEBUG 'New device_id is %', device_id;

	IF dev_rec.component_id IS NOT NULL THEN
		UPDATE
			asset a
		SET
			component_id = dev_rec.component_id
		WHERE
			asset_id = ass_id;
	END IF;

	IF bmc_mac IS NOT NULL THEN
		INSERT INTO jazzhands.device (
			device_type_id,
			device_name,
			physical_label,
			site_code,
			host_id,
			device_status,
			service_environment_id,
			operating_system_id,
			is_monitored,
			should_fetch_config,
			date_in_service
		) VALUES (
			( 
				SELECT
					dt.device_type_id
				FROM
					device_type dt JOIN
					company c USING (company_id)
				WHERE
					company_name = 'BMC' AND
					model = 'BMC'
			),
			bmc_device_name,
			bmc_device_name,
			scode,
			serial_number,
			'up',
			svc_env_id,
			0,
			'N',
			'N',
			current_timestamp
		) RETURNING device.device_id INTO insert_server_device.bmc_device_id;

		RAISE DEBUG 'New BMC device_id is %', device_id;

		INSERT INTO jazzhands.device_management_controller (
			manager_device_id,
			device_id,
			device_mgmt_control_type
		) VALUES (
			bmc_device_id,
			device_id,
			'bmc'
		);

		INSERT INTO jazzhands.network_interface (
			device_id,
			network_interface_type,
			mac_addr,
			should_monitor
		) VALUES (
			bmc_device_id,
			'broadcast',
			bmc_mac,
			'N'
		) RETURNING network_interface.network_interface_id INTO
			network_interface_id;

		INSERT INTO network_interface_netblock (
			device_id,
			network_interface_id,
			netblock_id
		) VALUES
			(bmc_device_id, network_interface_id, bmc_netblock_id);

		INSERT INTO network_interface_purpose (
			device_id,
			network_interface_id,
			network_interface_purpose
		) VALUES
			(bmc_device_id, network_interface_id, 'cloudapi');

		INSERT INTO device_collection_device (
			device_id,
			device_collection_id
		) VALUES
			(device_id, (
				SELECT device_collection_id FROM device_collection
				WHERE device_collection_type = 'device-function' AND
					device_collection_name = 'server')
			),
			(bmc_device_id, (
				SELECT device_collection_id FROM device_collection
				WHERE device_collection_type = 'device-function' AND
					device_collection_name = 'BMC')
			);

		INSERT INTO jazzhands.dns_record (
			dns_name,
			dns_domain_id,
			dns_type,
			netblock_id
		) VALUES (
			array_to_string(hostname_text, '.'),
			domain_id,
			'A',
			bmc_netblock_id
		);
	END IF;

	RETURN;
END
$$ LANGUAGE plpgsql SET search_path = jazzhands,cloud_jazz,cloudapi;

CREATE OR REPLACE FUNCTION device_provisioning.insert_network_interface(
	device_id				jazzhands.device.device_id%TYPE,
	network_interface_name	text,
	network_interface_type	text,
	netblock_id				jazzhands.netblock.netblock_id%TYPE DEFAULT NULL,
	description				jazzhands.network_interface.description%TYPE
								DEFAULT NULL,
	physical_port_id		jazzhands.network_interface.physical_port_id%TYPE
								DEFAULT NULL,
	logical_port_id			jazzhands.network_interface.logical_port_id%TYPE
								DEFAULT NULL,
	mac_addr				jazzhands.network_interface.mac_addr%TYPE
								DEFAULT NULL,
	is_interface_up			jazzhands.network_interface.is_interface_up%TYPE
								DEFAULT 'Y',
	should_monitor			jazzhands.network_interface.should_monitor%TYPE
								DEFAULT 'N',
	create_physical_port	boolean DEFAULT false,
	remote_physical_port_id	jazzhands.physical_port.physical_port_id%TYPE
								DEFAULT NULL
) RETURNS jazzhands.network_interface AS $$
DECLARE
	zone_text				text[];
	iface_text				text[];
	dns_entry				text;
	domain_id				jazzhands.dns_domain.dns_domain_id%TYPE;
	dev						RECORD;
	ni						RECORD;
BEGIN

	-- Attempt to tie the network interface to the physical port

	IF physical_port_id IS NULL THEN
		SELECT
			p.physical_port_id INTO insert_network_interface.physical_port_id
		FROM
			jazzhands.physical_port p
		WHERE
			p.device_id = insert_network_interface.device_id AND
			regexp_replace(network_interface_name, '\.0', '') = p.port_name;

		--
		-- if the port isn't already created, then just put in a 1000BaseT
		-- slot
		--
		IF physical_port_id IS NULL AND create_physical_port THEN
			INSERT INTO slot (
				component_id,
				slot_name,
				slot_type_id
			) SELECT
				component_id,
				regexp_replace(network_interface_name, '\.0', ''),
				slot_type_id
			FROM
				device d JOIN
				component c USING (component_id),
				slot_type st
			WHERE
				d.device_id = insert_network_interface.device_id AND
				slot_type = '1000BaseTEthernet' AND
				slot_function = 'network'
			RETURNING slot_id INTO
				insert_network_interface.physical_port_id;
		END IF;
	END IF;

	SELECT * INTO dev FROM jazzhands.device d WHERE
		d.device_id = insert_network_interface.device_id;

	IF NOT FOUND THEN
		RAISE EXCEPTION 'Device % not found', device_id;
	END IF;

	dns_entry := regexp_replace(
		concat_ws('.', network_interface_name, dev.device_name),
		'/', '-', 'g');

	INSERT INTO network_interface(
		device_id,
		network_interface_name,
		network_interface_type,
		physical_port_id,
		logical_port_id,
		description,
		is_interface_up,
		mac_addr,
		should_monitor
	) VALUES (
		device_id,
		network_interface_name,
		network_interface_type,
		physical_port_id,
		logical_port_id,
		description,
		is_interface_up,
		mac_addr,
		should_monitor
	) RETURNING * INTO ni;

	IF netblock_id IS NOT NULL THEN
		INSERT INTO network_interface_netblock(
			device_id,
			network_interface_id,
			netblock_id
		) VALUES (
			device_id,
			ni.network_interface_id,
			netblock_id
		);

		zone_text := regexp_split_to_array(dns_entry, '\.');
		WHILE (array_upper(zone_text, 1) > 1) LOOP
			iface_text := iface_text || zone_text[1];
			zone_text := zone_text[2:array_upper(zone_text, 1)];
			SELECT dns_domain_id INTO domain_id FROM jazzhands.dns_domain d
				WHERE soa_name = array_to_string(zone_text, '.');
			IF FOUND THEN
				EXIT;
			END IF;
		END LOOP;

		IF domain_id IS NOT NULL THEN
			INSERT INTO dns_record (
				dns_name,
				dns_domain_id,
				dns_type,
				netblock_id
			) VALUES (
				array_to_string(iface_text, '.'),
				domain_id,
				'A',
				netblock_id
			);
		END IF;
	END IF;

	IF physical_port_id IS NOT NULL AND remote_physical_port_id IS NOT NULL THEN
		INSERT INTO layer1_connection (
			physical_port1_id,
			physical_port2_id
		) VALUES (
			physical_port_id,
			remote_physical_port_id
		);
	END IF;
	RETURN ni;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;


CREATE OR REPLACE FUNCTION device_provisioning.insert_server_device_type(
    model               TEXT,
	company_name        TEXT DEFAULT NULL,
	company_id          jazzhands.company.company_id%TYPE DEFAULT NULL
) RETURNS TABLE (
	component_type_id   	jazzhands.component_type.component_type_id%TYPE,
	device_type_id			jazzhands.device_type.device_type_id%TYPE,
	server_configuration_id	cloudapi.server_configuration.id%TYPE,
	device_prefix			text
) AS $$
DECLARE
	_model				ALIAS FOR model;
	_company_name		ALIAS FOR company_name;
	_company_id			ALIAS FOR company_id;
	_component_type_id	ALIAS FOR component_type_id;
	_device_type_id		ALIAS FOR device_type_id;
	_server_configuration_id
						ALIAS FOR server_configuration_id;
	_device_prefix		ALIAS FOR device_prefix;
	ctid				integer;
	stid				integer;
	model_name			text;
	comp_rec			RECORD;
	sc_rec				RECORD;
	dt_rec				RECORD;
BEGIN
	IF (company_name IS NULL AND company_id IS NULL) OR
			(company_name IS NOT NULL and company_id IS NOT NULL) THEN
		RAISE 'Exactly one of company_name or company_id must be passed to device_provisioning.insert_server_device_type' USING ERRCODE = 'invalid_parameter_value';
	END IF;

	IF model IS NULL THEN
		RAISE 'model must be passed to device_provisioning.insert_server_device_type' USING ERRCODE ='invalid_parameter_value';
	END IF;

	--
	-- Locate the device vendor, otherwise insert a new one
	--
	IF _company_id IS NULL THEN
		SELECT
			c.* INTO comp_rec
		FROM
			company c JOIN
			jazzhands.property p USING (company_id)
		WHERE
			p.property_name = 'DeviceVendorProbeString' AND
			p.property_type = 'DeviceProvisioning' AND
			p.property_value = _company_name
		ORDER BY
			c.company_id
		LIMIT 1;

		IF NOT FOUND THEN
			SELECT c.* INTO comp_rec FROM company c
			WHERE c.company_name = _company_name;

			IF NOT FOUND THEN
				SELECT company_manip.add_company(
					_company_name := _company_name,
					_company_types := ARRAY['hardware provider'],
					 _description := 'device vendor auto-insert'
				) INTO _company_id;
			ELSE
				_company_id := comp_rec.company_id;
			END IF;

			INSERT INTO property (
				property_name,
				property_type,
				property_value,
				company_id
			) VALUES (
				'DeviceVendorProbeString',
				'DeviceProvisioning',
				_company_name,
				_company_id
			);
		END IF;
		_company_id := comp_rec.company_id;
	ELSE
		SELECT
			c.* INTO comp_rec
		FROM
			company c
		WHERE
			c.company_id = _company_id;

		_company_id := comp_rec.company_id;
	END IF;

	--
	-- Locate the component_type, otherwise insert a new one
	--
	SELECT
		ct.component_type_id INTO _component_type_id
	FROM
		component_type ct JOIN
		jazzhands.component_property p USING (component_type_id)
	WHERE
		p.component_property_name = 'DeviceModelProbeString' AND
		p.component_property_type = 'device' AND
		p.property_value = _model AND
		ct.company_id = _company_id;

	IF NOT FOUND THEN
		SELECT ct.component_type_id INTO _component_type_id
		FROM component_type ct
		WHERE
			ct.company_id = _company_id AND
			ct.model = _model;

		IF NOT FOUND THEN
			INSERT INTO component_type (
				company_id,
				model,
				asset_permitted,
				is_rack_mountable
			) VALUES (
				_company_id,
				_model,
				'Y',
				'Y'
			) RETURNING component_type.component_type_id INTO
				_component_type_id;

			INSERT INTO component_type_component_func (
				component_type_id,
				component_function
			) VALUES (
				_component_type_id,
				'device'
			);
		END IF;

		INSERT INTO component_property (
			component_property_name,
			component_property_type,
			property_value,
			component_type_id
		) VALUES (
			'DeviceModelProbeString',
			'device',
			_model,
			_component_type_id
		);
	END IF;

	--
	-- Locate the device_type, otherwise insert a new one.  If more than
	-- one are returned, use the first one inserted.
	--
	SELECT
		dt.device_type_id INTO _device_type_id
	FROM
		device_type dt
	WHERE
		dt.component_type_id = _component_type_id OR
		(dt.model = _model AND dt.company_id = _company_id)
	ORDER BY
		dt.device_type_id
	LIMIT 1;

	IF NOT FOUND THEN
		INSERT INTO device_type (
			component_type_id,
			device_type_name,
			company_id,
			description
		) VALUES (
			_component_type_id,
			_model,
			_company_id,
			concat_ws(' ', comp_rec.company_name, _model)
		) RETURNING * INTO dt_rec;

		_device_type_id = dt_rec.device_type_id;

		INSERT INTO cloud_jazz.network_device_type(
			device_type_id,
			type
		) VALUES (
			_device_type_id,
			'server'
		);
	END IF;

	SELECT
		dtsc.server_configuration_id INTO _server_configuration_id
	FROM
		device_provisioning.device_type_to_server_config dtsc
	WHERE
		dtsc.device_type_id = _device_type_id
	ORDER BY
		dtsc.server_configuration_id
	LIMIT 1;

	IF NOT FOUND THEN
		INSERT INTO device_provisioning.device_type_to_server_config (
			device_type_id,
			server_configuration_id,
			device_prefix,
			max_memory,
			max_disks
		) SELECT
			_device_type_id,
			sc.id,
			regexp_replace(_model, '[ _/]', '-', 'g'),
			10485760,
			100
		FROM
			cloudapi.server_configuration sc
		WHERE
			sc.name = 'unclassified'
		RETURNING
			device_type_to_server_config.server_configuration_id,
			device_type_to_server_config.device_prefix
		INTO
			_server_configuration_id,
			_device_prefix;
	END IF;

	RETURN NEXT;
	RETURN;
END;
$$
SET search_path=jazzhands
LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION device_provisioning.set_device_rack_location(
	INOUT device_id			jazzhands.device.device_id%TYPE,
	rack_id					jazzhands.rack.rack_id%TYPE DEFAULT NULL,
	rack_u					integer DEFAULT NULL,
	rack_side				jazzhands.rack_location.rack_side%TYPE DEFAULT NULL,
	OUT rack_location_id	jazzhands.rack_location.rack_location_id%TYPE
) RETURNS SETOF RECORD AS $$
BEGIN
	RETURN QUERY SELECT * FROM device_provisioning.set_device_rack_location(
		device_id_list := ARRAY[device_id],
		rack_id := rack_id,
		rack_u := rack_u,
		rack_side := rack_side
	);
	RETURN;
END;
$$
SET search_path=jazzhands
LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION device_provisioning.set_device_rack_location(
	device_id_list		integer[],
	rack_id				jazzhands.rack.rack_id%TYPE DEFAULT NULL,
	rack_u				integer DEFAULT NULL,
	rack_side			jazzhands.rack_location.rack_side%TYPE DEFAULT NULL
) RETURNS TABLE (
	device_id			jazzhands.device.device_id%TYPE,
	rack_location_id	jazzhands.rack_location.rack_location_id%TYPE
) AS $$
DECLARE
	dev_id			ALIAS FOR device_id;
	r_id			jazzhands.rack.rack_id%TYPE;
	r_side			ALIAS FOR rack_side;
	rl_id			ALIAS FOR rack_location_id;
	ru				integer;
	saved_rl_id		jazzhands.rack_location.rack_location_id%TYPE;
	current_rl		RECORD;
	new_rl			RECORD;
	rack_rec		RECORD;
	port_map		jsonb;
BEGIN
	--
	-- Pull map of all devices we care about once, because this query is
	-- expensive
	--

	WITH z AS (
		SELECT
			x.device_id,
			x.remote_device_id,
			x.remote_slot_index
		FROM (
			SELECT
				*,
				rank() OVER (PARTITION BY dsc.device_id ORDER BY dsc.slot_name)
					AS connection_rank
			FROM
				v_device_slot_connections dsc
			WHERE
				dsc.slot_function = 'network' AND
				dsc.inter_component_connection_id IS NOT NULL AND
				dsc.remote_slot_index IS NOT NULL AND dsc.device_id =
					ANY (device_id_list)
		) x
		WHERE
			connection_rank = 1
	), y AS (
		SELECT
			d.device_id,
			d.device_type_id,
			sdt.config_fetch_type,
			remote_slot_index,
			dtrl.site_code,
			dtrl.rack_u,
			rank() OVER (PARTITION BY d.device_id ORDER BY dtrl.site_code
				NULLS LAST)
		FROM
			z JOIN
			device d USING (device_id) JOIN
			device s ON (z.remote_device_id = s.device_id) JOIN
			device_type sdt ON (s.device_type_id = sdt.device_type_id) JOIN
			device_provisioning.device_type_rack_location dtrl ON (
				d.device_type_id = dtrl.device_type_id AND
				sdt.config_fetch_type = dtrl.switch_type AND
				z.remote_slot_index = dtrl.port AND
				(dtrl.site_code IS NULL OR d.site_code = dtrl.site_code)
			)
	)
	SELECT
		jsonb_object_agg(y.device_id, y.rack_u) INTO port_map
	FROM y
	WHERE
		rank = 1;

	saved_rl_id := rl_id;
	FOREACH dev_id IN ARRAY device_id_list LOOP
		rl_id := saved_rl_id;
		SELECT
			d.device_type_id,
			rl.rack_location_id,
			rl.rack_id,
			rl.rack_u_offset_of_device_top,
			rl.rack_side
		INTO current_rl
		FROM
			jazzhands.device d LEFT JOIN
			jazzhands.rack_location rl USING (rack_location_id)
		WHERE
			d.device_id = dev_id;

		IF rack_id IS NULL THEN
			r_id := current_rl.rack_id;
		ELSE
			r_id := rack_id;
		END IF;

		IF r_id IS NULL THEN
			rack_location_id := NULL;
			RETURN NEXT;
			CONTINUE;
		END IF;

		SELECT * FROM jazzhands.rack r INTO rack_rec WHERE r.rack_id = r_id;

		IF NOT FOUND THEN
			rack_location_id := NULL;
			RETURN NEXT;
			CONTINUE;
		END IF;

		IF rack_u IS NULL THEN
			IF port_map ? dev_id::text THEN
				ru := port_map->(dev_id::text);
			END IF;
			IF ru IS NULL THEN
				rack_location_id := NULL;
				RETURN NEXT;
				CONTINUE;
			END IF;
		ELSE
			ru := rack_u;
		END IF;

		SELECT rl.rack_location_id INTO rack_location_id
		FROM
			rack_location rl
		WHERE
			rl.rack_id = r_id AND
			rl.rack_u_offset_of_device_top = ru AND
			rl.rack_side = COALESCE(r_side, 'FRONT');

		IF rack_location_id IS NULL THEN
			INSERT INTO rack_location (
				rack_id,
				rack_u_offset_of_device_top,
				rack_side
			) VALUES (
				r_id,
				ru,
				COALESCE(rack_side, 'FRONT')
			) RETURNING rack_location.rack_location_id INTO rl_id;
		END IF;

		IF current_rl.rack_location_id != rl_id THEN
			UPDATE
				device d
			SET
				rack_location_id = rl_id
			WHERE
				d.device_id = dev_id;
		END IF;
		RETURN NEXT;
		CONTINUE;
	END LOOP;
END;
$$
SET search_path=jazzhands
LANGUAGE plpgsql SECURITY DEFINER;

GRANT USAGE ON SCHEMA device_provisioning TO device_provisioning_role;
GRANT USAGE ON SCHEMA device_provisioning TO ro_role;
GRANT USAGE ON SCHEMA device_provisioning TO iud_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA device_provisioning
	TO device_provisioning_role;
