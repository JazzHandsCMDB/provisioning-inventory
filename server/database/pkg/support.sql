INSERT INTO val_property_type (
	property_type,
	description,
	is_multivalue
) VALUES (
	'DeviceProvisioning',
	'properties related to automatic device provisioning',
	'Y'
);

INSERT INTO val_property (
	property_name,
	property_type,
	description,
	is_multivalue,
	property_data_type,
	permit_company_id
) VALUES (
	'DeviceVendorProbeString',
	'DeviceProvisioning',
	'Vendor string that may be found during a probe',
	'Y',
	'string',
	'REQUIRED'
);

DO $$
DECLARE
	cid	integer;
BEGIN
	SELECT company_id INTO cid FROM company WHERE
		company_name in ('Dell', 'Dell/EMC') LIMIT 1;

	IF NOT FOUND THEN
		company_id := company_manip.add_company(
			_company_name := 'Dell/EMC',
			_company_types := ARRAY['vendor']
		);
	END IF;

	INSERT INTO property (
		property_name, property_type, company_id, property_value
	) VALUES
		('DeviceVendorProbeString', 'DeviceProvisioning', cid, 'Dell Inc.'),
		('DeviceVendorProbeString', 'DeviceProvisioning', cid, 'Dell'),
		('DeviceVendorProbeString', 'DeviceProvisioning', cid, 'Dell/EMC');

	SELECT company_id INTO cid FROM company WHERE
		company_name in ('IBM') LIMIT 1;

	IF NOT FOUND THEN
		company_id := company_manip.add_company(
			_company_name := 'IBM',
			_company_types := ARRAY['vendor']
		);
	END IF;

	INSERT INTO property (
		property_name, property_type, company_id, property_value
	) VALUES
		('DeviceVendorProbeString', 'DeviceProvisioning', cid, 'IBM');

	SELECT company_id INTO cid FROM company WHERE
		company_name in ('Lenovo') LIMIT 1;

	IF NOT FOUND THEN
		company_id := company_manip.add_company(
			_company_name := 'Lenovo',
			_company_types := ARRAY['vendor']
		);
	END IF;

	INSERT INTO property (
		property_name, property_type, company_id, property_value
	) VALUES
		('DeviceVendorProbeString', 'DeviceProvisioning', cid, 'Lenovo');

END; $$;

CREATE TABLE device_provisioning.device_type_rack_location (
	device_type_id			integer NOT NULL,
	switch_type				text NOT NULL,
	port					integer NOT NULL,
	site_code				text NULL,
	rack_u					integer NOT NULL
);

ALTER TABLE device_provisioning.device_type_rack_location 
	ADD CONSTRAINT uq_dtrl_dtid_st_po_sc UNIQUE 
		(device_type_id, switch_type, port, site_code);

ALTER TABLE device_provisioning.device_type_rack_location 
	ADD CONSTRAINT fk_dtrl_dev_type_id FOREIGN KEY (device_type_id) REFERENCES 
		jazzhands.device_type(device_type_id) DEFERRABLE;

ALTER TABLE device_provisioning.device_type_rack_location 
	ADD CONSTRAINT fk_dtrl_site_code FOREIGN KEY (site_code) REFERENCES 
		jazzhands.site(site_code) DEFERRABLE;


GRANT SELECT ON device_provisioning.device_type_rack_location TO ro_role;
GRANT SELECT,INSERT,UPDATE,DELETE ON device_provisioning.device_type_rack_location TO iud_role;

