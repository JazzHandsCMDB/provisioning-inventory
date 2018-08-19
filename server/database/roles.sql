CREATE ROLE device_provisioning_role NOLOGIN;

GRANT ro_role TO device_provisioning_role;
GRANT USAGE ON SCHEMA device_provisioning TO device_provisioning_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA device_provisioning TO device_provisioning_role;
GRANT USAGE ON SCHEMA netblock_manip TO device_provisioning_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA netblock_manip TO device_provisioning_role;
GRANT SELECT ON jazzhands.v_device_slot_connections TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.device TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.network_interface TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.physical_port TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.layer1_connection TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.network_interface_purpose TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.netblock TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.device_management_controller TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.device_collection_device TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.dns_record TO device_provisioning_role;
GRANT INSERT,UPDATE,DELETE ON jazzhands.rack_location TO device_provisioning_role;
GRANT INSERT,UPDATE ON jazzhands.rack TO device_provisioning_role;
GRANT INSERT,UPDATE ON jazzhands.netblock TO device_provisioning_role;

GRANT USAGE ON SCHEMA component_connection_utils TO device_provisioning_role;
GRANT EXECUTE ON FUNCTION component_connection_utils.create_inter_component_connection(integer, character varying, character varying, integer, character varying, character varying, boolean) to device_provisioning_role;
