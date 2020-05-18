namespace java org.apache.druid.emitter.scribe

struct DruidAdminLogEvent {
    1: required string audit_key
    2: required string audit_type
    3: required string author
    4: required string comment
    5: required string remote_address
    6: required string created_date
    7: required string payload

    8: optional string role
    9: optional string druid_version
    10: optional string environment
    11: optional string datacenter
    12: optional string cluster_name
}(persisted='true')
