namespace java org.apache.druid.emitter.scribe

struct DruidIndexingLogEvent {
    1: required string task_id
    2: required string task_type
    3: required string datasource
    4: required string task_status
    5: required i64 duration
    6: required i64 creation_time
    7: required string owner

    8: optional string role
    9: optional string druid_version
    10: optional string environment
    11: optional string datacenter
    12: optional string cluster_name
}(persisted='true')
