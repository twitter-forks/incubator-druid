namespace java org.apache.druid.emitter.scribe

struct DruidQueryLogEvent {
    1: required string native_query_id
    2: optional string sql_query_id

    3: optional string role
    4: optional string druid_version
    5: optional string environment
    6: optional string datacenter
    7: optional string cluster_name
    8: optional string service_name
    9: optional string host
    10: optional string remote_address

    11: required bool is_sql_query
    12: required string query
    13: optional string datasource
    14: required bool success
    15: required i64 creation_time
    16: required i64 execution_time
    17: required i64 output_result_size
    18: required string authenticator
    19: required string stats

    20: optional string imply_data_cube
    21: optional string imply_feature
    22: optional string imply_user
    23: optional string imply_user_email
    24: optional string imply_view
    25: optional string imply_view_title
    26: optional i32 imply_priority
}(persisted='true')
