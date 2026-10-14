load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_contrib_extension",
    "envoy_cc_library",
    "envoy_contrib_package",
)

licenses(["notice"])  # Apache 2

envoy_contrib_package()

envoy_cc_library(
    name = "hbone_lib",
    srcs = ["io_handle.cc"],
    hdrs = [
        "hbone.h",
        "io_handle.h",
    ],
    deps = [
        "//envoy/event:dispatcher_interface",
        "//envoy/network:io_handle_interface",
        "//envoy/upstream:cluster_manager_interface",
        "//source/common/buffer:watermark_buffer_lib",
        "//source/common/http:header_map_lib",
        "//source/common/http:response_decoder_impl_base",
        "//source/common/http:utility_lib",
        "//source/common/network:filter_state_dst_address_lib",
        "//source/common/network:io_socket_error_lib",
        "//source/common/network:utility_lib",
        "//source/common/stream_info:stream_info_lib",
        "//source/common/upstream:load_balancer_context_base_lib",
        "//source/extensions/clusters/original_dst:original_dst_cluster_lib",
        "//source/extensions/transport_sockets/common:passthrough_lib",
    ],
)

envoy_cc_contrib_extension(
    name = "config",
    srcs = ["config.cc"],
    deps = [
        ":hbone_lib",
        "//envoy/registry",
        "//envoy/server:transport_socket_config_interface",
        "//source/common/config:utility_lib",
        "@envoy_api//contrib/envoy/extensions/transport_sockets/hbone/v3:pkg_cc_proto",
    ],
)
