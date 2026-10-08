load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_contrib_extension",
    "envoy_cc_library",
    "envoy_contrib_package",
)

licenses(["notice"])  # Apache 2

envoy_contrib_package()

# The paired factories share registration code. Distinct extension targets keep
# the contrib binary's dependency list unique when both names are selected.
envoy_cc_contrib_extension(
    name = "upstream_config",
    deps = [":config"],
)

envoy_cc_contrib_extension(
    name = "goaway_lb_config",
    deps = [":goaway_config"],
)

envoy_cc_contrib_extension(
    name = "goaway_config",
    srcs = ["goaway_config.cc"],
    hdrs = ["goaway_config.h"],
    deps = [
        ":goaway_lib",
        "//envoy/http:client_codec_factory_interface",
        "//envoy/registry",
        "//envoy/server:filter_config_interface",
        "//envoy/singleton:manager_interface",
        "//source/common/config:utility_lib",
        "//source/common/http/http2:codec_lib",
        "//source/common/protobuf:utility_lib",
        "//source/common/upstream:load_balancer_factory_base_lib",
        "@envoy_api//contrib/envoy/extensions/filters/network/istio_hbone/v3alpha:pkg_cc_proto",
    ],
)

envoy_cc_library(
    name = "goaway_lib",
    srcs = ["goaway.cc"],
    hdrs = ["goaway.h"],
    deps = [
        "//envoy/common:time_interface",
        "//envoy/http:codec_interface",
        "//envoy/singleton:instance_interface",
        "//envoy/stats:scope_interface",
        "//envoy/stats:stats_macros",
        "//envoy/upstream:load_balancer_interface",
        "//source/common/common:assert_lib",
        "//source/common/network:utility_lib",
        "//source/common/upstream:load_balancer_context_base_lib",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/synchronization",
    ],
)

envoy_cc_contrib_extension(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    deps = [
        ":hbone_lib",
        "//envoy/registry",
        "//envoy/server:transport_socket_config_interface",
        "//source/common/network:transport_socket_options_lib",
        "//source/extensions/filters/network/common:factory_base_lib",
        "//source/extensions/transport_sockets/internal_upstream:config",
        "@envoy_api//contrib/envoy/extensions/filters/network/istio_hbone/v3alpha:pkg_cc_proto",
    ],
)

envoy_cc_library(
    name = "hbone_lib",
    srcs = ["hbone.cc"],
    hdrs = ["hbone.h"],
    deps = [
        "//envoy/event:dispatcher_interface",
        "//envoy/stream_info:bool_accessor_interface",
        "//envoy/stream_info:filter_state_interface",
        "//source/common/network:raw_buffer_socket_lib",
        "//source/common/tcp_proxy",
        "//source/extensions/io_socket/user_space:io_handle_lib",
        "//source/extensions/transport_sockets/internal_upstream",
    ],
)
