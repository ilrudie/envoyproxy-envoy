load("//bazel:envoy_build_system.bzl", "envoy_cc_test", "envoy_contrib_package")

licenses(["notice"])  # Apache 2

envoy_contrib_package()

envoy_cc_test(
    name = "io_handle_test",
    srcs = ["io_handle_test.cc"],
    deps = [
        "//contrib/istio/transport_sockets/hbone/source:hbone_lib",
        "//source/common/network:connection_impl",
        "//source/common/network:default_client_connection_factory",
        "//source/common/network:filter_state_dst_address_lib",
        "//source/common/network:raw_buffer_socket_lib",
        "//source/common/network:utility_lib",
        "//source/extensions/clusters/original_dst:original_dst_cluster_lib",
        "//test/mocks/http:http_mocks",
        "//test/mocks/network:network_mocks",
        "//test/mocks/upstream:upstream_mocks",
        "//test/test_common:simulated_time_system_lib",
        "//test/test_common:utility_lib",
    ],
)

envoy_cc_test(
    name = "integration_test",
    srcs = ["integration_test.cc"],
    data = ["//test/config/integration/certs"],
    deps = [
        "//contrib/istio/transport_sockets/hbone/source:config",
        "//test/common/http/http2:http2_frame",
        "//test/integration:http_integration_lib",
        "@envoy_api//contrib/envoy/extensions/transport_sockets/hbone/v3:pkg_cc_proto",
        "@envoy_api//envoy/extensions/transport_sockets/raw_buffer/v3:pkg_cc_proto",
        "@envoy_api//envoy/extensions/transport_sockets/tls/v3:pkg_cc_proto",
    ],
)
