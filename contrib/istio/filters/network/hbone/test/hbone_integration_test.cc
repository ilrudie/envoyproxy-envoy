#include "envoy/config/bootstrap/v3/bootstrap.pb.h"
#include "envoy/extensions/bootstrap/internal_listener/v3/internal_listener.pb.h"
#include "envoy/extensions/load_balancing_policies/round_robin/v3/round_robin.pb.h"
#include "envoy/extensions/retry/host/previous_hosts/v3/previous_hosts.pb.h"

#include "test/integration/http_integration.h"
#include "test/test_common/utility.h"

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "contrib/istio/filters/network/hbone/source/config.h"
#include "contrib/istio/filters/network/hbone/source/goaway_config.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace Envoy {
namespace Extensions {
namespace Istio {
namespace Hbone {
namespace {

using testing::Eq;
using testing::HasSubstr;

class HboneIntegrationTest : public testing::TestWithParam<bool>, public HttpIntegrationTest {
public:
  HboneIntegrationTest()
      : HttpIntegrationTest(Http::CodecType::HTTP1,
                            TestEnvironment::getIpVersionsForTest().front()) {
    setUpstreamProtocol(Http::CodecType::HTTP2);
  }

  void initialize() override {
    config_helper_.addRuntimeOverride("envoy.restart_features.upstream_http_filters_with_tcp_proxy",
                                      GetParam() ? "true" : "false");
    config_helper_.addConfigModifier([this](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
      envoy::extensions::bootstrap::internal_listener::v3::InternalListener internal_listener;
      auto* extension = bootstrap.add_bootstrap_extensions();
      extension->set_name("envoy.bootstrap.internal_listener");
      ASSERT_TRUE(extension->mutable_typed_config()->PackFrom(internal_listener));

      auto* resources = bootstrap.mutable_static_resources();
      if (separate_peers_) {
        auto second = resources->clusters(0);
        second.set_name("cluster_1");
        second.mutable_load_assignment()->set_cluster_name("cluster_1");
        *resources->add_clusters() = second;
      }
      if (goaway_preference_) {
        for (auto& cluster : *resources->mutable_clusters()) {
          auto& options = (*cluster.mutable_typed_extension_protocol_options())
              ["envoy.upstream_options.istio_hbone"];
          GoAwayOptionsProto config;
          config.mutable_cooldown()->set_seconds(30);
          ASSERT_TRUE(options.PackFrom(config));
          if (GetParam()) {
            // The control plane uses a typed struct until its Go dependency includes this API.
            TestUtility::loadFromYaml(R"EOF(
"@type": type.googleapis.com/udpa.type.v1.TypedStruct
type_url: type.googleapis.com/envoy.extensions.filters.network.istio_hbone.v3alpha.GoAwayOptions
value: {}
)EOF",
                                      options);
          }
        }
      }
      auto* service = resources->add_clusters();
      service->set_name("service");
      service->mutable_connect_timeout()->set_seconds(5);
      service->set_lb_policy(envoy::config::cluster::v3::Cluster::ROUND_ROBIN);
      if (goaway_preference_) {
        service->set_lb_policy(envoy::config::cluster::v3::Cluster::LOAD_BALANCING_POLICY_CONFIG);
        GoAwayLbProto config;
        auto* child =
            config.mutable_child_policy()->add_policies()->mutable_typed_extension_config();
        child->set_name("envoy.load_balancing_policies.round_robin");
        ASSERT_TRUE(child->mutable_typed_config()->PackFrom(
            envoy::extensions::load_balancing_policies::round_robin::v3::RoundRobin{}));
        auto* policy = service->mutable_load_balancing_policy()
                           ->add_policies()
                           ->mutable_typed_extension_config();
        policy->set_name("envoy.load_balancing_policies.istio_hbone");
        ASSERT_TRUE(policy->mutable_typed_config()->PackFrom(config));
        if (GetParam()) {
          TestUtility::loadFromYaml(R"EOF(
"@type": type.googleapis.com/udpa.type.v1.TypedStruct
type_url: type.googleapis.com/envoy.extensions.filters.network.istio_hbone.v3alpha.GoAwayLoadBalancingConfig
value:
  child_policy:
    policies:
    - typed_extension_config:
        name: envoy.load_balancing_policies.round_robin
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.load_balancing_policies.round_robin.v3.RoundRobin
)EOF",
                                    *policy->mutable_typed_config());
        }
      }
      auto* transport = service->mutable_transport_socket();
      transport->set_name("envoy.transport_sockets.istio_hbone");
      ASSERT_TRUE(transport->mutable_typed_config()->PackFrom(SocketConfig{}));
      auto* assignment = service->mutable_load_assignment();
      assignment->set_cluster_name("service");
      auto* endpoints = assignment->add_endpoints();

      // Distinct application hosts share the same outer HTTP/2 connection.
      for (int i = 0; i < 2; ++i) {
        const auto name = absl::StrCat("originate_", i);
        auto* lb_endpoint = endpoints->add_lb_endpoints();
        auto* endpoint = lb_endpoint->mutable_endpoint();
        if (separate_peers_) {
          (*(*lb_endpoint->mutable_metadata()
                  ->mutable_filter_metadata())["envoy.filters.listener.original_dst"]
                .mutable_fields())["local"]
              .set_string_value(fake_upstreams_[i]->localAddress()->asString());
        }
        endpoint->mutable_address()->mutable_envoy_internal_address()->set_server_listener_name(
            name);
        auto* listener = resources->add_listeners();
        listener->set_name(name);
        listener->mutable_internal_listener();
        auto* filter = listener->add_filter_chains()->add_filters();
        filter->set_name("envoy.filters.network.istio_hbone");
        FilterConfig config;
        auto* tcp = config.mutable_tcp_proxy();
        tcp->set_stat_prefix(name);
        tcp->set_cluster(separate_peers_ ? absl::StrCat("cluster_", i) : "cluster_0");
        tcp->mutable_tunneling_config()->set_hostname(absl::StrCat("app-", i, ":8080"));
        ASSERT_TRUE(filter->mutable_typed_config()->PackFrom(config));
      }
    });
    config_helper_.addConfigModifier(
        [this](
            envoy::extensions::filters::network::http_connection_manager::v3::HttpConnectionManager&
                hcm) {
          auto* route = hcm.mutable_route_config()
                            ->mutable_virtual_hosts(0)
                            ->mutable_routes(0)
                            ->mutable_route();
          route->set_cluster("service");
          if (retry_) {
            auto* retry = route->mutable_retry_policy();
            retry->set_retry_on("connect-failure");
            retry->mutable_num_retries()->set_value(1);
            retry->set_host_selection_retry_max_attempts(3);
            auto* predicate = retry->add_retry_host_predicate();
            predicate->set_name("envoy.retry_host_predicates.previous_hosts");
            ASSERT_TRUE(predicate->mutable_typed_config()->PackFrom(
                envoy::extensions::retry::host::previous_hosts::v3::PreviousHostsPredicate{}));
          }
        });
    HttpIntegrationTest::initialize();
  }

  void TearDown() override { cleanupUpstreamAndDownstream(); }

  void waitForConnect(FakeStreamPtr& stream) {
    if (!fake_upstream_connection_) {
      ASSERT_TRUE(
          fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, fake_upstream_connection_));
    }
    ASSERT_TRUE(fake_upstream_connection_->waitForNewStream(*dispatcher_, stream));
    ASSERT_TRUE(stream->waitForHeadersComplete());
    EXPECT_EQ("CONNECT", stream->headers().getMethodValue());
    EXPECT_EQ(0, stream->bodyLength());
  }

  void acceptAndRespond(FakeStream& stream, IntegrationStreamDecoder& response) {
    stream.encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
    ASSERT_TRUE(stream.waitForData(*dispatcher_, [](const std::string& data) {
      return absl::StrContains(data, "\r\n\r\npayload");
    })) << stream.body().toString();
    EXPECT_THAT(stream.body().toString(), HasSubstr("POST / HTTP/1.1\r\n"));
    stream.encodeData("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", false);
    ASSERT_TRUE(response.waitForEndStream());
    EXPECT_EQ("200", response.headers().getStatusValue());
    EXPECT_EQ("ok", response.body());
  }

  const Http::TestRequestHeaderMapImpl request_headers_{{":method", "POST"},
                                                        {":path", "/"},
                                                        {":scheme", "http"},
                                                        {":authority", "service"},
                                                        {"content-length", "7"}};
  bool retry_{false};
  bool separate_peers_{false};
  bool goaway_preference_{false};
};

INSTANTIATE_TEST_SUITE_P(TcpProxyPoolImplementations, HboneIntegrationTest, testing::Bool());

TEST_P(HboneIntegrationTest, ConnectRejectionIsConnectionFailureWithoutPayload) {
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  waitForConnect(upstream_request_);
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "503"}}, true);
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("503", response->headers().getStatusValue());
  EXPECT_EQ(0, upstream_request_->bodyLength());
  test_server_->waitForCounter("cluster.service.upstream_cx_connect_fail", Eq(1));
}

TEST_P(HboneIntegrationTest, RetriesPostOnAnotherHostAfterConnectRejection) {
  retry_ = true;
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  FakeStreamPtr rejected;
  waitForConnect(rejected);
  const std::string rejected_host(rejected->headers().getHostValue());
  rejected->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "503"}}, true);
  waitForConnect(upstream_request_);
  EXPECT_NE(rejected_host, upstream_request_->headers().getHostValue());
  acceptAndRespond(*upstream_request_, *response);
  EXPECT_EQ(0, rejected->bodyLength());
  test_server_->waitForCounter("cluster.service.upstream_rq_retry", Eq(1));
  test_server_->waitForCounter("cluster.cluster_0.upstream_cx_total", Eq(1));
}

TEST_P(HboneIntegrationTest, Connect200WithEndStreamDoesNotReleasePayload) {
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  waitForConnect(upstream_request_);
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, true);
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("503", response->headers().getStatusValue());
  EXPECT_EQ(0, upstream_request_->bodyLength());
  test_server_->waitForCounter("cluster.service.upstream_cx_connect_fail", Eq(1));
}

TEST_P(HboneIntegrationTest, ResetBeforeConnectAcceptanceDoesNotReleasePayload) {
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  waitForConnect(upstream_request_);
  upstream_request_->encodeResetStream();
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("503", response->headers().getStatusValue());
  EXPECT_EQ(0, upstream_request_->bodyLength());
  test_server_->waitForCounter("cluster.service.upstream_cx_connect_fail", Eq(1));
}

TEST_P(HboneIntegrationTest, ConcurrentConnectStreamsFailIndependently) {
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  waitForConnect(upstream_request_);

  auto second_client = makeHttpConnection(lookupPort("http"));
  auto second_response = second_client->makeRequestWithBody(request_headers_, "payload");
  FakeStreamPtr rejected;
  waitForConnect(rejected);
  rejected->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "503"}}, true);
  ASSERT_TRUE(second_response->waitForEndStream());
  EXPECT_EQ("503", second_response->headers().getStatusValue());
  acceptAndRespond(*upstream_request_, *response);
  EXPECT_EQ(0, rejected->bodyLength());
  test_server_->waitForCounter("cluster.cluster_0.upstream_cx_total", Eq(1));
  second_client->close();
}

TEST_P(HboneIntegrationTest, GracefulGoawayPreservesAcceptedStream) {
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  waitForConnect(upstream_request_);
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  fake_upstream_connection_->encodeGoAway();
  ASSERT_TRUE(upstream_request_->waitForData(*dispatcher_, [](const std::string& data) {
    return absl::StrContains(data, "\r\n\r\npayload");
  }));
  upstream_request_->encodeData("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", false);
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("200", response->headers().getStatusValue());
  EXPECT_EQ("ok", response->body());
}

TEST_P(HboneIntegrationTest, NoConnectFailureRetryAfterApplicationBytesWereSent) {
  retry_ = true;
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  waitForConnect(upstream_request_);
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  ASSERT_TRUE(upstream_request_->waitForData(*dispatcher_, [](const std::string& data) {
    return absl::StrContains(data, "\r\n\r\npayload");
  }));
  upstream_request_->encodeResetStream();
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("503", response->headers().getStatusValue());
  EXPECT_EQ(0, test_server_->counter("cluster.service.upstream_rq_retry")->value());
  EXPECT_EQ(0, test_server_->counter("cluster.service.upstream_cx_connect_fail")->value());
}

TEST_P(HboneIntegrationTest, GoawayWithoutPreferenceStillSelectsRetiredPeer) {
  separate_peers_ = true;
  fake_upstreams_count_ = 2;
  retry_ = false;
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto first_response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  const auto first_peer =
      waitForNextUpstreamConnection({0, 1}, TestUtility::DefaultTimeout, fake_upstream_connection_);
  ASSERT_TRUE(first_peer.has_value());
  ASSERT_TRUE(fake_upstream_connection_->waitForNewStream(*dispatcher_, upstream_request_));
  ASSERT_TRUE(upstream_request_->waitForHeadersComplete());
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  fake_upstream_connection_->encodeGoAway();
  test_server_->waitForCounter(
      absl::StrCat("cluster.cluster_", first_peer.value(), ".upstream_cx_close_notify"), Eq(1));

  // Round robin first selects the other peer, then returns to the retired peer.
  auto second_client = makeHttpConnection(lookupPort("http"));
  auto second_response = second_client->makeRequestWithBody(request_headers_, "payload");
  FakeHttpConnectionPtr other;
  ASSERT_TRUE(fake_upstreams_[1 - first_peer.value()]->waitForHttpConnection(*dispatcher_, other));
  FakeStreamPtr second;
  ASSERT_TRUE(other->waitForNewStream(*dispatcher_, second));
  ASSERT_TRUE(second->waitForHeadersComplete());
  second->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  ASSERT_TRUE(second->waitForData(*dispatcher_, [](const std::string& data) {
    return absl::StrContains(data, "\r\n\r\npayload");
  }));
  second->encodeData("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n", true);
  ASSERT_TRUE(second_response->waitForEndStream());
  ASSERT_TRUE(second->waitForEndStream(*dispatcher_));
  second_client->close();

  auto third_client = makeHttpConnection(lookupPort("http"));
  auto third_response = third_client->makeRequestWithBody(request_headers_, "payload");
  FakeHttpConnectionPtr retired;
  ASSERT_TRUE(fake_upstreams_[first_peer.value()]->waitForHttpConnection(*dispatcher_, retired));
  FakeStreamPtr rejected;
  ASSERT_TRUE(retired->waitForNewStream(*dispatcher_, rejected));
  ASSERT_TRUE(rejected->waitForHeadersComplete());
  rejected->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "503"}}, true);
  ASSERT_TRUE(third_response->waitForEndStream());
  EXPECT_EQ("503", third_response->headers().getStatusValue());
  EXPECT_EQ(0, rejected->bodyLength());
  third_client->close();
  upstream_request_->encodeData("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", false);
  ASSERT_TRUE(first_response->waitForEndStream());
  EXPECT_EQ("200", first_response->headers().getStatusValue());
  ASSERT_TRUE(other->close());
  ASSERT_TRUE(other->waitForDisconnect());
  ASSERT_TRUE(retired->close());
  ASSERT_TRUE(retired->waitForDisconnect());
}

TEST_P(HboneIntegrationTest, GoawayDuringRejectedConnectRetriesWithoutPayload) {
  retry_ = true;
  separate_peers_ = true;
  goaway_preference_ = true;
  fake_upstreams_count_ = 2;
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  const auto first_peer =
      waitForNextUpstreamConnection({0, 1}, TestUtility::DefaultTimeout, fake_upstream_connection_);
  ASSERT_TRUE(first_peer.has_value());
  ASSERT_TRUE(fake_upstream_connection_->waitForNewStream(*dispatcher_, upstream_request_));
  ASSERT_TRUE(upstream_request_->waitForHeadersComplete());
  fake_upstream_connection_->encodeGoAway();
  test_server_->waitForCounter("istio_hbone.goaway_received", Eq(1));
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "503"}}, true);
  FakeHttpConnectionPtr other;
  ASSERT_TRUE(fake_upstreams_[1 - first_peer.value()]->waitForHttpConnection(*dispatcher_, other));
  FakeStreamPtr accepted;
  ASSERT_TRUE(other->waitForNewStream(*dispatcher_, accepted));
  ASSERT_TRUE(accepted->waitForHeadersComplete());
  EXPECT_EQ(0, upstream_request_->bodyLength());
  EXPECT_EQ(0, accepted->bodyLength());
  accepted->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  ASSERT_TRUE(accepted->waitForData(*dispatcher_, [](const std::string& data) {
    return absl::StrContains(data, "\r\n\r\npayload");
  }));
  accepted->encodeData("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", false);
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("200", response->headers().getStatusValue());
  EXPECT_EQ(1, test_server_->counter("cluster.service.upstream_rq_retry")->value());
  EXPECT_EQ(0, upstream_request_->bodyLength());
  ASSERT_TRUE(other->close());
  ASSERT_TRUE(other->waitForDisconnect());
}

// The peer is retired before subsequent requests start. Avoiding it must not
// depend on those requests first consuming a CONNECT retry on that peer.
TEST_P(HboneIntegrationTest, GoawayPrefersAnotherEndpointAndPreservesAcceptedStream) {
  separate_peers_ = true;
  goaway_preference_ = true;
  fake_upstreams_count_ = 2;
  initialize();
  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto first_response = codec_client_->makeRequestWithBody(request_headers_, "payload");
  const auto first_peer =
      waitForNextUpstreamConnection({0, 1}, TestUtility::DefaultTimeout, fake_upstream_connection_);
  ASSERT_TRUE(first_peer.has_value());
  ASSERT_TRUE(fake_upstream_connection_->waitForNewStream(*dispatcher_, upstream_request_));
  ASSERT_TRUE(upstream_request_->waitForHeadersComplete());
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  fake_upstream_connection_->encodeGoAway();
  test_server_->waitForCounter("istio_hbone.goaway_received", Eq(1));
  ASSERT_TRUE(upstream_request_->waitForData(*dispatcher_, [](const std::string& data) {
    return absl::StrContains(data, "\r\n\r\npayload");
  }));

  // Keep the first request active while the other endpoint serves new requests.
  FakeHttpConnectionPtr other_connection;
  const auto other_peer = 1 - first_peer.value();
  for (int i = 0; i < 6; ++i) {
    auto client = makeHttpConnection(lookupPort("http"));
    auto response = client->makeRequestWithBody(request_headers_, "payload");
    if (!other_connection) {
      ASSERT_TRUE(
          fake_upstreams_[other_peer]->waitForHttpConnection(*dispatcher_, other_connection));
    }
    FakeStreamPtr stream;
    ASSERT_TRUE(other_connection->waitForNewStream(*dispatcher_, stream));
    ASSERT_TRUE(stream->waitForHeadersComplete());
    EXPECT_EQ(0, stream->bodyLength());
    stream->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
    ASSERT_TRUE(stream->waitForData(*dispatcher_, [](const std::string& data) {
      return absl::StrContains(data, "\r\n\r\npayload");
    }));
    // Retire the inner HTTP connection so each request exercises host selection.
    stream->encodeData("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok", true);
    ASSERT_TRUE(response->waitForEndStream());
    EXPECT_EQ("200", response->headers().getStatusValue());
    ASSERT_TRUE(stream->waitForEndStream(*dispatcher_));
    client->close();
  }
  EXPECT_GT(test_server_->counter("istio_hbone.hosts_skipped")->value(), 0);
  EXPECT_EQ(0, test_server_->counter("cluster.service.upstream_cx_connect_fail")->value());
  EXPECT_EQ(0, test_server_->counter("cluster.service.upstream_rq_retry")->value());
  EXPECT_EQ(0, test_server_->counter("istio_hbone.fallback_selections")->value());

  upstream_request_->encodeData("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", false);
  ASSERT_TRUE(first_response->waitForEndStream());
  EXPECT_EQ("200", first_response->headers().getStatusValue());
  ASSERT_TRUE(other_connection->close());
  ASSERT_TRUE(other_connection->waitForDisconnect());
}

} // namespace
} // namespace Hbone
} // namespace Istio
} // namespace Extensions
} // namespace Envoy
