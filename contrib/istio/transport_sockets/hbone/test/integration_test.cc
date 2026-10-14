#include "envoy/extensions/transport_sockets/raw_buffer/v3/raw_buffer.pb.h"
#include "envoy/extensions/transport_sockets/tls/v3/tls.pb.h"

#include "test/integration/http_integration.h"

#include "contrib/envoy/extensions/transport_sockets/hbone/v3/hbone.pb.h"

namespace Envoy {
namespace {

// Fake upstream speaks outer H2. Application requests pass through the ordinary router and H1
// pool; the fake peer inspects/responds to the H1 bytes carried inside CONNECT DATA.
class HboneIntegrationTest : public testing::Test, public HttpIntegrationTest {
public:
  HboneIntegrationTest()
      : HttpIntegrationTest(Http::CodecType::HTTP1, Network::Address::IpVersion::v4) {
    setUpstreamProtocol(Http::CodecType::HTTP2);
    config_helper_.addConfigModifier([this](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
      auto* resources = bootstrap.mutable_static_resources();
      auto* app = resources->mutable_clusters(0);
      auto outer = *app;
      outer.set_name("hbone_outer");
      outer.set_type(envoy::config::cluster::v3::Cluster::ORIGINAL_DST);
      outer.set_lb_policy(envoy::config::cluster::v3::Cluster::CLUSTER_PROVIDED);
      outer.clear_load_assignment();
      outer.mutable_original_dst_lb_config()->mutable_upstream_port_override()->set_value(
          fake_upstreams_[0]->localAddress()->ip()->port());
      // The application address identifies the workload and target port, not the HBONE socket.
      app->mutable_load_assignment()
          ->mutable_endpoints(0)
          ->mutable_lb_endpoints(0)
          ->mutable_endpoint()
          ->mutable_address()
          ->mutable_socket_address()
          ->set_port_value(8080);
      ConfigHelper::HttpProtocolOptions options;
      options.mutable_explicit_http_config()->mutable_http_protocol_options();
      ConfigHelper::setProtocolOptions(*app, options);
      envoy::extensions::transport_sockets::hbone::v3::Hbone hbone;
      hbone.set_cluster("hbone_outer");
      hbone.mutable_connect_timeout()->set_seconds(2);
      auto* inner = hbone.mutable_transport_socket();
      inner->set_name("envoy.transport_sockets.raw_buffer");
      std::ignore = inner->mutable_typed_config()->PackFrom(
          envoy::extensions::transport_sockets::raw_buffer::v3::RawBuffer());
      app->mutable_transport_socket()->set_name("envoy.transport_sockets.hbone");
      std::ignore = app->mutable_transport_socket()->mutable_typed_config()->PackFrom(hbone);
      *resources->add_clusters() = outer;
    });
  }

  void initialize() override {
    HttpIntegrationTest::initialize();
    codec_client_ = makeHttpConnection(lookupPort("http"));
  }

  void enableConnectRetries() {
    config_helper_.addConfigModifier([](ConfigHelper::HttpConnectionManager& hcm) {
      auto* retry = hcm.mutable_route_config()
                        ->mutable_virtual_hosts(0)
                        ->mutable_routes(0)
                        ->mutable_route()
                        ->mutable_retry_policy();
      retry->set_retry_on("connect-failure");
      retry->mutable_num_retries()->set_value(1);
    });
  }

  void connectStream(FakeStreamPtr& stream, absl::string_view authority = "127.0.0.1:8080") {
    if (!fake_upstream_connection_) {
      ASSERT_TRUE(
          fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, fake_upstream_connection_));
    }
    ASSERT_TRUE(fake_upstream_connection_->waitForNewStream(*dispatcher_, stream));
    ASSERT_TRUE(stream->waitForHeadersComplete());
    EXPECT_EQ("CONNECT", stream->headers().getMethodValue());
    if (!authority.empty()) {
      EXPECT_EQ(authority, stream->headers().getHostValue());
    }
    EXPECT_EQ(0, stream->bodyLength());
  }

  void acceptAndRespond(FakeStream& stream, absl::string_view body = "ok") {
    stream.encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
    ASSERT_TRUE(stream.waitForData(*dispatcher_, [](const std::string& data) {
      return data.find("\r\n\r\n") != std::string::npos;
    }));
    stream.encodeData(
        absl::StrCat("HTTP/1.1 200 OK\r\ncontent-length: ", body.size(), "\r\n\r\n", body), false);
  }

  Http::TestRequestHeaderMapImpl post_headers_{{":method", "POST"},
                                               {":path", "/"},
                                               {":scheme", "http"},
                                               {":authority", "service"},
                                               {"x-envoy-retry-on", "connect-failure"}};
};

TEST_F(HboneIntegrationTest, RejectedConnectRetriesPostWithoutSendingApplicationData) {
  enableConnectRetries();
  initialize();
  auto response = codec_client_->makeRequestWithBody(post_headers_, "non-idempotent-body");
  FakeStreamPtr rejected;
  ASSERT_NO_FATAL_FAILURE(connectStream(rejected));
  rejected->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "503"}}, true);
  ASSERT_TRUE(rejected->waitForReset());
  EXPECT_EQ(0, rejected->bodyLength());
  FakeStreamPtr accepted;
  // A second CONNECT on the existing outer connection proves pool reuse across retry attempts.
  ASSERT_NO_FATAL_FAILURE(connectStream(accepted));
  ASSERT_NO_FATAL_FAILURE(acceptAndRespond(*accepted));
  ASSERT_TRUE(accepted->waitForData(*dispatcher_, [](const std::string& data) {
    return data.find("non-idempotent-body") != std::string::npos;
  }));
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("200", response->headers().getStatusValue());
  EXPECT_EQ("ok", response->body());
  EXPECT_EQ(0, rejected->bodyLength());
  EXPECT_EQ(1, test_server_->counter("cluster.hbone_outer.upstream_cx_total")->value());
  EXPECT_EQ(1, test_server_->counter("cluster.cluster_0.upstream_rq_retry")->value());
  cleanupUpstreamAndDownstream();
}

TEST_F(HboneIntegrationTest, ApplicationKeepaliveReusesAcceptedTunnel) {
  initialize();
  auto first = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  FakeStreamPtr tunnel;
  ASSERT_NO_FATAL_FAILURE(connectStream(tunnel));
  ASSERT_NO_FATAL_FAILURE(acceptAndRespond(*tunnel));
  ASSERT_TRUE(first->waitForEndStream());
  tunnel->body();
  auto second = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  ASSERT_TRUE(tunnel->waitForData(*dispatcher_, [](const std::string& data) {
    return data.find("\r\n\r\n") != std::string::npos;
  }));
  tunnel->encodeData("HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\ntwo", false);
  ASSERT_TRUE(second->waitForEndStream());
  EXPECT_EQ("two", second->body());
  EXPECT_EQ(1, test_server_->counter("cluster.hbone_outer.upstream_rq_total")->value());
  cleanupUpstreamAndDownstream();
}

TEST_F(HboneIntegrationTest, ConcurrentApplicationConnectionsMultiplexOuterH2) {
  initialize();
  auto second_client = makeHttpConnection(lookupPort("http"));
  auto first = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  FakeStreamPtr one;
  ASSERT_NO_FATAL_FAILURE(connectStream(one));
  one->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  ASSERT_TRUE(one->waitForData(*dispatcher_, 1));
  auto second = second_client->makeHeaderOnlyRequest(default_request_headers_);
  FakeStreamPtr two;
  ASSERT_NO_FATAL_FAILURE(connectStream(two));
  ASSERT_NO_FATAL_FAILURE(acceptAndRespond(*two, "two"));
  ASSERT_TRUE(second->waitForEndStream());
  one->encodeData("HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\none", false);
  ASSERT_TRUE(first->waitForEndStream());
  EXPECT_EQ("one", first->body());
  EXPECT_EQ("two", second->body());
  EXPECT_EQ(1, test_server_->counter("cluster.hbone_outer.upstream_cx_total")->value());
  EXPECT_EQ(2, test_server_->counter("cluster.hbone_outer.upstream_rq_total")->value());
  second_client->close();
  cleanupUpstreamAndDownstream();
}

TEST_F(HboneIntegrationTest, ResetAfterApplicationWriteDoesNotRetryPost) {
  enableConnectRetries();
  initialize();
  auto response = codec_client_->makeRequestWithBody(post_headers_, "non-idempotent-body");
  FakeStreamPtr tunnel;
  ASSERT_NO_FATAL_FAILURE(connectStream(tunnel));
  tunnel->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  ASSERT_TRUE(tunnel->waitForData(*dispatcher_, [](const std::string& data) {
    return data.find("non-idempotent-body") != std::string::npos;
  }));
  tunnel->encodeResetStream();
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("503", response->headers().getStatusValue());
  EXPECT_EQ(1, test_server_->counter("cluster.hbone_outer.upstream_rq_total")->value());
  EXPECT_EQ(0, test_server_->counter("cluster.cluster_0.upstream_rq_retry")->value());
  cleanupUpstreamAndDownstream();
}

TEST_F(HboneIntegrationTest, DifferentPortsOnOneWorkloadShareOuterConnection) {
  config_helper_.addConfigModifier([](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
    auto* endpoints = bootstrap.mutable_static_resources()
                          ->mutable_clusters(0)
                          ->mutable_load_assignment()
                          ->mutable_endpoints(0);
    auto endpoint = endpoints->lb_endpoints(0);
    endpoint.mutable_endpoint()->mutable_address()->mutable_socket_address()->set_port_value(8081);
    *endpoints->add_lb_endpoints() = endpoint;
  });
  initialize();
  auto second_client = makeHttpConnection(lookupPort("http"));
  auto first = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  FakeStreamPtr one;
  ASSERT_NO_FATAL_FAILURE(connectStream(one, ""));
  auto second = second_client->makeHeaderOnlyRequest(default_request_headers_);
  FakeStreamPtr two;
  ASSERT_NO_FATAL_FAILURE(connectStream(two, ""));
  EXPECT_NE(one->headers().getHostValue(), two->headers().getHostValue());
  ASSERT_NO_FATAL_FAILURE(acceptAndRespond(*one));
  ASSERT_NO_FATAL_FAILURE(acceptAndRespond(*two));
  ASSERT_TRUE(first->waitForEndStream());
  ASSERT_TRUE(second->waitForEndStream());
  EXPECT_EQ(1, test_server_->counter("cluster.hbone_outer.upstream_cx_total")->value());
  second_client->close();
  cleanupUpstreamAndDownstream();
}

TEST_F(HboneIntegrationTest, DifferentWorkloadIpsUseDifferentOuterConnections) {
  // One fake ztunnel listener accepts connections to two pod loopback IPs on the same port.
  upstream_address_fn_ = [](uint32_t) { return Network::Utility::getIpv4AnyAddress(); };
  config_helper_.addConfigModifier([](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
    auto* endpoints = bootstrap.mutable_static_resources()
                          ->mutable_clusters(0)
                          ->mutable_load_assignment()
                          ->mutable_endpoints(0);
    auto endpoint = endpoints->lb_endpoints(0);
    endpoint.mutable_endpoint()->mutable_address()->mutable_socket_address()->set_address(
        "127.0.0.2");
    *endpoints->add_lb_endpoints() = endpoint;
  });
  initialize();
  auto second_client = makeHttpConnection(lookupPort("http"));
  auto first = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  FakeStreamPtr one;
  ASSERT_NO_FATAL_FAILURE(connectStream(one, ""));
  auto second = second_client->makeHeaderOnlyRequest(default_request_headers_);
  FakeHttpConnectionPtr other_connection;
  ASSERT_TRUE(fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, other_connection));
  FakeStreamPtr two;
  ASSERT_TRUE(other_connection->waitForNewStream(*dispatcher_, two));
  ASSERT_TRUE(two->waitForHeadersComplete());
  EXPECT_NE(one->headers().getHostValue(), two->headers().getHostValue());
  ASSERT_NO_FATAL_FAILURE(acceptAndRespond(*one));
  ASSERT_NO_FATAL_FAILURE(acceptAndRespond(*two));
  ASSERT_TRUE(first->waitForEndStream());
  ASSERT_TRUE(second->waitForEndStream());
  EXPECT_EQ(2, test_server_->counter("cluster.hbone_outer.upstream_cx_total")->value());
  second_client->close();
  ASSERT_TRUE(other_connection->close());
  ASSERT_TRUE(other_connection->waitForDisconnect());
  cleanupUpstreamAndDownstream();
}

TEST_F(HboneIntegrationTest, ApplicationHttp2MultiplexesInsideOneConnect) {
  config_helper_.addConfigModifier([](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
    ConfigHelper::HttpProtocolOptions options;
    options.mutable_explicit_http_config()->mutable_http2_protocol_options();
    ConfigHelper::setProtocolOptions(*bootstrap.mutable_static_resources()->mutable_clusters(0),
                                     options);
  });
  initialize();
  auto second_client = makeHttpConnection(lookupPort("http"));
  auto first = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  FakeStreamPtr tunnel;
  ASSERT_NO_FATAL_FAILURE(connectStream(tunnel));
  tunnel->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  tunnel->encodeData(std::string(Http2Frame::makeEmptySettingsFrame()), false);
  ASSERT_TRUE(tunnel->waitForData(*dispatcher_, 24));
  auto second = second_client->makeHeaderOnlyRequest(default_request_headers_);
  ASSERT_TRUE(tunnel->waitForData(*dispatcher_, [](const std::string& bytes) {
    // The two application streams use IDs 1 and 3. Inspect complete frame headers after the
    // client preface, leaving HPACK decoding to the existing codec tests.
    for (size_t offset = 24; offset + 9 <= bytes.size();) {
      const auto* frame = reinterpret_cast<const uint8_t*>(bytes.data() + offset);
      const size_t size = (size_t(frame[0]) << 16) | (size_t(frame[1]) << 8) | frame[2];
      const uint32_t id = (uint32_t(frame[5] & 0x7f) << 24) | (uint32_t(frame[6]) << 16) |
                          (uint32_t(frame[7]) << 8) | frame[8];
      if (offset + 9 + size > bytes.size()) {
        return false;
      }
      if (frame[3] == 1 && id == 3) {
        return true;
      }
      offset += 9 + size;
    }
    return false;
  }));
  tunnel->encodeData(std::string(Http2Frame::makeHeadersFrameWithStatus("200", 3)), false);
  ASSERT_TRUE(second->waitForEndStream());
  tunnel->encodeData(std::string(Http2Frame::makeHeadersFrameWithStatus("200", 1)), false);
  ASSERT_TRUE(first->waitForEndStream());
  EXPECT_EQ("200", first->headers().getStatusValue());
  EXPECT_EQ("200", second->headers().getStatusValue());
  EXPECT_EQ(1, test_server_->counter("cluster.hbone_outer.upstream_rq_total")->value());
  second_client->close();
  cleanupUpstreamAndDownstream();
}

TEST_F(HboneIntegrationTest, OuterTlsUsesConfiguredProxyCertificate) {
  upstream_tls_ = true;
  config_helper_.addConfigModifier([](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
    envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext tls;
    auto* common = tls.mutable_common_tls_context();
    common->add_alpn_protocols("h2");
    common->mutable_tls_params()->set_tls_minimum_protocol_version(
        envoy::extensions::transport_sockets::tls::v3::TlsParameters::TLSv1_3);
    common->mutable_tls_params()->set_tls_maximum_protocol_version(
        envoy::extensions::transport_sockets::tls::v3::TlsParameters::TLSv1_3);
    const auto directory = TestEnvironment::runfilesDirectory() + "/test/config/integration/certs/";
    common->mutable_validation_context()->mutable_trusted_ca()->set_filename(directory +
                                                                             "upstreamcacert.pem");
    auto* certificate = common->add_tls_certificates();
    certificate->mutable_certificate_chain()->set_filename(directory + "clientcert.pem");
    certificate->mutable_private_key()->set_filename(directory + "clientkey.pem");
    auto* transport =
        bootstrap.mutable_static_resources()->mutable_clusters(1)->mutable_transport_socket();
    transport->set_name("envoy.transport_sockets.tls");
    std::ignore = transport->mutable_typed_config()->PackFrom(tls);
    // Make the application pool supply a caller-selected SNI override. It must not reach mesh TLS.
    ConfigHelper::HttpProtocolOptions options;
    options.mutable_explicit_http_config()->mutable_http_protocol_options();
    options.mutable_upstream_http_protocol_options()->set_auto_sni(true);
    ConfigHelper::setProtocolOptions(*bootstrap.mutable_static_resources()->mutable_clusters(0),
                                     options);
  });
  initialize();
  auto headers = default_request_headers_;
  headers.setHost("caller-selected.example");
  auto response = codec_client_->makeHeaderOnlyRequest(headers);
  FakeStreamPtr tunnel;
  ASSERT_NO_FATAL_FAILURE(connectStream(tunnel));
  ASSERT_TRUE(fake_upstream_connection_->sharedConnection().executeOnDispatcher(
      [](Network::Connection& connection) {
        ASSERT_NE(nullptr, connection.ssl());
        EXPECT_TRUE(connection.ssl()->peerCertificatePresented());
        EXPECT_THAT(connection.ssl()->uriSanPeerCertificate(),
                    testing::Contains("spiffe://lyft.com/frontend-team"));
        EXPECT_NE("caller-selected.example", connection.requestedServerName());
      }));
  ASSERT_NO_FATAL_FAILURE(acceptAndRespond(*tunnel));
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("200", response->headers().getStatusValue());
  cleanupUpstreamAndDownstream();
}

TEST_F(HboneIntegrationTest, RejectionAndGoAwayLeaveAcceptedStreamUsable) {
  initialize();
  auto second_client = makeHttpConnection(lookupPort("http"));
  auto first = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  FakeStreamPtr accepted;
  ASSERT_NO_FATAL_FAILURE(connectStream(accepted));
  accepted->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  ASSERT_TRUE(accepted->waitForData(*dispatcher_, 1));
  auto second = second_client->makeHeaderOnlyRequest(default_request_headers_);
  FakeStreamPtr rejected;
  ASSERT_NO_FATAL_FAILURE(connectStream(rejected));
  rejected->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "401"}}, true);
  ASSERT_TRUE(second->waitForEndStream());
  EXPECT_EQ("503", second->headers().getStatusValue());
  EXPECT_EQ(0, rejected->bodyLength());
  fake_upstream_connection_->encodeGoAway();
  test_server_->waitForCounter("cluster.hbone_outer.upstream_cx_close_notify", testing::Ge(1));
  accepted->encodeData("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok", false);
  ASSERT_TRUE(first->waitForEndStream());
  EXPECT_EQ("200", first->headers().getStatusValue());
  EXPECT_EQ("ok", first->body());
  second_client->close();
  cleanupUpstreamAndDownstream();
}

} // namespace
} // namespace Envoy
