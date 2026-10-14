#include "source/common/network/connection_impl.h"
#include "source/common/network/default_client_connection_factory.h"
#include "source/common/network/filter_state_dst_address.h"
#include "source/common/network/raw_buffer_socket.h"
#include "source/common/network/utility.h"
#include "source/extensions/clusters/original_dst/original_dst_cluster.h"

#include "test/mocks/http/conn_pool.h"
#include "test/mocks/http/stream_encoder.h"
#include "test/mocks/network/mocks.h"
#include "test/mocks/upstream/cluster_manager.h"
#include "test/test_common/simulated_time_system.h"
#include "test/test_common/utility.h"

#include "contrib/istio/transport_sockets/hbone/source/hbone.h"

namespace Envoy {
namespace Extensions {
namespace TransportSockets {
namespace Hbone {
namespace {

using testing::_;
using testing::Invoke;
using testing::Return;
using testing::ReturnRef;
using testing::StrictMock;

class IoHandleTest : public testing::Test {
protected:
  IoHandleTest()
      : api_(Api::createApiForTest(time_system_)), dispatcher_(api_->allocateDispatcher("test")),
        info_(time_system_, nullptr, StreamInfo::FilterState::LifeSpan::Connection),
        destination_(Network::Utility::parseInternetAddressAndPortNoThrow("10.0.0.1:8080")),
        failure_(std::make_shared<std::string>()),
        handle_(*dispatcher_, cluster_manager_, "outer", std::chrono::milliseconds(100), 16,
                failure_) {
    EXPECT_CALL(encoder_, getStream()).WillRepeatedly(ReturnRef(stream_));
  }

  ~IoHandleTest() override { handle_.close(); }

  void expectPool() {
    EXPECT_CALL(cluster_manager_, getThreadLocalCluster("outer")).WillOnce(Return(&cluster_));
    EXPECT_CALL(cluster_, chooseHost(_))
        .WillOnce(Invoke([this](Upstream::LoadBalancerContext* ctx) {
          EXPECT_EQ(nullptr, ctx->downstreamConnection());
          EXPECT_EQ(nullptr, ctx->upstreamTransportSocketOptions());
          EXPECT_EQ(nullptr, ctx->upstreamSocketOptions());
          const auto* dst =
              ctx->requestStreamInfo()->filterState()->getDataReadOnly<Network::AddressObject>(
                  Upstream::OriginalDstClusterFilterStateKey);
          EXPECT_EQ(destination_->asString(), dst->address()->asString());
          return Upstream::HostSelectionResponse(host_);
        }));
    EXPECT_CALL(*host_, address()).WillRepeatedly(Return(destination_));
    EXPECT_CALL(cluster_, httpConnPool(testing::Eq(host_), Upstream::ResourcePriority::Default,
                                       testing::Optional(Http::Protocol::Http2), _))
        .WillOnce(Return(Upstream::HttpPoolData([] {}, &pool_)));
  }

  void readyEncoder(Http::ConnectionPool::Callbacks& callbacks) {
    EXPECT_CALL(stream_, addCallbacks(_));
    EXPECT_CALL(encoder_, encodeHeaders(_, false))
        .WillOnce(Invoke([this](const auto& headers, bool) {
          EXPECT_EQ("CONNECT", headers.getMethodValue());
          EXPECT_EQ(destination_->asString(), headers.getHostValue());
          EXPECT_EQ(nullptr, headers.Path());
          return absl::OkStatus();
        }));
    callbacks.onPoolReady(encoder_, host_, info_, Http::Protocol::Http2);
  }

  void connect(IoHandle& handle) {
    expectPool();
    EXPECT_CALL(pool_, newStream(_, _, _))
        .WillOnce(Invoke([this](auto&, auto& callbacks, const auto& options) {
          EXPECT_FALSE(options.can_send_early_data_);
          EXPECT_FALSE(options.can_use_http3_);
          readyEncoder(callbacks);
          return nullptr;
        }));
    EXPECT_EQ(EINPROGRESS, handle.connect(destination_).errno_);
  }

  void accept(IoHandle& handle) {
    auto headers = Http::ResponseHeaderMapImpl::create();
    headers->setStatus(200);
    handle.decodeHeaders(std::move(headers), false);
  }

  void expectReset() {
    EXPECT_CALL(stream_, removeCallbacks(_));
    EXPECT_CALL(stream_, resetStream(Http::StreamResetReason::LocalReset));
  }

  void events() {
    handle_.initializeFileEvent(
        *dispatcher_,
        [this](uint32_t events) {
          observed_events_ |= events;
          return absl::OkStatus();
        },
        Event::FileTriggerType::Edge, Event::FileReadyType::Read | Event::FileReadyType::Write);
  }
  void run() { dispatcher_->run(Event::Dispatcher::RunType::NonBlock); }

  Event::SimulatedTimeSystem time_system_;
  Api::ApiPtr api_;
  Event::DispatcherPtr dispatcher_;
  StrictMock<Upstream::MockClusterManager> cluster_manager_;
  StrictMock<Upstream::MockThreadLocalCluster> cluster_;
  std::shared_ptr<StrictMock<Upstream::MockHost>> host_{
      std::make_shared<StrictMock<Upstream::MockHost>>()};
  StrictMock<Http::ConnectionPool::MockInstance> pool_;
  StrictMock<Http::MockRequestEncoder> encoder_;
  StrictMock<Http::MockStream> stream_;
  StreamInfo::StreamInfoImpl info_;
  Network::Address::InstanceConstSharedPtr destination_;
  std::shared_ptr<std::string> failure_;
  IoHandle handle_;
  uint32_t observed_events_{0};
};

TEST_F(IoHandleTest, WithholdsReadinessAndBytesUntilAccepted) {
  events();
  connect(handle_);
  Buffer::OwnedImpl data("POST / HTTP/1.1\r\n\r\nbody");
  EXPECT_EQ(Api::IoError::IoErrorCode::Again, handle_.write(data).err_->getErrorCode());
  handle_.activateFileEvents(Event::FileReadyType::Write);
  handle_.enableFileEvents(Event::FileReadyType::Write);
  run();
  EXPECT_EQ(0, observed_events_);
  accept(handle_);
  run();
  EXPECT_EQ(Event::FileReadyType::Write, observed_events_);
  EXPECT_TRUE(handle_.wasConnected());
  EXPECT_CALL(encoder_, encodeData(_, false)).WillOnce(Invoke([](Buffer::Instance& data, bool) {
    EXPECT_EQ(16, data.length());
    data.drain(data.length());
  }));
  EXPECT_EQ(16, handle_.write(data).return_value_);
  expectReset();
}

TEST_F(IoHandleTest, RejectsConnectWithoutBecomingConnected) {
  events();
  connect(handle_);
  expectReset();
  auto headers = Http::ResponseHeaderMapImpl::create();
  headers->setStatus(503);
  handle_.decodeHeaders(std::move(headers), false);
  run();
  EXPECT_EQ(Event::FileReadyType::Write, observed_events_);
  EXPECT_FALSE(handle_.wasConnected());
  EXPECT_EQ("hbone_connect_response: 503", *failure_);
  int error = 0;
  socklen_t size = sizeof(error);
  EXPECT_EQ(0, handle_.getOption(SOL_SOCKET, SO_ERROR, &error, &size).return_value_);
  EXPECT_EQ(ECONNREFUSED, error);
}

TEST_F(IoHandleTest, AcceptanceTimeoutResetsOnlyThisStream) {
  events();
  connect(handle_);
  expectReset();
  time_system_.advanceTimeAndRun(std::chrono::milliseconds(101), *dispatcher_,
                                 Event::Dispatcher::RunType::NonBlock);
  run();
  EXPECT_EQ("hbone_connect_timeout", *failure_);
  EXPECT_FALSE(handle_.wasConnected());
  EXPECT_EQ(Event::FileReadyType::Write, observed_events_);
}

TEST_F(IoHandleTest, CloseCancelsPendingPoolAndEvents) {
  StrictMock<Envoy::ConnectionPool::MockCancellable> pending;
  events();
  expectPool();
  EXPECT_CALL(pool_, newStream(_, _, _)).WillOnce(Return(&pending));
  handle_.connect(destination_);
  EXPECT_CALL(pending, cancel(Envoy::ConnectionPool::CancelPolicy::Default));
  handle_.close();
  time_system_.advanceTimeAndRun(std::chrono::milliseconds(101), *dispatcher_,
                                 Event::Dispatcher::RunType::NonBlock);
  run();
  EXPECT_EQ(0, observed_events_);
  EXPECT_TRUE(failure_->empty());
}

TEST_F(IoHandleTest, ReceiveBackpressureAndHalfClose) {
  connect(handle_);
  accept(handle_);
  Buffer::OwnedImpl incoming("01234567890123456789");
  EXPECT_CALL(stream_, readDisable(true));
  handle_.decodeData(incoming, false);
  Buffer::OwnedImpl output;
  EXPECT_CALL(stream_, readDisable(false));
  EXPECT_EQ(16, handle_.read(output, std::nullopt).return_value_);
  EXPECT_EQ("0123456789012345", output.toString());
  // A half-close must propagate even when DATA writes are blocked.
  handle_.onAboveWriteBufferHighWatermark();
  EXPECT_CALL(encoder_, encodeData(_, true));
  EXPECT_EQ(0, handle_.shutdown(SHUT_WR).return_value_);
  Buffer::OwnedImpl empty;
  EXPECT_CALL(stream_, removeCallbacks(_));
  handle_.decodeData(empty, true);
  EXPECT_EQ(4, handle_.read(output, std::nullopt).return_value_);
  EXPECT_EQ(0, handle_.read(output, std::nullopt).return_value_);
}

TEST_F(IoHandleTest, NestedWriteWatermarks) {
  events();
  connect(handle_);
  accept(handle_);
  run();
  observed_events_ = 0;
  handle_.onAboveWriteBufferHighWatermark();
  handle_.onAboveWriteBufferHighWatermark();
  handle_.onBelowWriteBufferLowWatermark();
  Buffer::OwnedImpl data("hello");
  EXPECT_EQ(Api::IoError::IoErrorCode::Again, handle_.write(data).err_->getErrorCode());
  run();
  EXPECT_EQ(0, observed_events_);
  handle_.onBelowWriteBufferLowWatermark();
  run();
  EXPECT_EQ(Event::FileReadyType::Write, observed_events_);
  expectReset();
}

TEST_F(IoHandleTest, ResetAfterWriteRemainsEstablishedFailure) {
  connect(handle_);
  accept(handle_);
  Buffer::OwnedImpl data("hello");
  EXPECT_CALL(encoder_, encodeData(_, false)).WillOnce(Invoke([](Buffer::Instance& data, bool) {
    data.drain(data.length());
  }));
  EXPECT_EQ(5, handle_.write(data).return_value_);
  handle_.onResetStream(Http::StreamResetReason::RemoteReset, "reset");
  EXPECT_TRUE(handle_.wasConnected());
  EXPECT_EQ(Api::IoError::IoErrorCode::ConnectionReset, handle_.write(data).err_->getErrorCode());
}

TEST_F(IoHandleTest, SourceBindIsRejected) {
  events();
  EXPECT_EQ(-1, handle_.bind(destination_).return_value_);
  run();
  EXPECT_EQ("hbone_source_bind_unsupported", *failure_);
  EXPECT_EQ(Event::FileReadyType::Write, observed_events_);
}

TEST_F(IoHandleTest, ExplicitReadActivationDispatchesConnectionBufferedDataWhileReadDisabled) {
  events();
  connect(handle_);
  accept(handle_);
  run();
  observed_events_ = 0;
  handle_.enableFileEvents(0);
  handle_.activateFileEvents(Event::FileReadyType::Read);
  run();
  EXPECT_EQ(Event::FileReadyType::Read, observed_events_);
  expectReset();
}

TEST_F(IoHandleTest, PoolFailureIsAsynchronousConnectionFailure) {
  events();
  expectPool();
  EXPECT_CALL(pool_, newStream(_, _, _)).WillOnce(Invoke([](auto&, auto& cb, const auto&) {
    cb.onPoolFailure(Http::ConnectionPool::PoolFailureReason::Overflow, "overflow", nullptr);
    return nullptr;
  }));
  handle_.connect(destination_);
  EXPECT_EQ(0, observed_events_);
  run();
  EXPECT_EQ(Event::FileReadyType::Write, observed_events_);
  EXPECT_EQ("hbone_pool_failure: overflow", *failure_);
}

TEST_F(IoHandleTest, TimeoutCancelsPendingPoolAcquisition) {
  StrictMock<Envoy::ConnectionPool::MockCancellable> pending;
  expectPool();
  EXPECT_CALL(pool_, newStream(_, _, _)).WillOnce(Return(&pending));
  handle_.connect(destination_);
  EXPECT_CALL(pending, cancel(Envoy::ConnectionPool::CancelPolicy::Default));
  time_system_.advanceTimeAndRun(std::chrono::milliseconds(101), *dispatcher_,
                                 Event::Dispatcher::RunType::NonBlock);
  run();
  EXPECT_EQ("hbone_connect_timeout", *failure_);
}

TEST_F(IoHandleTest, EndingResponseHeadersDoNotAcceptTunnel) {
  connect(handle_);
  expectReset();
  auto headers = Http::ResponseHeaderMapImpl::create();
  headers->setStatus(200);
  handle_.decodeHeaders(std::move(headers), true);
  EXPECT_FALSE(handle_.wasConnected());
  EXPECT_EQ("hbone_connect_response: 200 end_stream", *failure_);
}

TEST_F(IoHandleTest, StreamResetDuringEncodeDoesNotUseStaleEncoder) {
  connect(handle_);
  accept(handle_);
  Buffer::OwnedImpl data("hello");
  EXPECT_CALL(encoder_, encodeData(_, false)).WillOnce(Invoke([this](Buffer::Instance& data, bool) {
    data.drain(data.length());
    handle_.onResetStream(Http::StreamResetReason::ConnectionTermination, "closed");
  }));
  EXPECT_EQ(5, handle_.write(data).return_value_);
  EXPECT_TRUE(handle_.wasConnected());
  EXPECT_EQ(Api::IoError::IoErrorCode::ConnectionReset, handle_.write(data).err_->getErrorCode());
}

TEST_F(IoHandleTest, ReadVectorsPeekAndRemoteHalfClose) {
  connect(handle_);
  accept(handle_);
  Buffer::OwnedImpl incoming("hello");
  handle_.decodeData(incoming, true);
  char first[3] = {};
  EXPECT_EQ(3, handle_.recv(first, 3, MSG_PEEK).return_value_);
  EXPECT_EQ("hel", std::string(first, 3));
  char second[3] = {};
  Buffer::RawSlice slices[] = {{first, 3}, {second, 3}};
  EXPECT_EQ(5, handle_.readv(6, slices, 2).return_value_);
  EXPECT_EQ("hel", std::string(first, 3));
  EXPECT_EQ("lo", std::string(second, 2));
  EXPECT_EQ(0, handle_.recv(first, 3, 0).return_value_);
  // Read EOF does not prevent writes on the other half of the tunnel.
  EXPECT_CALL(encoder_, encodeData(_, false)).WillOnce(Invoke([](Buffer::Instance& data, bool) {
    EXPECT_EQ("hello", data.toString());
    data.drain(data.length());
  }));
  EXPECT_EQ(5, handle_.send("hello", 5).return_value_);
  EXPECT_CALL(stream_, removeCallbacks(_));
  EXPECT_CALL(encoder_, encodeData(_, true));
  EXPECT_EQ(0, handle_.shutdown(SHUT_WR).return_value_);
}

TEST_F(IoHandleTest, RealClientConnectionWaitsForConnectResponse) {
  auto config = std::make_shared<Config>(
      Config{cluster_manager_, "outer", std::chrono::milliseconds(100), 16});
  auto transport = std::make_unique<PassthroughSocket>(
      std::make_unique<Socket>(std::make_unique<Network::RawBufferSocket>(), config));
  Network::DefaultClientConnectionFactory factory;
  auto connection = factory.createClientConnection(*dispatcher_, destination_, nullptr,
                                                   std::move(transport), nullptr, nullptr);
  auto& handle =
      dynamic_cast<IoHandle&>(dynamic_cast<Network::ConnectionImpl&>(*connection).ioHandle());
  StrictMock<Network::MockConnectionCallbacks> callbacks;
  connection->addConnectionCallbacks(callbacks);
  expectPool();
  EXPECT_CALL(pool_, newStream(_, _, _)).WillOnce(Invoke([this](auto&, auto& cb, const auto&) {
    readyEncoder(cb);
    return nullptr;
  }));
  connection->connect();
  Buffer::OwnedImpl data("POST body");
  connection->write(data, false);
  run(); // No Connected callback and no encodeData allowed yet.
  EXPECT_CALL(callbacks, onEvent(Network::ConnectionEvent::Connected));
  EXPECT_CALL(encoder_, encodeData(_, false)).WillOnce(Invoke([](Buffer::Instance& data, bool) {
    EXPECT_EQ("POST body", data.toString());
    data.drain(data.length());
  }));
  accept(handle);
  run();
  expectReset();
  EXPECT_CALL(callbacks, onEvent(Network::ConnectionEvent::LocalClose));
  connection->close(Network::ConnectionCloseType::NoFlush);
}

TEST_F(IoHandleTest, Ipv6AuthorityAndAddresses) {
  destination_ = Network::Utility::parseInternetAddressAndPortNoThrow("[::1]:8080");
  connect(handle_);
  EXPECT_EQ(AF_INET6, handle_.domain());
  EXPECT_EQ("[::]:0", handle_.localAddress().value()->asString());
  EXPECT_EQ("[::1]:8080", handle_.peerAddress().value()->asString());
  expectReset();
}

TEST_F(IoHandleTest, UnsupportedSocketOptionFailsAsynchronously) {
  events();
  int value = 1;
  EXPECT_EQ(-1, handle_.setOption(SOL_SOCKET, SO_KEEPALIVE, &value, sizeof(value)).return_value_);
  EXPECT_EQ(0, observed_events_);
  run();
  EXPECT_EQ(Event::FileReadyType::Write, observed_events_);
  EXPECT_EQ("hbone_socket_option_unsupported", *failure_);
}

TEST_F(IoHandleTest, OuterPeerCannotSilentlyRedirectToAnotherWorkload) {
  EXPECT_CALL(cluster_manager_, getThreadLocalCluster("outer")).WillOnce(Return(&cluster_));
  EXPECT_CALL(cluster_, chooseHost(_)).WillOnce(Invoke([this](auto*) {
    return Upstream::HostSelectionResponse(host_);
  }));
  EXPECT_CALL(*host_, address())
      .WillRepeatedly(
          Return(Network::Utility::parseInternetAddressAndPortNoThrow("10.0.0.2:15008")));
  handle_.connect(destination_);
  EXPECT_EQ("hbone_peer_must_match_workload_ip", *failure_);
}

} // namespace
} // namespace Hbone
} // namespace TransportSockets
} // namespace Extensions
} // namespace Envoy
