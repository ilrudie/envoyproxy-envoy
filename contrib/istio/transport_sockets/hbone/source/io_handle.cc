#include "contrib/istio/transport_sockets/hbone/source/io_handle.h"

#include "source/common/http/header_map_impl.h"
#include "source/common/http/utility.h"
#include "source/common/network/filter_state_dst_address.h"
#include "source/common/network/io_socket_error_impl.h"
#include "source/common/network/utility.h"
#include "source/extensions/clusters/original_dst/original_dst_cluster.h"

namespace Envoy {
namespace Extensions {
namespace TransportSockets {
namespace Hbone {
namespace {
Api::IoCallUint64Result ioError(int error) {
  return {0, error == EAGAIN ? Network::IoSocketError::getIoSocketEagainError()
                             : Network::IoSocketError::create(error)};
}
constexpr uint32_t Read = Event::FileReadyType::Read;
constexpr uint32_t Write = Event::FileReadyType::Write;
constexpr uint32_t Closed = Event::FileReadyType::Closed;
} // namespace

IoHandle::IoHandle(Event::Dispatcher& dispatcher, Upstream::ClusterManager& cluster_manager,
                   std::string cluster, std::chrono::milliseconds timeout, uint32_t buffer_limit,
                   std::shared_ptr<std::string> failure_reason)
    : dispatcher_(dispatcher), cluster_manager_(cluster_manager), cluster_(std::move(cluster)),
      timeout_(timeout), buffer_limit_(buffer_limit), failure_reason_(std::move(failure_reason)),
      stream_info_(dispatcher.timeSource(), nullptr, StreamInfo::FilterState::LifeSpan::Connection),
      received_([this] { updateReadDisable(false); }, [this] { updateReadDisable(true); }, [] {}) {
  ASSERT(buffer_limit_ > 0);
  received_.setWatermarks(buffer_limit_);
}

IoHandle::~IoHandle() { close(); }

Api::SysCallIntResult IoHandle::connect(Network::Address::InstanceConstSharedPtr address) {
  if (closed_) {
    return {-1, EBADF};
  }
  if (started_) {
    return {-1, EALREADY};
  }
  started_ = true;
  address_ = std::move(address);
  if (error_ != 0) {
    return {-1, error_};
  }
  if (address_->ip() == nullptr) {
    fail("hbone_requires_ip_destination");
    return {-1, EINVAL};
  }
  timer_ = dispatcher_.createTimer([this] { fail("hbone_connect_timeout", ETIMEDOUT); });
  timer_->enableTimer(timeout_);
  auto* cluster = cluster_manager_.getThreadLocalCluster(cluster_);
  if (cluster == nullptr) {
    fail("hbone_cluster_not_found");
    return {-1, EINPROGRESS};
  }
  stream_info_.filterState()->setData(Upstream::OriginalDstClusterFilterStateKey,
                                      std::make_shared<Network::AddressObject>(address_),
                                      StreamInfo::FilterState::LifeSpan::Connection);
  auto selection = cluster->chooseHost(this);
  if (selection.cancelable != nullptr) {
    selection.cancelable->cancel();
    fail("hbone_async_host_selection_unsupported");
    return {-1, EINPROGRESS};
  }
  if (!selection.host) {
    fail("hbone_no_healthy_host");
    return {-1, EINPROGRESS};
  }
  // Direct workload HBONE only: a misconfigured shared gateway cluster must not silently pool
  // different workloads together. Ports are intentionally excluded from this comparison.
  const auto* peer_ip = selection.host->address()->ip();
  if (peer_ip == nullptr || peer_ip->addressAsString() != address_->ip()->addressAsString()) {
    fail("hbone_peer_must_match_workload_ip");
    return {-1, EINPROGRESS};
  }
  auto pool = cluster->httpConnPool(selection.host, Upstream::ResourcePriority::Default,
                                    Http::Protocol::Http2, this);
  if (!pool) {
    fail("hbone_pool_unavailable");
    return {-1, EINPROGRESS};
  }
  pending_pool_ = pool->newStream(*this, *this, {false, false});
  return {-1, EINPROGRESS};
}

void IoHandle::onPoolReady(Http::RequestEncoder& encoder, Upstream::HostDescriptionConstSharedPtr,
                           StreamInfo::StreamInfo&, std::optional<Http::Protocol> protocol) {
  pending_pool_ = nullptr;
  encoder_ = &encoder;
  encoder_->getStream().addCallbacks(*this);
  if (protocol != Http::Protocol::Http2) {
    fail("hbone_requires_http2");
    return;
  }
  auto headers = Http::RequestHeaderMapImpl::create();
  headers->setMethod(Http::Headers::get().MethodValues.Connect);
  headers->setHost(address_->asString());
  const auto status = encoder_->encodeHeaders(*headers, false);
  if (!status.ok()) {
    fail(absl::StrCat("hbone_encode_headers: ", status.message()));
  }
}

void IoHandle::onPoolFailure(Http::ConnectionPool::PoolFailureReason, absl::string_view reason,
                             Upstream::HostDescriptionConstSharedPtr) {
  pending_pool_ = nullptr;
  fail(absl::StrCat("hbone_pool_failure: ", reason));
}

void IoHandle::decodeHeaders(Http::ResponseHeaderMapPtr&& headers, bool end_stream) {
  const auto status = Http::Utility::getResponseStatus(*headers);
  if (status != 200 || end_stream) {
    fail(absl::StrCat("hbone_connect_response: ", status, end_stream ? " end_stream" : ""));
    return;
  }
  accepted_ = true;
  timer_->disableTimer();
  schedule(Write);
}

void IoHandle::decodeData(Buffer::Instance& data, bool end_stream) {
  received_.move(data);
  if (end_stream) {
    finishResponse();
  }
  schedule(Read | Closed);
}

void IoHandle::decodeTrailers(Http::ResponseTrailerMapPtr&&) {
  finishResponse();
  schedule(Read | Closed);
}

void IoHandle::finishResponse() {
  read_end_ = true;
  if (write_end_ && encoder_ != nullptr) {
    encoder_->getStream().removeCallbacks(*this);
    encoder_ = nullptr;
  }
}

void IoHandle::onResetStream(Http::StreamResetReason, absl::string_view details) {
  // A reset invalidates the encoder. Do not try resetting it again from fail().
  encoder_ = nullptr;
  fail(absl::StrCat("hbone_stream_reset: ", details));
}

void IoHandle::onAboveWriteBufferHighWatermark() { ++write_blocked_; }

void IoHandle::onBelowWriteBufferLowWatermark() {
  ASSERT(write_blocked_ > 0);
  --write_blocked_;
  schedule(Write);
}

void IoHandle::updateReadDisable(bool disable) {
  if (encoder_ != nullptr && !read_end_) {
    encoder_->getStream().readDisable(disable);
  }
}

void IoHandle::resetStream() {
  if (pending_pool_ != nullptr) {
    auto* pending = std::exchange(pending_pool_, nullptr);
    pending->cancel(Envoy::ConnectionPool::CancelPolicy::Default);
  }
  if (encoder_ != nullptr) {
    auto* encoder = std::exchange(encoder_, nullptr);
    encoder->getStream().removeCallbacks(*this);
    encoder->getStream().resetStream(Http::StreamResetReason::LocalReset);
  }
}

void IoHandle::fail(absl::string_view reason, int error) {
  if (closed_ || error_ != 0) {
    return;
  }
  // Once accepted, errors retain ordinary established-connection semantics.
  error_ = accepted_ ? ECONNRESET : error;
  *failure_reason_ = std::string(reason);
  if (timer_) {
    timer_->disableTimer();
  }
  resetStream();
  schedule(Read | Write | Closed);
}

Api::IoCallUint64Result IoHandle::close() {
  if (!closed_) {
    closed_ = true;
    resetFileEvents();
    timer_.reset();
    resetStream();
    received_.drain(received_.length());
  }
  return {0, Api::IoError::none()};
}

uint32_t IoHandle::readyEvents() const {
  if (closed_) {
    return 0;
  }
  if (error_ != 0) {
    return Write | (accepted_ ? Read | Closed : 0);
  }
  if (!accepted_) {
    return 0;
  }
  return (write_blocked_ == 0 ? Write : 0) | (received_.length() != 0 || read_end_ ? Read : 0) |
         (read_end_ ? Closed : 0);
}

void IoHandle::schedule(uint32_t events) {
  pending_events_ |= events & enabled_events_ & readyEvents();
  if (event_ && (pending_events_ | activated_events_) != 0) {
    event_->scheduleCallbackNextIteration();
  }
}

void IoHandle::initializeFileEvent(Event::Dispatcher& dispatcher, Event::FileReadyCb cb,
                                   Event::FileTriggerType, uint32_t events) {
  ASSERT(&dispatcher == &dispatcher_);
  ASSERT(!event_);
  callback_ = std::move(cb);
  event_ = dispatcher_.createSchedulableCallback([this] {
    uint32_t events =
        (std::exchange(pending_events_, 0) & enabled_events_) | std::exchange(activated_events_, 0);
    if ((readyEvents() & Write) == 0) {
      events &= ~Write;
    }
    if ((events & Read) != 0) {
      events &= ~Closed;
    }
    if (events != 0) {
      // The connection may close and destroy the stored callback from inside this invocation.
      auto callback = callback_;
      THROW_IF_NOT_OK(callback(events));
    }
  });
  enableFileEvents(events);
}

void IoHandle::activateFileEvents(uint32_t events) {
  // Explicit reads also dispatch data already buffered by ConnectionImpl or an inner TLS socket.
  if (accepted_ && !closed_) {
    activated_events_ |= events & Read;
  }
  activated_events_ |= events & readyEvents() & Write;
  schedule(events);
}

void IoHandle::enableFileEvents(uint32_t events) {
  enabled_events_ = events;
  pending_events_ = activated_events_ = 0;
  schedule(events);
}

void IoHandle::resetFileEvents() {
  event_.reset();
  callback_ = nullptr;
  enabled_events_ = pending_events_ = activated_events_ = 0;
}

Api::IoCallUint64Result IoHandle::readError() const {
  if (closed_) {
    return ioError(EBADF);
  }
  if (error_ != 0) {
    return ioError(error_);
  }
  if (!accepted_ || (received_.length() == 0 && !read_end_)) {
    return ioError(EAGAIN);
  }
  return {0, Api::IoError::none()};
}

Api::IoCallUint64Result IoHandle::writeError() const {
  if (closed_) {
    return ioError(EBADF);
  }
  if (error_ != 0) {
    return ioError(error_);
  }
  if (write_end_) {
    return ioError(EPIPE);
  }
  if (!accepted_ || write_blocked_ != 0) {
    return ioError(EAGAIN);
  }
  return {0, Api::IoError::none()};
}

Api::IoCallUint64Result IoHandle::read(Buffer::Instance& buffer,
                                       std::optional<uint64_t> max_length) {
  auto result = readError();
  if (!result.ok()) {
    return result;
  }
  const uint64_t length = std::min(received_.length(), max_length.value_or(buffer_limit_));
  buffer.move(received_, length);
  return {length, Api::IoError::none()};
}

Api::IoCallUint64Result IoHandle::readv(uint64_t max_length, Buffer::RawSlice* slices,
                                        uint64_t num_slice) {
  auto result = readError();
  if (!result.ok()) {
    return result;
  }
  uint64_t total = 0;
  for (uint64_t i = 0; i < num_slice && total < max_length; ++i) {
    const uint64_t length =
        std::min({max_length - total, uint64_t(slices[i].len_), received_.length()});
    received_.copyOut(0, length, slices[i].mem_);
    received_.drain(length);
    total += length;
  }
  return {total, Api::IoError::none()};
}

Api::IoCallUint64Result IoHandle::recv(void* buffer, size_t length, int flags) {
  auto result = readError();
  if (!result.ok()) {
    return result;
  }
  if (flags != 0 && flags != MSG_PEEK) {
    return ioError(ENOTSUP);
  }
  const uint64_t size = std::min(uint64_t(length), received_.length());
  received_.copyOut(0, size, buffer);
  if (flags == 0) {
    received_.drain(size);
  }
  return {size, Api::IoError::none()};
}

Api::IoCallUint64Result IoHandle::write(Buffer::Instance& buffer) {
  auto result = writeError();
  if (!result.ok()) {
    return result;
  }
  const uint64_t length = std::min(buffer.length(), uint64_t(buffer_limit_));
  if (length == 0) {
    return {0, Api::IoError::none()};
  }
  Buffer::OwnedImpl data;
  data.move(buffer, length);
  encoder_->encodeData(data, false);
  // encodeData may synchronously reset the stream. Bytes handed to the codec count as written.
  return {length, Api::IoError::none()};
}

Api::IoCallUint64Result IoHandle::writev(const Buffer::RawSlice* slices, uint64_t num_slice) {
  auto result = writeError();
  if (!result.ok()) {
    return result;
  }
  Buffer::OwnedImpl data;
  for (uint64_t i = 0; i < num_slice && data.length() < buffer_limit_; ++i) {
    data.add(slices[i].mem_, std::min(uint64_t(slices[i].len_), buffer_limit_ - data.length()));
  }
  return write(data);
}

Api::IoCallUint64Result IoHandle::send(const void* buffer, size_t length) {
  Buffer::RawSlice slice{const_cast<void*>(buffer), length};
  return writev(&slice, 1);
}

Api::SysCallIntResult IoHandle::shutdown(int how) {
  if (how != SHUT_WR) {
    return {-1, ENOTSUP};
  }
  if (write_end_) {
    return {0, 0};
  }
  if (closed_ || error_ != 0 || !accepted_) {
    return {-1, closed_ ? EBADF : (error_ != 0 ? error_ : EAGAIN)};
  }
  write_end_ = true;
  auto* encoder = encoder_;
  if (read_end_) {
    encoder->getStream().removeCallbacks(*this);
    encoder_ = nullptr;
  }
  Buffer::OwnedImpl empty;
  encoder->encodeData(empty, true);
  return {0, 0};
}

Api::SysCallIntResult IoHandle::bind(Network::Address::InstanceConstSharedPtr) {
  fail("hbone_source_bind_unsupported", ENOTSUP);
  return {-1, ENOTSUP};
}

Api::SysCallIntResult IoHandle::setOption(int level, int name, const void*, socklen_t) {
  // ConnectionImpl unconditionally sets TCP_NODELAY. A stream has no TCP send delay to disable.
  if (level == IPPROTO_TCP && name == TCP_NODELAY) {
    return {0, 0};
  }
  fail("hbone_socket_option_unsupported", ENOTSUP);
  return {-1, ENOTSUP};
}

Api::SysCallIntResult IoHandle::getOption(int level, int name, void* value, socklen_t* length) {
  if (level == SOL_SOCKET && name == SO_ERROR && *length >= sizeof(int)) {
    *static_cast<int*>(value) = error_;
    *length = sizeof(int);
    return {0, 0};
  }
  return {-1, ENOTSUP};
}

std::optional<int> IoHandle::domain() {
  if (!address_ || !address_->ip()) {
    return std::nullopt;
  }
  return address_->ip()->version() == Network::Address::IpVersion::v4 ? AF_INET : AF_INET6;
}

absl::StatusOr<Network::Address::InstanceConstSharedPtr> IoHandle::localAddress() {
  // The logical stream has no source socket. Never report or bind an original caller address.
  return domain() == AF_INET6 ? Network::Utility::getIpv6AnyAddress()
                              : Network::Utility::getIpv4AnyAddress();
}

absl::StatusOr<Network::Address::InstanceConstSharedPtr> IoHandle::peerAddress() {
  if (!address_) {
    return absl::FailedPreconditionError("HBONE connect has not started");
  }
  return address_;
}

Api::IoCallUint64Result IoHandle::sendmsg(const Buffer::RawSlice*, uint64_t, int,
                                          const Network::Address::Ip*,
                                          const Network::Address::Instance&) {
  return ioError(ENOTSUP);
}
Api::IoCallUint64Result IoHandle::recvmsg(Buffer::RawSlice*, uint64_t, uint32_t,
                                          const UdpSaveCmsgConfig&, RecvMsgOutput&) {
  return ioError(ENOTSUP);
}
Api::IoCallUint64Result IoHandle::recvmmsg(RawSliceArrays&, uint32_t, const UdpSaveCmsgConfig&,
                                           RecvMsgOutput&) {
  return ioError(ENOTSUP);
}

void IoHandle::dumpState(std::ostream& os, int) const {
  os << "HBONE accepted=" << accepted_ << " closed=" << closed_ << " error=" << error_;
}

} // namespace Hbone
} // namespace TransportSockets
} // namespace Extensions
} // namespace Envoy
