#pragma once

#include "envoy/event/dispatcher.h"
#include "envoy/network/io_handle.h"
#include "envoy/upstream/cluster_manager.h"

#include "source/common/buffer/watermark_buffer.h"
#include "source/common/http/response_decoder_impl_base.h"
#include "source/common/stream_info/stream_info_impl.h"
#include "source/common/upstream/load_balancer_context_base.h"

namespace Envoy {
namespace Extensions {
namespace TransportSockets {
namespace Hbone {

// One application connection backed by one HTTP/2 CONNECT stream. There is no peer I/O handle,
// internal listener, or application fd. All callbacks execute on the owning worker's dispatcher.
class IoHandle : public Network::IoHandle,
                 public Http::ResponseDecoderImplBase,
                 public Http::StreamCallbacks,
                 public Http::ConnectionPool::Callbacks,
                 public Upstream::LoadBalancerContextBase {
public:
  IoHandle(Event::Dispatcher& dispatcher, Upstream::ClusterManager& cluster_manager,
           std::string cluster, std::chrono::milliseconds timeout, uint32_t buffer_limit,
           std::shared_ptr<std::string> failure_reason);
  ~IoHandle() override;

  os_fd_t fdDoNotUse() const override { return INVALID_SOCKET; }
  void setAbortiveClose() override {} // Closing an unfinished CONNECT stream always resets it.
  Api::IoCallUint64Result close() override;
  bool isOpen() const override { return !closed_; }
  bool wasConnected() const override { return accepted_; }
  Api::IoCallUint64Result readv(uint64_t max_length, Buffer::RawSlice* slices,
                                uint64_t num_slice) override;
  Api::IoCallUint64Result read(Buffer::Instance& buffer,
                               std::optional<uint64_t> max_length) override;
  Api::IoCallUint64Result writev(const Buffer::RawSlice* slices, uint64_t num_slice) override;
  Api::IoCallUint64Result write(Buffer::Instance& buffer) override;
  Api::IoCallUint64Result send(const void* buffer, size_t length) override;
  Api::IoCallUint64Result recv(void* buffer, size_t length, int flags) override;
  Api::IoCallUint64Result sendmsg(const Buffer::RawSlice*, uint64_t, int,
                                  const Network::Address::Ip*,
                                  const Network::Address::Instance&) override;
  Api::IoCallUint64Result recvmsg(Buffer::RawSlice*, uint64_t, uint32_t, const UdpSaveCmsgConfig&,
                                  RecvMsgOutput&) override;
  Api::IoCallUint64Result recvmmsg(RawSliceArrays&, uint32_t, const UdpSaveCmsgConfig&,
                                   RecvMsgOutput&) override;
  bool supportsMmsg() const override { return false; }
  bool supportsUdpGro() const override { return false; }
  Api::SysCallIntResult bind(Network::Address::InstanceConstSharedPtr) override;
  Api::SysCallIntResult listen(int) override { return {-1, ENOTSUP}; }
  Network::IoHandlePtr accept(struct sockaddr*, socklen_t*) override { return nullptr; }
  Api::SysCallIntResult connect(Network::Address::InstanceConstSharedPtr address) override;
  Api::SysCallIntResult setOption(int level, int name, const void*, socklen_t) override;
  Api::SysCallIntResult getOption(int level, int name, void* value, socklen_t* length) override;
  Api::SysCallIntResult ioctl(unsigned long, void*, unsigned long, void*, unsigned long,
                              unsigned long*) override {
    return {-1, ENOTSUP};
  }
  Api::SysCallIntResult setBlocking(bool blocking) override {
    return blocking ? Api::SysCallIntResult{-1, ENOTSUP} : Api::SysCallIntResult{0, 0};
  }
  std::optional<int> domain() override;
  absl::StatusOr<Network::Address::InstanceConstSharedPtr> localAddress() override;
  absl::StatusOr<Network::Address::InstanceConstSharedPtr> peerAddress() override;
  Network::IoHandlePtr duplicate() override { return nullptr; }
  void initializeFileEvent(Event::Dispatcher& dispatcher, Event::FileReadyCb cb,
                           Event::FileTriggerType trigger, uint32_t events) override;
  void activateFileEvents(uint32_t events) override;
  void enableFileEvents(uint32_t events) override;
  void resetFileEvents() override;
  Api::SysCallIntResult shutdown(int how) override;
  std::optional<std::chrono::milliseconds> lastRoundTripTime() override { return std::nullopt; }
  std::optional<uint64_t> congestionWindowInBytes() const override { return std::nullopt; }
  std::optional<std::string> interfaceName() override { return std::nullopt; }

  // The outer cluster sees only this locally constructed destination context. In particular it
  // never receives the downstream connection, source address, or caller transport options.
  StreamInfo::StreamInfo* requestStreamInfo() const override { return &stream_info_; }

  void onPoolReady(Http::RequestEncoder& encoder, Upstream::HostDescriptionConstSharedPtr,
                   StreamInfo::StreamInfo&, std::optional<Http::Protocol> protocol) override;
  void onPoolFailure(Http::ConnectionPool::PoolFailureReason, absl::string_view reason,
                     Upstream::HostDescriptionConstSharedPtr) override;
  void decode1xxHeaders(Http::ResponseHeaderMapPtr&&) override {}
  void decodeHeaders(Http::ResponseHeaderMapPtr&& headers, bool end_stream) override;
  void decodeData(Buffer::Instance& data, bool end_stream) override;
  void decodeTrailers(Http::ResponseTrailerMapPtr&&) override;
  void decodeMetadata(Http::MetadataMapPtr&&) override {}
  void dumpState(std::ostream& os, int indent_level) const override;
  void onResetStream(Http::StreamResetReason reason, absl::string_view details) override;
  void onAboveWriteBufferHighWatermark() override;
  void onBelowWriteBufferLowWatermark() override;

private:
  void fail(absl::string_view reason, int error = ECONNREFUSED);
  void resetStream();
  void updateReadDisable(bool disable);
  void finishResponse();
  uint32_t readyEvents() const;
  void schedule(uint32_t events);
  Api::IoCallUint64Result readError() const;
  Api::IoCallUint64Result writeError() const;

  Event::Dispatcher& dispatcher_;
  Upstream::ClusterManager& cluster_manager_;
  const std::string cluster_;
  const std::chrono::milliseconds timeout_;
  const uint32_t buffer_limit_;
  const std::shared_ptr<std::string> failure_reason_;
  mutable StreamInfo::StreamInfoImpl stream_info_;
  Network::Address::InstanceConstSharedPtr address_;
  Http::ConnectionPool::Cancellable* pending_pool_{nullptr};
  Http::RequestEncoder* encoder_{nullptr};
  Event::TimerPtr timer_;
  Event::SchedulableCallbackPtr event_;
  Event::FileReadyCb callback_;
  Buffer::WatermarkBuffer received_;
  uint32_t enabled_events_{0};
  uint32_t pending_events_{0};
  uint32_t activated_events_{0};
  uint32_t write_blocked_{0};
  int error_{0};
  bool started_{false};
  bool accepted_{false};
  bool closed_{false};
  bool read_end_{false};
  bool write_end_{false};
};

} // namespace Hbone
} // namespace TransportSockets
} // namespace Extensions
} // namespace Envoy
