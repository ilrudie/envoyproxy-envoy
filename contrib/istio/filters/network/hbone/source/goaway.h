#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "envoy/common/time.h"
#include "envoy/http/codec.h"
#include "envoy/singleton/instance.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/upstream/load_balancer.h"

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

namespace Envoy {
namespace Extensions {
namespace Istio {
namespace Hbone {

#define ALL_HBONE_GOAWAY_STATS(COUNTER)                                                            \
  COUNTER(goaway_received)                                                                         \
  COUNTER(hosts_skipped)                                                                           \
  COUNTER(fallback_selections)                                                                     \
  COUNTER(cache_evictions)

struct GoAwayStats {
  ALL_HBONE_GOAWAY_STATS(GENERATE_COUNTER_STRUCT)
};

// A bounded process-wide cache. Workers share hints immediately; entries contain
// no host or connection pointers and cannot extend either object's lifetime.
class GoAwayRegistry : public Singleton::Instance {
public:
  GoAwayRegistry(TimeSource& time_source, Stats::Scope& scope, uint32_t capacity = 4096);
  void record(absl::string_view peer, std::chrono::milliseconds cooldown);
  bool contains(absl::string_view peer);
  GoAwayStats& stats() { return stats_; }

private:
  TimeSource& time_source_;
  GoAwayStats stats_;
  const uint32_t capacity_;
  absl::Mutex mutex_;
  absl::flat_hash_map<std::string, MonotonicTime> deadlines_ ABSL_GUARDED_BY(mutex_);
};

using GoAwayRegistrySharedPtr = std::shared_ptr<GoAwayRegistry>;

// Follow Istio's original-destination metadata, including its waypoint override.
// Only internal endpoints participate; an ordinary IP endpoint is left alone.
std::string hbonePeer(const Upstream::HostDescription& host, uint32_t port_override);

class GoAwayCallbacks : public Http::ConnectionCallbacks {
public:
  GoAwayCallbacks(Http::ConnectionCallbacks& inner, GoAwayRegistrySharedPtr registry,
                  std::string peer, std::chrono::milliseconds cooldown)
      : inner_(inner), registry_(std::move(registry)), peer_(std::move(peer)), cooldown_(cooldown) {
  }

  void onGoAway(Http::GoAwayErrorCode error_code) override;
  void onSettings(Http::ReceivedSettings& settings) override { inner_.onSettings(settings); }
  void onMaxStreamsChanged(uint32_t num_streams) override {
    inner_.onMaxStreamsChanged(num_streams);
  }
  void onMetadata(Http::MetadataMapPtr&& metadata) override {
    inner_.onMetadata(std::move(metadata));
  }

private:
  Http::ConnectionCallbacks& inner_;
  const GoAwayRegistrySharedPtr registry_;
  const std::string peer_;
  const std::chrono::milliseconds cooldown_;
  bool received_{false};
};

// Preserve the stock codec and its pool callbacks, while observing received GOAWAY.
class GoAwayClientConnection : public Http::ClientConnection {
public:
  GoAwayClientConnection(std::unique_ptr<GoAwayCallbacks> callbacks,
                         Http::ClientConnectionPtr codec)
      : callbacks_(std::move(callbacks)), codec_(std::move(codec)) {}

  Http::RequestEncoder& newStream(Http::ResponseDecoder& decoder) override {
    return codec_->newStream(decoder);
  }
  Http::Status dispatch(Buffer::Instance& data) override { return codec_->dispatch(data); }
  void goAway() override { codec_->goAway(); }
  Http::Protocol protocol() override { return codec_->protocol(); }
  void shutdownNotice() override { codec_->shutdownNotice(); }
  bool wantsToWrite() override { return codec_->wantsToWrite(); }
  void encodeMetadata(const Http::MetadataMapVector& metadata) override {
    codec_->encodeMetadata(metadata);
  }
  void onUnderlyingConnectionAboveWriteBufferHighWatermark() override {
    codec_->onUnderlyingConnectionAboveWriteBufferHighWatermark();
  }
  void onUnderlyingConnectionBelowWriteBufferLowWatermark() override {
    codec_->onUnderlyingConnectionBelowWriteBufferLowWatermark();
  }

private:
  // The callbacks must outlive the codec that references them.
  const std::unique_ptr<GoAwayCallbacks> callbacks_;
  const Http::ClientConnectionPtr codec_;
};

class GoAwayLoadBalancer : public Upstream::LoadBalancer {
public:
  GoAwayLoadBalancer(Upstream::LoadBalancerPtr child, GoAwayRegistrySharedPtr registry,
                     uint32_t max_selection_attempts, uint32_t port_override)
      : child_(std::move(child)), registry_(std::move(registry)),
        max_selection_attempts_(max_selection_attempts), port_override_(port_override) {}

  Upstream::HostSelectionResponse chooseHost(Upstream::LoadBalancerContext* context) override;
  Upstream::HostConstSharedPtr peekAnotherHost(Upstream::LoadBalancerContext*) override {
    // Avoid preconnecting to candidates that have not passed the preference check.
    return nullptr;
  }
  OptRef<Http::ConnectionPool::ConnectionLifetimeCallbacks> lifetimeCallbacks() override {
    return child_->lifetimeCallbacks();
  }
  std::optional<Upstream::SelectedPoolAndConnection>
  selectExistingConnection(Upstream::LoadBalancerContext* context, const Upstream::Host& host,
                           std::vector<uint8_t>& hash_key) override {
    return child_->selectExistingConnection(context, host, hash_key);
  }

private:
  const Upstream::LoadBalancerPtr child_;
  const GoAwayRegistrySharedPtr registry_;
  const uint32_t max_selection_attempts_;
  const uint32_t port_override_;
};

} // namespace Hbone
} // namespace Istio
} // namespace Extensions
} // namespace Envoy
