#include "contrib/istio/filters/network/hbone/source/goaway.h"

#include <algorithm>

#include "source/common/common/assert.h"
#include "source/common/network/utility.h"
#include "source/common/upstream/load_balancer_context_base.h"

namespace Envoy {
namespace Extensions {
namespace Istio {
namespace Hbone {

GoAwayRegistry::GoAwayRegistry(TimeSource& time_source, Stats::Scope& scope, uint32_t capacity)
    : time_source_(time_source),
      stats_{ALL_HBONE_GOAWAY_STATS(POOL_COUNTER_PREFIX(scope, "istio_hbone."))},
      capacity_(capacity) {
  ASSERT(capacity > 0);
}

void GoAwayRegistry::record(absl::string_view peer, std::chrono::milliseconds cooldown) {
  absl::MutexLock lock(&mutex_);
  const auto now = time_source_.monotonicTime();
  if (!deadlines_.contains(peer) && deadlines_.size() >= capacity_) {
    absl::erase_if(deadlines_, [now](const auto& entry) { return entry.second <= now; });
    if (deadlines_.size() >= capacity_) {
      auto oldest =
          std::min_element(deadlines_.begin(), deadlines_.end(),
                           [](const auto& a, const auto& b) { return a.second < b.second; });
      deadlines_.erase(oldest);
      stats_.cache_evictions_.inc();
    }
  }
  auto& deadline = deadlines_[std::string(peer)];
  deadline = std::max(deadline, now + cooldown);
  stats_.goaway_received_.inc();
}

bool GoAwayRegistry::contains(absl::string_view peer) {
  absl::MutexLock lock(&mutex_);
  const auto it = deadlines_.find(peer);
  if (it == deadlines_.end()) {
    return false;
  }
  if (it->second <= time_source_.monotonicTime()) {
    deadlines_.erase(it);
    return false;
  }
  return true;
}

std::string hbonePeer(const Upstream::HostDescription& host, uint32_t port_override) {
  if (host.address()->type() != Network::Address::Type::EnvoyInternal) {
    return {};
  }
  const auto metadata = host.metadata();
  if (!metadata) {
    return {};
  }
  const auto ns = metadata->filter_metadata().find("envoy.filters.listener.original_dst");
  if (ns == metadata->filter_metadata().end()) {
    return {};
  }
  const auto& fields = ns->second.fields();
  auto address = fields.find("waypoint");
  if (address == fields.end() || address->second.string_value().empty()) {
    address = fields.find("local");
  }
  if (address == fields.end()) {
    return {};
  }
  auto peer =
      Network::Utility::parseInternetAddressAndPortNoThrow(address->second.string_value(), false);
  if (!peer) {
    return {};
  }
  if (port_override != 0) {
    peer = Network::Utility::getAddressWithPort(*peer, port_override);
  }
  return peer->asString();
}

void GoAwayCallbacks::onGoAway(Http::GoAwayErrorCode error_code) {
  if (!received_) {
    received_ = true;
    registry_->record(peer_, cooldown_);
  }
  // This can close and delete an idle codec. Do not touch members afterward.
  inner_.onGoAway(error_code);
}

namespace {

// Only synchronous child policies are allowed by the configuration factory, so
// this wrapper cannot outlive chooseHost(). Preserve the router's existing
// retry predicates and all other selection context.
class PreferenceContext : public Upstream::LoadBalancerContextBase {
public:
  PreferenceContext(Upstream::LoadBalancerContext* inner, GoAwayRegistry& registry,
                    uint32_t attempts, uint32_t port_override)
      : inner_(inner), registry_(registry), attempts_(attempts), port_override_(port_override) {}

  std::optional<uint64_t> computeHashKey() override {
    return inner_ ? inner_->computeHashKey() : std::nullopt;
  }
  const Router::MetadataMatchCriteria* metadataMatchCriteria() override {
    return inner_ ? inner_->metadataMatchCriteria() : nullptr;
  }
  const Network::Connection* downstreamConnection() const override {
    return inner_ ? inner_->downstreamConnection() : nullptr;
  }
  StreamInfo::StreamInfo* requestStreamInfo() const override {
    return inner_ ? inner_->requestStreamInfo() : nullptr;
  }
  const Http::RequestHeaderMap* downstreamHeaders() const override {
    return inner_ ? inner_->downstreamHeaders() : nullptr;
  }
  const Upstream::HealthyAndDegradedLoad&
  determinePriorityLoad(const Upstream::PrioritySet& priority_set,
                        const Upstream::HealthyAndDegradedLoad& original,
                        const Upstream::RetryPriority::PriorityMappingFunc& mapping) override {
    return inner_ ? inner_->determinePriorityLoad(priority_set, original, mapping) : original;
  }
  bool shouldSelectAnotherHost(const Upstream::Host& host) override {
    const bool previous = inner_ && inner_->shouldSelectAnotherHost(host);
    if (registry_.contains(hbonePeer(host, port_override_))) {
      registry_.stats().hosts_skipped_.inc();
      return true;
    }
    return previous;
  }
  uint32_t hostSelectionRetryCount() const override {
    return std::max(attempts_ - 1, inner_ ? inner_->hostSelectionRetryCount() : 0);
  }
  Network::Socket::OptionsSharedPtr upstreamSocketOptions() const override {
    return inner_ ? inner_->upstreamSocketOptions() : nullptr;
  }
  Network::TransportSocketOptionsConstSharedPtr upstreamTransportSocketOptions() const override {
    return inner_ ? inner_->upstreamTransportSocketOptions() : nullptr;
  }
  OptRef<const OverrideHost> overrideHostToSelect() const override {
    return inner_ ? inner_->overrideHostToSelect() : OptRef<const OverrideHost>{};
  }
  void setHeadersModifier(std::function<void(Http::ResponseHeaderMap&)> modifier) override {
    if (inner_) {
      inner_->setHeadersModifier(std::move(modifier));
    }
  }

private:
  Upstream::LoadBalancerContext* inner_;
  GoAwayRegistry& registry_;
  const uint32_t attempts_;
  const uint32_t port_override_;
};

} // namespace

Upstream::HostSelectionResponse
GoAwayLoadBalancer::chooseHost(Upstream::LoadBalancerContext* context) {
  PreferenceContext preference(context, *registry_, max_selection_attempts_, port_override_);
  auto result = child_->chooseHost(&preference);
  if (result.host && registry_->contains(hbonePeer(*result.host, port_override_))) {
    registry_->stats().fallback_selections_.inc();
  }
  return result;
}

} // namespace Hbone
} // namespace Istio
} // namespace Extensions
} // namespace Envoy
