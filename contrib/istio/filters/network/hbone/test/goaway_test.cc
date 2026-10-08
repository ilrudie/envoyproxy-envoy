#include <thread>

#include "envoy/extensions/load_balancing_policies/least_request/v3/least_request.pb.h"
#include "envoy/extensions/load_balancing_policies/random/v3/random.pb.h"
#include "envoy/extensions/load_balancing_policies/round_robin/v3/round_robin.pb.h"

#include "source/common/network/address_impl.h"
#include "source/common/stats/isolated_store_impl.h"
#include "source/common/upstream/load_balancer_context_base.h"
#include "source/common/upstream/upstream_impl.h"

#include "test/mocks/http/mocks.h"
#include "test/mocks/server/server_factory_context.h"
#include "test/mocks/upstream/cluster_info.h"
#include "test/mocks/upstream/host.h"
#include "test/mocks/upstream/load_balancer.h"
#include "test/mocks/upstream/load_balancer_context.h"
#include "test/mocks/upstream/priority_set.h"
#include "test/test_common/simulated_time_system.h"
#include "test/test_common/utility.h"

#include "contrib/istio/filters/network/hbone/source/goaway_config.h"

namespace Envoy {
namespace Extensions {
namespace Istio {
namespace Hbone {
namespace {

using testing::_;
using testing::Return;
using testing::ReturnRef;
using testing::StrictMock;
using namespace std::chrono_literals;

class GoAwayTest : public testing::Test {
protected:
  std::shared_ptr<Upstream::MockHost> host(const std::string& local,
                                           const std::string& waypoint = "", bool internal = true) {
    auto host = std::make_shared<StrictMock<Upstream::MockHost>>();
    Network::Address::InstanceConstSharedPtr address =
        internal ? Network::Address::InstanceConstSharedPtr{std::make_shared<
                       Network::Address::EnvoyInternalInstance>("shim", local)}
                 : Network::Address::InstanceConstSharedPtr{
                       std::make_shared<Network::Address::Ipv4Instance>("10.0.0.1", 8080)};
    EXPECT_CALL(*host, address()).WillRepeatedly(Return(address));
    auto metadata = std::make_shared<envoy::config::core::v3::Metadata>();
    auto& fields = *(*metadata->mutable_filter_metadata())["envoy.filters.listener.original_dst"]
                        .mutable_fields();
    fields["local"].set_string_value(local);
    if (!waypoint.empty()) {
      fields["waypoint"].set_string_value(waypoint);
    }
    if (internal) {
      EXPECT_CALL(*host, metadata()).WillRepeatedly(Return(metadata));
    }
    return host;
  }

  Event::SimulatedTimeSystem time_;
  Stats::IsolatedStoreImpl store_;
  GoAwayRegistrySharedPtr registry_{std::make_shared<GoAwayRegistry>(time_, *store_.rootScope())};
};

TEST_F(GoAwayTest, SharedHintExpiresWithoutEndpointRemoval) {
  // A GOAWAY on one worker is immediately visible to selection on another.
  std::thread worker([this]() { registry_->record("10.0.0.1:15008", 30s); });
  worker.join();
  EXPECT_TRUE(registry_->contains("10.0.0.1:15008"));
  EXPECT_FALSE(registry_->contains("10.0.0.2:15008"));
  EXPECT_FALSE(registry_->contains("10.0.0.1:8080"));
  time_.advanceTimeWait(30s);
  EXPECT_FALSE(registry_->contains("10.0.0.1:15008"));
}

TEST_F(GoAwayTest, RepeatedGoawayOnSameConnectionDoesNotExtendCooldown) {
  StrictMock<Http::MockConnectionCallbacks> inner;
  GoAwayCallbacks callbacks(inner, registry_, "10.0.0.1:15008", 30s);
  EXPECT_CALL(inner, onGoAway(Http::GoAwayErrorCode::NoError)).Times(2);
  callbacks.onGoAway(Http::GoAwayErrorCode::NoError);
  time_.advanceTimeWait(20s);
  callbacks.onGoAway(Http::GoAwayErrorCode::NoError);
  time_.advanceTimeWait(10s);
  EXPECT_FALSE(registry_->contains("10.0.0.1:15008"));
  EXPECT_EQ(1, registry_->stats().goaway_received_.value());
}

TEST_F(GoAwayTest, AnotherConnectionCanRefreshTheHint) {
  registry_->record("10.0.0.1:15008", 30s);
  time_.advanceTimeWait(20s);
  registry_->record("10.0.0.1:15008", 30s);
  time_.advanceTimeWait(10s);
  EXPECT_TRUE(registry_->contains("10.0.0.1:15008"));
  time_.advanceTimeWait(20s);
  EXPECT_FALSE(registry_->contains("10.0.0.1:15008"));
}

TEST_F(GoAwayTest, BoundedCacheExpiresAndEvictsOldestHint) {
  GoAwayRegistry registry(time_, *store_.rootScope(), 2);
  registry.record("a", 10s);
  registry.record("b", 20s);
  registry.record("c", 30s);
  EXPECT_FALSE(registry.contains("a"));
  EXPECT_TRUE(registry.contains("b"));
  EXPECT_TRUE(registry.contains("c"));
  EXPECT_EQ(1, registry.stats().cache_evictions_.value());
  time_.advanceTimeWait(20s);
  registry.record("d", 30s);
  EXPECT_FALSE(registry.contains("b"));
  EXPECT_TRUE(registry.contains("c"));
  EXPECT_TRUE(registry.contains("d"));
  EXPECT_EQ(1, registry.stats().cache_evictions_.value());
}

TEST_F(GoAwayTest, ResolvesDirectAndWaypointDestinationsWithPortOverride) {
  auto direct = host("10.0.0.1:8080");
  EXPECT_EQ("10.0.0.1:15008", hbonePeer(*direct, 15008));
  EXPECT_EQ("10.0.0.1:8080", hbonePeer(*direct, 0));
  auto waypoint = host("10.0.0.1:8080", "[2001:db8::1]:15009");
  EXPECT_EQ("[2001:db8::1]:15008", hbonePeer(*waypoint, 15008));
  auto invalid = host("invalid");
  EXPECT_TRUE(hbonePeer(*invalid, 15008).empty());
  auto invalid_waypoint = host("10.0.0.1:8080", "invalid");
  EXPECT_TRUE(hbonePeer(*invalid_waypoint, 15008).empty());
  auto plain = host("10.0.0.1:8080", "", false);
  EXPECT_TRUE(hbonePeer(*plain, 15008).empty());
}

TEST_F(GoAwayTest, MissingMetadataDoesNotAffectSelection) {
  auto candidate = std::make_shared<StrictMock<Upstream::MockHost>>();
  auto address = std::make_shared<Network::Address::EnvoyInternalInstance>("shim");
  EXPECT_CALL(*candidate, address()).WillRepeatedly(Return(address));
  EXPECT_CALL(*candidate, metadata()).WillOnce(Return(nullptr));
  EXPECT_TRUE(hbonePeer(*candidate, 15008).empty());
  auto metadata = std::make_shared<envoy::config::core::v3::Metadata>();
  EXPECT_CALL(*candidate, metadata()).WillRepeatedly(Return(metadata));
  EXPECT_TRUE(hbonePeer(*candidate, 15008).empty());
  (*metadata->mutable_filter_metadata())["envoy.filters.listener.original_dst"];
  EXPECT_TRUE(hbonePeer(*candidate, 15008).empty());
}

TEST_F(GoAwayTest, PreferenceAppliesBeforeFirstAttemptAndRetainsRetryPredicate) {
  auto draining = host("10.0.0.1:8080");
  auto previous = host("10.0.0.2:8080");
  auto available = host("10.0.0.3:8080");
  registry_->record("10.0.0.1:15008", 30s);
  auto child = std::make_unique<StrictMock<Upstream::MockLoadBalancer>>();
  auto* child_ptr = child.get();
  GoAwayLoadBalancer lb(std::move(child), registry_, 16, 15008);
  EXPECT_CALL(*child_ptr, chooseHost(_)).WillOnce([&](Upstream::LoadBalancerContext* context) {
    EXPECT_EQ(15, context->hostSelectionRetryCount());
    EXPECT_TRUE(context->shouldSelectAnotherHost(*draining));
    EXPECT_FALSE(context->shouldSelectAnotherHost(*available));
    return Upstream::HostSelectionResponse(available);
  });
  EXPECT_EQ(available, lb.chooseHost(nullptr).host);

  StrictMock<Upstream::MockLoadBalancerContext> request;
  EXPECT_CALL(request, hostSelectionRetryCount()).WillOnce(Return(20));
  EXPECT_CALL(request, shouldSelectAnotherHost(testing::Ref(*previous))).WillOnce(Return(true));
  EXPECT_CALL(request, shouldSelectAnotherHost(testing::Ref(*available))).WillOnce(Return(false));
  EXPECT_CALL(*child_ptr, chooseHost(_)).WillOnce([&](Upstream::LoadBalancerContext* context) {
    EXPECT_EQ(20, context->hostSelectionRetryCount());
    EXPECT_TRUE(context->shouldSelectAnotherHost(*previous));
    EXPECT_FALSE(context->shouldSelectAnotherHost(*available));
    return Upstream::HostSelectionResponse(available);
  });
  EXPECT_EQ(available, lb.chooseHost(&request).host);
  EXPECT_EQ(1, registry_->stats().hosts_skipped_.value());
  EXPECT_EQ(0, registry_->stats().fallback_selections_.value());
}

TEST_F(GoAwayTest, AllDrainingFallsBackWithoutMarkingHostsUnhealthy) {
  auto draining = host("10.0.0.1:8080");
  registry_->record("10.0.0.1:15008", 30s);
  auto child = std::make_unique<StrictMock<Upstream::MockLoadBalancer>>();
  EXPECT_CALL(*child, chooseHost(_)).WillOnce([&](Upstream::LoadBalancerContext* context) {
    EXPECT_EQ(1, context->hostSelectionRetryCount());
    EXPECT_TRUE(context->shouldSelectAnotherHost(*draining));
    return Upstream::HostSelectionResponse(draining);
  });
  GoAwayLoadBalancer lb(std::move(child), registry_, 2, 15008);
  EXPECT_EQ(draining, lb.chooseHost(nullptr).host);
  EXPECT_EQ(1, registry_->stats().fallback_selections_.value());
  EXPECT_EQ(nullptr, lb.peekAnotherHost(nullptr));
}

TEST_F(GoAwayTest, NoHealthyHostsPreservesChildFailure) {
  auto child = std::make_unique<StrictMock<Upstream::MockLoadBalancer>>();
  EXPECT_CALL(*child, chooseHost(_)).WillOnce(Return(Upstream::HostSelectionResponse{nullptr}));
  GoAwayLoadBalancer lb(std::move(child), registry_, 16, 15008);
  EXPECT_EQ(nullptr, lb.chooseHost(nullptr).host);
  EXPECT_EQ(0, registry_->stats().fallback_selections_.value());
}

TEST_F(GoAwayTest, CodecPreservesSettingsAndMetadataCallbacks) {
  StrictMock<Http::MockConnectionCallbacks> inner;
  GoAwayCallbacks callbacks(inner, registry_, "10.0.0.1:15008", 30s);
  class Settings : public Http::ReceivedSettings {
  public:
    const std::optional<uint32_t>& maxConcurrentStreams() const override { return max_streams_; }
    const std::optional<uint32_t> max_streams_{100};
  } settings;
  EXPECT_CALL(inner, onSettings(testing::Ref(settings)));
  callbacks.onSettings(settings);
  EXPECT_CALL(inner, onMetadata(_));
  callbacks.onMetadata(std::make_unique<Http::MetadataMap>());
}

TEST_F(GoAwayTest, NativePoliciesKeepWorkingAcrossExpiryAndHostUpdates) {
  testing::NiceMock<Server::Configuration::MockServerFactoryContext> context;
  auto info = std::make_shared<testing::NiceMock<Upstream::MockClusterInfo>>();
  for (const auto& name : {"round_robin", "least_request", "random"}) {
    SCOPED_TRACE(name);
    testing::NiceMock<Upstream::MockPrioritySet> priorities;
    auto& hosts = *priorities.getMockHostSet(0);
    auto make_host = [&](const std::string& peer) {
      auto metadata = std::make_shared<envoy::config::core::v3::Metadata>();
      (*(*metadata->mutable_filter_metadata())["envoy.filters.listener.original_dst"]
            .mutable_fields())["local"]
          .set_string_value(peer);
      return Upstream::HostSharedPtr{
          Upstream::HostImpl::create(
              info, "", std::make_shared<Network::Address::EnvoyInternalInstance>("shim", peer),
              metadata, nullptr, 1, std::make_shared<envoy::config::core::v3::Locality>(),
              envoy::config::endpoint::v3::Endpoint::HealthCheckConfig::default_instance(), 0,
              envoy::config::core::v3::UNKNOWN)
              .value()};
    };
    auto draining = make_host("10.0.0.1:8080");
    hosts.hosts_ = hosts.healthy_hosts_ = {draining};
    GoAwayLbProto proto;
    proto.set_max_selection_attempts(3);
    proto.set_upstream_port_override(15008);
    auto* child = proto.mutable_child_policy()->add_policies()->mutable_typed_extension_config();
    child->set_name(absl::StrCat("envoy.load_balancing_policies.", name));
    if (std::string(name) == "round_robin") {
      ASSERT_TRUE(child->mutable_typed_config()->PackFrom(
          envoy::extensions::load_balancing_policies::round_robin::v3::RoundRobin{}));
    } else if (std::string(name) == "least_request") {
      ASSERT_TRUE(child->mutable_typed_config()->PackFrom(
          envoy::extensions::load_balancing_policies::least_request::v3::LeastRequest{}));
    } else {
      ASSERT_TRUE(child->mutable_typed_config()->PackFrom(
          envoy::extensions::load_balancing_policies::random::v3::Random{}));
    }
    GoAwayLbFactory factory;
    auto config = factory.loadConfig(context, proto).value();
    auto registry =
        context.singletonManager().getTyped<GoAwayRegistry>("istio_hbone_goaway_singleton");
    ASSERT_NE(nullptr, registry);
    auto thread_aware = factory.create(*config, *info, priorities, context.runtime_loader_,
                                       context.api_.random_, time_);
    ASSERT_TRUE(thread_aware->initialize().ok());
    auto worker_factory = thread_aware->factory();
    // A worker factory may outlive the main-thread configuration and LB.
    config.reset();
    thread_aware.reset();
    auto lb = worker_factory->create({priorities, nullptr});
    registry->record("10.0.0.1:15008", 30s);
    const auto skipped = registry->stats().hosts_skipped_.value();
    const auto fallback = registry->stats().fallback_selections_.value();
    EXPECT_EQ(draining, lb->chooseHost(nullptr).host);
    EXPECT_EQ(skipped + 3, registry->stats().hosts_skipped_.value());
    EXPECT_EQ(fallback + 1, registry->stats().fallback_selections_.value());
    time_.advanceTimeWait(30s);
    EXPECT_EQ(draining, lb->chooseHost(nullptr).host);
    EXPECT_EQ(skipped + 3, registry->stats().hosts_skipped_.value());
    EXPECT_EQ(fallback + 1, registry->stats().fallback_selections_.value());
    auto replacement = make_host("10.0.0.2:8080");
    hosts.hosts_ = hosts.healthy_hosts_ = {replacement};
    priorities.runUpdateCallbacks(0, {replacement}, {draining});
    EXPECT_EQ(replacement, lb->chooseHost(nullptr).host);
  }
}

TEST_F(GoAwayTest, RejectsUnsupportedChildPolicy) {
  // Factory context services are scaffolding; this test focuses on config validation.
  testing::NiceMock<Server::Configuration::MockServerFactoryContext> context;
  GoAwayLbFactory factory;
  GoAwayLbProto config;
  auto* child = config.mutable_child_policy()->add_policies()->mutable_typed_extension_config();
  child->set_name("envoy.load_balancing_policies.istio_hbone");
  ASSERT_TRUE(child->mutable_typed_config()->PackFrom(GoAwayLbProto{}));
  EXPECT_FALSE(factory.loadConfig(context, config).ok());
}

} // namespace
} // namespace Hbone
} // namespace Istio
} // namespace Extensions
} // namespace Envoy
