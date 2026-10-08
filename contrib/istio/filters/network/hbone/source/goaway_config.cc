#include "contrib/istio/filters/network/hbone/source/goaway_config.h"

#include "envoy/registry/registry.h"
#include "envoy/singleton/manager.h"

#include "source/common/config/utility.h"
#include "source/common/http/http2/codec_impl.h"
#include "source/common/protobuf/utility.h"

namespace Envoy {
namespace Extensions {
namespace Istio {
namespace Hbone {

SINGLETON_MANAGER_REGISTRATION(istio_hbone_goaway);

namespace {

GoAwayRegistrySharedPtr registryFor(Server::Configuration::ServerFactoryContext& context) {
  return context.singletonManager().getTyped<GoAwayRegistry>(
      SINGLETON_MANAGER_REGISTERED_NAME(istio_hbone_goaway), [&context]() {
        return std::make_shared<GoAwayRegistry>(context.timeSource(), context.scope());
      });
}

struct GoAwayLbConfig : public Upstream::LoadBalancerConfig {
  GoAwayLbConfig(Upstream::TypedLoadBalancerFactory& factory,
                 Upstream::LoadBalancerConfigPtr child_config, GoAwayRegistrySharedPtr registry,
                 uint32_t attempts, uint32_t port_override)
      : child_factory_(factory), child_config_(std::move(child_config)),
        registry_(std::move(registry)), attempts_(attempts), port_override_(port_override) {}

  Upstream::TypedLoadBalancerFactory& child_factory_;
  std::shared_ptr<const Upstream::LoadBalancerConfig> child_config_;
  GoAwayRegistrySharedPtr registry_;
  uint32_t attempts_;
  uint32_t port_override_;
};

class WorkerFactory : public Upstream::LoadBalancerFactory {
public:
  WorkerFactory(const GoAwayLbConfig& config, Upstream::LoadBalancerFactorySharedPtr child)
      : config_(config), child_(std::move(child)) {}
  Upstream::LoadBalancerPtr create(Upstream::LoadBalancerParams params) override {
    return std::make_unique<GoAwayLoadBalancer>(child_->create(params), config_.registry_,
                                                config_.attempts_, config_.port_override_);
  }
  bool recreateOnHostChangeDeprecated() const override { return false; }

private:
  // Keep the child configuration alive even after the main-thread LB is removed.
  const GoAwayLbConfig config_;
  const Upstream::LoadBalancerFactorySharedPtr child_;
};

class ThreadAwareLb : public Upstream::ThreadAwareLoadBalancer {
public:
  ThreadAwareLb(const GoAwayLbConfig& config, const Upstream::ClusterInfo& cluster_info,
                const Upstream::PrioritySet& priority_set, Runtime::Loader& runtime,
                Random::RandomGenerator& random, TimeSource& time_source)
      : child_(config.child_factory_.create(*config.child_config_, cluster_info, priority_set,
                                            runtime, random, time_source)),
        factory_(std::make_shared<WorkerFactory>(config, child_->factory())) {}

  absl::Status initialize() override { return child_->initialize(); }
  Upstream::LoadBalancerFactorySharedPtr factory() override { return factory_; }

private:
  Upstream::ThreadAwareLoadBalancerPtr child_;
  Upstream::LoadBalancerFactorySharedPtr factory_;
};

} // namespace

Http::ClientConnectionPtr GoAwayProtocolOptions::createClientCodec(const Context& context) const {
  const auto& peer = context.connection.connectionInfoProvider().remoteAddress();
  if (context.type != Http::CodecType::HTTP2 || !peer || !peer->ip()) {
    return nullptr;
  }
  auto callbacks =
      std::make_unique<GoAwayCallbacks>(context.callbacks, registry_, peer->asString(), cooldown_);
  const auto& cluster = context.cluster;
  auto codec = std::make_unique<Http::Http2::ClientConnectionImpl>(
      context.connection, *callbacks, cluster.http2CodecStats(), context.random,
      cluster.httpProtocolOptions().http2Options(),
      cluster.maxResponseHeadersKb().value_or(Http::DEFAULT_MAX_REQUEST_HEADERS_KB),
      cluster.maxResponseHeadersCount(), Http::Http2::ProdNghttp2SessionFactory::get());
  return std::make_unique<GoAwayClientConnection>(std::move(callbacks), std::move(codec));
}

absl::StatusOr<Upstream::ProtocolOptionsConfigConstSharedPtr>
GoAwayOptionsFactory::createProtocolOptionsConfig(
    const Protobuf::Message& config,
    Server::Configuration::ProtocolOptionsFactoryContext& context) {
  const auto& proto = MessageUtil::downcastAndValidate<const GoAwayOptionsProto&>(
      config, context.messageValidationVisitor());
  const auto cooldown =
      std::chrono::milliseconds(PROTOBUF_GET_MS_OR_DEFAULT(proto, cooldown, 30000));
  return std::make_shared<GoAwayProtocolOptions>(registryFor(context.serverFactoryContext()),
                                                 cooldown);
}

absl::StatusOr<Upstream::LoadBalancerConfigPtr>
GoAwayLbFactory::loadConfig(Server::Configuration::ServerFactoryContext& context,
                            const Protobuf::Message& config) {
  const auto& proto = MessageUtil::downcastAndValidate<const GoAwayLbProto&>(
      config, context.messageValidationVisitor());
  for (const auto& policy : proto.child_policy().policies()) {
    auto* factory = Config::Utility::getAndCheckFactory<Upstream::TypedLoadBalancerFactory>(
        policy.typed_extension_config(), true);
    if (factory == nullptr) {
      continue;
    }
    const auto name = factory->name();
    if (name != "envoy.load_balancing_policies.round_robin" &&
        name != "envoy.load_balancing_policies.least_request" &&
        name != "envoy.load_balancing_policies.random") {
      return absl::InvalidArgumentError(
          "HBONE GOAWAY preference requires round_robin, least_request, or random");
    }
    auto child_proto = factory->createEmptyConfigProto();
    RETURN_IF_NOT_OK(
        Config::Utility::translateOpaqueConfig(policy.typed_extension_config().typed_config(),
                                               context.messageValidationVisitor(), *child_proto));
    auto child_config = factory->loadConfig(context, *child_proto);
    RETURN_IF_NOT_OK(child_config.status());
    return Upstream::LoadBalancerConfigPtr{std::make_unique<GoAwayLbConfig>(
        *factory, std::move(child_config.value()), registryFor(context),
        proto.max_selection_attempts() == 0 ? 16 : proto.max_selection_attempts(),
        proto.upstream_port_override())};
  }
  return absl::InvalidArgumentError("HBONE GOAWAY preference requires a supported child policy");
}

Upstream::ThreadAwareLoadBalancerPtr
GoAwayLbFactory::create(OptRef<const Upstream::LoadBalancerConfig> config,
                        const Upstream::ClusterInfo& cluster_info,
                        const Upstream::PrioritySet& priority_set, Runtime::Loader& runtime,
                        Random::RandomGenerator& random, TimeSource& time_source) {
  const auto* typed = dynamic_cast<const GoAwayLbConfig*>(config.ptr());
  ASSERT(typed != nullptr);
  return std::make_unique<ThreadAwareLb>(*typed, cluster_info, priority_set, runtime, random,
                                         time_source);
}

REGISTER_FACTORY(GoAwayOptionsFactory, Server::Configuration::ProtocolOptionsFactory);
REGISTER_FACTORY(GoAwayLbFactory, Upstream::TypedLoadBalancerFactory);

} // namespace Hbone
} // namespace Istio
} // namespace Extensions
} // namespace Envoy
