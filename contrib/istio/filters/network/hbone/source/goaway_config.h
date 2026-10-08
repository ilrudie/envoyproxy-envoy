#pragma once

#include "envoy/http/client_codec_factory.h"
#include "envoy/server/filter_config.h"

#include "source/common/upstream/load_balancer_factory_base.h"

#include "contrib/envoy/extensions/filters/network/istio_hbone/v3alpha/istio_hbone.pb.validate.h"
#include "contrib/istio/filters/network/hbone/source/goaway.h"

namespace Envoy {
namespace Extensions {
namespace Istio {
namespace Hbone {

using GoAwayOptionsProto = envoy::extensions::filters::network::istio_hbone::v3alpha::GoAwayOptions;
using GoAwayLbProto =
    envoy::extensions::filters::network::istio_hbone::v3alpha::GoAwayLoadBalancingConfig;

class GoAwayProtocolOptions : public Upstream::ProtocolOptionsConfig,
                              public Http::ClientCodecFactory {
public:
  GoAwayProtocolOptions(GoAwayRegistrySharedPtr registry, std::chrono::milliseconds cooldown)
      : registry_(std::move(registry)), cooldown_(cooldown) {}
  OptRef<const Http::ClientCodecFactory> upstreamHttpClientCodecFactory() const override {
    return *this;
  }
  Http::ClientConnectionPtr createClientCodec(const Context& context) const override;

private:
  const GoAwayRegistrySharedPtr registry_;
  const std::chrono::milliseconds cooldown_;
};

class GoAwayOptionsFactory : public Server::Configuration::ProtocolOptionsFactory {
public:
  std::string name() const override { return "envoy.upstream_options.istio_hbone"; }
  std::string category() const override { return "envoy.upstream_options"; }
  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<GoAwayOptionsProto>();
  }
  ProtobufTypes::MessagePtr createEmptyProtocolOptionsProto() override {
    return createEmptyConfigProto();
  }
  absl::StatusOr<Upstream::ProtocolOptionsConfigConstSharedPtr> createProtocolOptionsConfig(
      const Protobuf::Message& config,
      Server::Configuration::ProtocolOptionsFactoryContext& context) override;
};

class GoAwayLbFactory : public Upstream::TypedLoadBalancerFactoryBase<GoAwayLbProto> {
public:
  GoAwayLbFactory() : TypedLoadBalancerFactoryBase("envoy.load_balancing_policies.istio_hbone") {}
  absl::StatusOr<Upstream::LoadBalancerConfigPtr>
  loadConfig(Server::Configuration::ServerFactoryContext& context,
             const Protobuf::Message& config) override;
  Upstream::ThreadAwareLoadBalancerPtr create(OptRef<const Upstream::LoadBalancerConfig> config,
                                              const Upstream::ClusterInfo& cluster_info,
                                              const Upstream::PrioritySet& priority_set,
                                              Runtime::Loader& runtime,
                                              Random::RandomGenerator& random,
                                              TimeSource& time_source) override;
};

} // namespace Hbone
} // namespace Istio
} // namespace Extensions
} // namespace Envoy
