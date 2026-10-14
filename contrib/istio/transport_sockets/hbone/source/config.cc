#include "envoy/registry/registry.h"
#include "envoy/server/transport_socket_config.h"

#include "source/common/config/utility.h"

#include "contrib/envoy/extensions/transport_sockets/hbone/v3/hbone.pb.h"
#include "contrib/envoy/extensions/transport_sockets/hbone/v3/hbone.pb.validate.h"
#include "contrib/istio/transport_sockets/hbone/source/hbone.h"

namespace Envoy {
namespace Extensions {
namespace TransportSockets {
namespace Hbone {

class ConfigFactory : public Server::Configuration::UpstreamTransportSocketConfigFactory {
public:
  std::string name() const override { return "envoy.transport_sockets.hbone"; }
  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<envoy::extensions::transport_sockets::hbone::v3::Hbone>();
  }
  absl::StatusOr<Network::UpstreamTransportSocketFactoryPtr> createTransportSocketFactory(
      const Protobuf::Message& message,
      Server::Configuration::TransportSocketFactoryContext& context) override {
    const auto& config = MessageUtil::downcastAndValidate<
        const envoy::extensions::transport_sockets::hbone::v3::Hbone&>(
        message, context.messageValidationVisitor());
    auto& inner_factory = Envoy::Config::Utility::getAndCheckFactory<
        Server::Configuration::UpstreamTransportSocketConfigFactory>(config.transport_socket());
    auto inner_config = Envoy::Config::Utility::translateToFactoryConfig(
        config.transport_socket(), context.messageValidationVisitor(), inner_factory);
    auto inner = inner_factory.createTransportSocketFactory(*inner_config, context);
    RETURN_IF_NOT_OK_REF(inner.status());
    auto shared_config = std::make_shared<Config>(
        Config{context.serverFactoryContext().clusterManager(), config.cluster(),
               std::chrono::milliseconds(PROTOBUF_GET_MS_OR_DEFAULT(config, connect_timeout, 5000)),
               PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, buffer_limit_bytes, 65536)});
    return std::make_unique<SocketFactory>(std::move(inner.value()), std::move(shared_config));
  }
};

REGISTER_FACTORY(ConfigFactory, Server::Configuration::UpstreamTransportSocketConfigFactory);

} // namespace Hbone
} // namespace TransportSockets
} // namespace Extensions
} // namespace Envoy
