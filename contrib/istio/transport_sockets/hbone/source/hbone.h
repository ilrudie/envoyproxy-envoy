#pragma once

#include "source/extensions/transport_sockets/common/passthrough.h"

#include "contrib/istio/transport_sockets/hbone/source/io_handle.h"

namespace Envoy {
namespace Extensions {
namespace TransportSockets {
namespace Hbone {

struct Config {
  Upstream::ClusterManager& cluster_manager_;
  const std::string cluster_;
  const std::chrono::milliseconds timeout_;
  const uint32_t buffer_limit_;
};
using ConfigSharedPtr = std::shared_ptr<const Config>;

class Socket : public PassthroughSocket {
public:
  Socket(Network::TransportSocketPtr&& inner, ConfigSharedPtr config)
      : PassthroughSocket(std::move(inner)), config_(std::move(config)),
        failure_reason_(std::make_shared<std::string>()) {}

  Network::IoHandlePtr createIoHandle(Event::Dispatcher& dispatcher) override {
    return std::make_unique<IoHandle>(dispatcher, config_->cluster_manager_, config_->cluster_,
                                      config_->timeout_, config_->buffer_limit_, failure_reason_);
  }
  absl::string_view failureReason() const override {
    return failure_reason_->empty() ? transport_socket_->failureReason() : *failure_reason_;
  }

private:
  const ConfigSharedPtr config_;
  const std::shared_ptr<std::string> failure_reason_;
};

class SocketFactory : public PassthroughFactory {
public:
  SocketFactory(Network::UpstreamTransportSocketFactoryPtr&& inner, ConfigSharedPtr config)
      : PassthroughFactory(std::move(inner)), config_(std::move(config)) {}

  Network::TransportSocketPtr
  createTransportSocket(Network::TransportSocketOptionsConstSharedPtr options,
                        Upstream::HostDescriptionConstSharedPtr host) const override {
    auto inner = transport_socket_factory_->createTransportSocket(options, host);
    if (!inner) {
      return nullptr;
    }
    return std::make_unique<Socket>(std::move(inner), config_);
  }

private:
  const ConfigSharedPtr config_;
};

} // namespace Hbone
} // namespace TransportSockets
} // namespace Extensions
} // namespace Envoy
