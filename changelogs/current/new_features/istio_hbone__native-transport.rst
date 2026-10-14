added the experimental contrib ``envoy.transport_sockets.hbone`` upstream transport, which establishes a CONNECT stream
before exposing application connection readiness and uses existing HTTP/2 pools without an internal listener.
