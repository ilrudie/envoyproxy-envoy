.. _config_native_hbone:

Native HBONE transport
======================

The experimental ``envoy.transport_sockets.hbone`` contrib extension establishes an HTTP/2
CONNECT stream as the I/O handle of an ordinary upstream connection. It requires no internal
listener, paired internal socket, or TCP proxy to encapsulate application traffic.

Connection establishment
------------------------

Each application connection acquires a stream from the configured outer cluster's worker-local
HTTP/2 pool. It sends CONNECT with the selected workload's ``IP:application-port`` as the authority.
The I/O handle withholds connection readiness and application writes until the peer returns
``200`` without END_STREAM. The configured application transport then performs its normal
connection handshake above that stream.

A rejection, reset, pool failure, or timeout before acceptance fails connection establishment.
The router can retry a request with ``connect-failure`` retry policy without replaying application
bytes into a rejected tunnel. Resets after readiness retain established connection semantics;
the extension does not turn them into retriable connection failures.

Outer pooling is provided by Envoy's existing upstream HTTP/2 pools. Distinct application
connections to one workload use distinct CONNECT streams and may share an outer connection.
Different application ports on that workload can also share it. Different workload IPs use
different outer connections. Normal application HTTP/1 keepalive and HTTP/2 multiplexing continue
inside an accepted tunnel.

Control-plane configuration
---------------------------

For the direct waypoint-to-workload hop, the service cluster retains IP endpoints. Its upstream
transport wraps the existing application transport:

.. code-block:: yaml

  transport_socket:
    name: envoy.transport_sockets.hbone
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.transport_sockets.hbone.v3.Hbone
      cluster: native_hbone_outer
      connect_timeout: 5s
      buffer_limit_bytes: 65536
      transport_socket:
        name: envoy.transport_sockets.raw_buffer
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.transport_sockets.raw_buffer.v3.RawBuffer

``native_hbone_outer`` is an ORIGINAL_DST cluster with CLUSTER_PROVIDED load balancing,
``original_dst_lb_config.upstream_port_override: 15008``, and explicit HTTP/2 protocol options.
Configure its transport with the waypoint's existing mesh TLS settings, proxy credentials,
certificate validation, and ``h2`` ALPN. Retain the existing outer cluster's connection limits,
socket options, and idle timeout. Do not put the HBONE transport on the outer cluster itself.

The extension supplies only the selected destination to outer host selection. The outer cluster
continues to apply Envoy's host, protocol, socket, and transport configuration partitions.
The extension rejects a selected outer peer whose IP differs from the workload IP. It does not
add an outer pool partition for the original caller IP.

Identity and metadata
---------------------

The destination authenticates the waypoint as its direct mesh client. Original caller addresses,
credentials, SNI overrides, SAN overrides, and shared filter state are not copied into the outer
pool context. Application transport options remain scoped to the application transport. The
logical connection reports an unspecified local address with port zero; it cannot bind an
original caller address or request source spoofing from the outer connection.

Original caller telemetry is separate from the authenticated mesh identity. This first version
does not copy the shim's metadata exchange or baggage headers into CONNECT. A future metadata
facility must define each field's request/stream scope without choosing outbound credentials
from caller data.

Scope of the proof of concept
-----------------------------

The first implementation targets the direct workload hop. Gateway delegation, multi-network
address overrides, double HBONE, original-source binding, and arbitrary per-application OS socket
options are unsupported. TCP_NODELAY is harmless on the logical stream and is accepted. Other
socket options fail explicitly; outer cluster socket options continue to apply to the physical
connection.

Half-close maps to CONNECT END_STREAM. Full connection close resets an unfinished CONNECT stream.
This proof of concept does not retain a detached stream to drain queued outer DATA after the
application connection has been destroyed. In particular, flushing the application connection
buffer does not guarantee that queued HTTP/2 DATA survives a subsequent full close. Reliable
graceful TCP draining across that boundary requires additional lifecycle work.

There is also a known receive/reset bug: DATA followed immediately by END_STREAM and
``RST_STREAM(NO_ERROR)`` can cause the adapter to discard a buffered application response,
resulting in an HTTP 503. Keeping the accepted tunnel open avoids this sequence in smoke tests
but does not fix the bug.

The inner transport interface permits application TLS above CONNECT. Additional transport
combinations, including application PROXY protocol and nested HBONE, require their own validation.
The connection's TLS information describes its application transport; outer mesh TLS remains on
the outer pool connection. This is not a replacement for the existing metadata and observability
plumbing of a production Istio waypoint.

The generic core change is an optional ``TransportSocket::createIoHandle`` hook in the default
client connection factory, forwarded by passthrough transports. Normal transports return no
handle and continue to allocate their ordinary socket. The contrib extension owns CONNECT,
flow control, cancellation, half-close, and readiness scheduling.
