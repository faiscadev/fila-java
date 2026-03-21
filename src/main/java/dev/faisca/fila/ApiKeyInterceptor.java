package dev.faisca.fila;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * gRPC client interceptor that attaches a {@code Bearer} API key to the {@code authorization}
 * metadata header on every outgoing RPC.
 */
final class ApiKeyInterceptor implements ClientInterceptor {
  private static final Metadata.Key<String> AUTH_KEY =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  private final String headerValue;

  ApiKeyInterceptor(String apiKey) {
    this.headerValue = "Bearer " + apiKey;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        headers.put(AUTH_KEY, headerValue);
        super.start(responseListener, headers);
      }
    };
  }
}
