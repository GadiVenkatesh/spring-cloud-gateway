/*
 * Copyright 2013-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.gateway.server.mvc.handler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.springframework.cloud.gateway.server.mvc.common.AbstractProxyExchange;
import org.springframework.cloud.gateway.server.mvc.common.ClientResponseCleanup;
import org.springframework.cloud.gateway.server.mvc.common.MvcUtils;
import org.springframework.cloud.gateway.server.mvc.config.GatewayMvcProperties;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestClient;
import org.springframework.web.servlet.function.ServerResponse;

public class RestClientProxyExchange extends AbstractProxyExchange {

	private final RestClient restClient;

	public RestClientProxyExchange(RestClient restClient, GatewayMvcProperties properties) {
		super(properties);
		this.restClient = restClient;
	}

	@Override
	public ServerResponse exchange(Request request) {
		Objects.requireNonNull(request.getUri(), "uri cannot be null");
		RestClient.RequestBodySpec requestSpec = restClient.method(request.getMethod())
			.uri(request.getUri())
			.headers(httpHeaders -> httpHeaders.putAll(request.getHeaders()));
		if (isBodyPresent(request)) {
			requestSpec.body(outputStream -> copyBody(request, outputStream));
		}
		Object cleanupAttribute = request.getServerRequest().attributes().get(MvcUtils.CLIENT_RESPONSE_CLEANUP_ATTR);
		ClientResponseCleanup responseCleanup = cleanupAttribute instanceof ClientResponseCleanup cleanup ? cleanup : null;
		return requestSpec.exchange((clientRequest, clientResponse) -> doExchange(request, clientResponse, responseCleanup),
				false);
	}

	private static boolean isBodyPresent(Request request) {
		try {
			return !request.getServerRequest().servletRequest().getInputStream().isFinished();
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static int copyBody(Request request, OutputStream outputStream) throws IOException {
		return StreamUtils.copy(request.getServerRequest().servletRequest().getInputStream(), outputStream);
	}

	private ServerResponse doExchange(Request request, ClientHttpResponse clientResponse,
			@Nullable ClientResponseCleanup responseCleanup) throws IOException {
		@Nullable InputStream body = null;
		try {
			InputStream responseBody = clientResponse.getBody();
			body = responseBody;
			IOException abandonedException = new IOException("Upstream response was abandoned");
			Runnable closeAction = () -> RestClientProxyExchange.this.closeResponse(clientResponse, responseBody,
					abandonedException);
			if (responseCleanup != null && !responseCleanup.register(clientResponse, closeAction)) {
				// The circuit breaker has already returned a fallback. This late response
				// will never be written downstream, so release it immediately.
				return ServerResponse.ok().build();
			}
			// put the body input stream in a request attribute so filters can read it.
			MvcUtils.putAttribute(request.getServerRequest(), MvcUtils.CLIENT_RESPONSE_INPUT_STREAM_ATTR, responseBody);
			MvcUtils.putAttribute(request.getServerRequest(), MvcUtils.CLIENT_RESPONSE_ATTR, clientResponse);
			ServerResponse serverResponse = GatewayServerResponse.status(clientResponse.getStatusCode())
				.build((req, httpServletResponse) -> {
					// get input stream from request attribute in case it was
					// modified.
					InputStream inputStream = MvcUtils.getAttribute(request.getServerRequest(),
							MvcUtils.CLIENT_RESPONSE_INPUT_STREAM_ATTR);
					Objects.requireNonNull(inputStream, "input stream cannot be null");
					IOException copyException = null;
					try {
						// copy body from request to clientHttpRequest
						RestClientProxyExchange.this.copyResponseBody(clientResponse, inputStream,
								httpServletResponse.getOutputStream());
					}
					catch (IOException ex) {
						copyException = ex;
						throw ex;
					}
					finally {
						try {
							RestClientProxyExchange.this.closeResponse(clientResponse, inputStream, copyException);
						}
						finally {
							if (responseCleanup != null) {
								responseCleanup.responseClosed(clientResponse);
							}
						}
					}
					return null;
				});
			ClientHttpResponseAdapter proxyExchangeResponse = new ClientHttpResponseAdapter(clientResponse);
			request.getResponseConsumers()
				.forEach(responseConsumer -> responseConsumer.accept(proxyExchangeResponse, serverResponse));
			return serverResponse;
		}
		catch (IOException | RuntimeException ex) {
			try {
				if (responseCleanup != null) {
					responseCleanup.abandon();
				}
				if (body == null) {
					clientResponse.close();
				}
				else if (responseCleanup == null) {
					IOException closeReason = ex instanceof IOException ioException ? ioException
							: new IOException("Failed to prepare upstream response", ex);
					RestClientProxyExchange.this.closeResponse(clientResponse, body, closeReason);
				}
			}
			catch (RuntimeException closeException) {
				ex.addSuppressed(closeException);
			}
			throw ex;
		}
	}

}
