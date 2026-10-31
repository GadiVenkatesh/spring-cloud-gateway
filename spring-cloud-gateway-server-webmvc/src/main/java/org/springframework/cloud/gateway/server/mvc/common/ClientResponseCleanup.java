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

package org.springframework.cloud.gateway.server.mvc.common;

import org.jspecify.annotations.Nullable;

import org.springframework.http.client.ClientHttpResponse;

/**
 * Tracks a proxy response that may arrive after a circuit breaker has already returned
 * a fallback. In that case the response cannot be written to the downstream response and
 * must be closed instead.
 */
public final class ClientResponseCleanup {

	private @Nullable ClientHttpResponse response;

	private @Nullable Runnable closeAction;

	private boolean abandoned;

	/**
	 * Register the upstream response unless the caller has already abandoned the exchange.
	 * @param clientResponse the upstream response
	 * @param closeAction the response-specific cleanup action
	 * @return whether the response can still be processed normally
	 */
	public boolean register(ClientHttpResponse clientResponse, Runnable closeAction) {
		synchronized (this) {
			if (!this.abandoned) {
				this.response = clientResponse;
				this.closeAction = closeAction;
				return true;
			}
		}
		closeAction.run();
		return false;
	}

	/**
	 * Mark the response as closed after it has been copied to the downstream response.
	 * @param clientResponse the response that was closed
	 */
	public synchronized void responseClosed(ClientHttpResponse clientResponse) {
		if (this.response == clientResponse) {
			this.response = null;
			this.closeAction = null;
		}
	}

	/**
	 * Abandon the exchange and close any upstream response that has already arrived.
	 */
	public void abandon() {
		Runnable actionToRun;
		synchronized (this) {
			this.abandoned = true;
			actionToRun = this.closeAction;
			this.response = null;
			this.closeAction = null;
		}
		if (actionToRun != null) {
			actionToRun.run();
		}
	}

}
