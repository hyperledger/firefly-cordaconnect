// Copyright © 2021 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.kaleido.cordaconnector.ws;

import io.kaleido.cordaconnector.config.SpringContext;
import io.kaleido.cordaconnector.service.EventStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;

@Component
@ServerEndpoint(value = "/ws", decoders = {ClientMessageDecoder.class}, encoders = {GenericEncoder.class})
public class EventServerSocket {
    private static final Logger logger = LoggerFactory.getLogger(EventServerSocket.class);
    private final EventStreamService eventStreamService;
    public EventServerSocket() {
        this.eventStreamService = SpringContext.getApplicationContext().getBean("eventStreamService", EventStreamService.class);
    }
    @OnOpen
    public void onWebSocketConnect(Session sess)
    {
        logger.info("WS: Client Connected, id:{}", sess.getId());
        eventStreamService.addWebSocketConnection(new WebSocketConnection(sess.getId(), sess));
    }

    @OnMessage
    public void onWebSocketText(Session sess, ClientMessage message)
    {
        logger.info("WS: -> received message, client id: {}", sess.getId());
        eventStreamService.onWebSocketMessage(sess.getId(), message);
    }

    @OnClose
    public void onWebSocketClose(Session session, CloseReason reason)
    {
        logger.info("WS: Client connection with id {} closed, code {}, reason {}", session.getId(), reason.getCloseCode(), reason.getReasonPhrase());
        eventStreamService.removeWebSocketConnection(session.getId());
    }

    @OnError
    public void onWebSocketError(Session session, Throwable cause)
    {
        logger.error("WS: Client with id {} returned a error", session.getId(), cause);
    }

}
