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

import javax.websocket.CloseReason;
import javax.websocket.EncodeException;
import javax.websocket.Session;
import java.io.IOException;

public class WebSocketConnection {
    private String id;
    private Session session;

    public WebSocketConnection(String id, Session session) {
        this.id = id;
        this.session = session;
    }

    public boolean isOpen() {
        return this.session.isOpen();
    }

    public String getId() {
        return id;
    }

    public Session getSession() {
        return session;
    }

    public void close() throws IOException {
        session.close(new CloseReason(CloseReason.CloseCodes.GOING_AWAY, "Going away."));
    }

    public void sendData(Object data) throws IOException, EncodeException {
        session.getBasicRemote().sendObject(data);
    }
}
