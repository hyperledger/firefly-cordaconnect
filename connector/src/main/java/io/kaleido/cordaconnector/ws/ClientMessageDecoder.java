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

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.websocket.DecodeException;
import javax.websocket.Decoder;
import javax.websocket.EndpointConfig;
import java.io.IOException;

public class ClientMessageDecoder implements Decoder.Text<ClientMessage>{
    final ObjectMapper objectMapper;

    public ClientMessageDecoder() {
        objectMapper = CustomMapper();
    }

    private ObjectMapper CustomMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        return mapper;
    }
    @Override
    public ClientMessage decode(String s) throws DecodeException {
        try {
            return objectMapper.readValue(s, ClientMessage.class);
        } catch (IOException e) {
            throw new DecodeException(s, e.getMessage(), e);
        }
    }

    @Override
    public boolean willDecode(String s) {
        return s != null;
    }

    @Override
    public void init(EndpointConfig config) {

    }

    @Override
    public void destroy() {

    }
}
