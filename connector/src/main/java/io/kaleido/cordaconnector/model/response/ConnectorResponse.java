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

package io.kaleido.cordaconnector.model.response;

import org.springframework.http.HttpStatus;

public class ConnectorResponse<T> {
    private int statusCode;
    private boolean status;
    private String message;
    private T data;

    public ConnectorResponse() {
        this.statusCode = HttpStatus.OK.value();
        this.status = true;
        this.message = "SUCCESS";
    }

    public ConnectorResponse(T data) {
        this.statusCode = HttpStatus.OK.value();
        this.status = true;
        this.message = "SUCCESS";
        this.data = data;
    }

    public ConnectorResponse(int statusCode, boolean status, String message, T data) {
        this.statusCode = statusCode;
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
