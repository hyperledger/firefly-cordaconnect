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

package io.kaleido.cordaconnector.exception;

import io.kaleido.cordaconnector.model.response.ConnectorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.NoSuchElementException;

@ControllerAdvice
public class ConnectionExceptionHandler {
    @ExceptionHandler(CordaConnectionException.class)
    public ResponseEntity<ConnectorResponse<String>> handleCordaException(CordaConnectionException e) {
        return new ResponseEntity<ConnectorResponse<String>>(new ConnectorResponse<String>(HttpStatus.CONFLICT.value(), false, e.getMessage(), null), HttpStatus.CONFLICT);
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<ConnectorResponse<String>> handleNotFound(NoSuchElementException e) {
        return new ResponseEntity<ConnectorResponse<String>>(new ConnectorResponse<String>(HttpStatus.CONFLICT.value(), false, e.getMessage(), null), HttpStatus.NOT_FOUND);
    }
}
