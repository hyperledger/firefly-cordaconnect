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

package io.kaleido.cordaconnector.db.entity;

import io.kaleido.cordaconnector.model.response.data.ErrorHandling;

import javax.persistence.*;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

@Entity
@Table(name = "eventstreams")
public class EventStreamInfo implements Serializable {
    @Id
    @Size(max = 39)
    @Column(name = "id", unique = true)
    private String id;

    @Size(max = 60)
    @Column(name = "name")
    private String name;

    @Column(name = "batch_size")
    private int batchSize;

    @Column(name = "batch_timeout_ms")
    private int batchTimeoutMs;

    @Column(name = "batch_retry_delay_sec")
    private int batchRetryDelaySec;

    @Column(name = "error_handling")
    @Enumerated(EnumType.STRING)
    private ErrorHandling errorHandling;

    @Column(name = "websocket_topic", unique = true)
    private String websocketTopic;

    @Column(name = "suspend_status")
    private boolean suspended;

    private Timestamp created;
    private Timestamp updated;

    @PrePersist
    protected void onCreate() {
        Date now = new Date();
        created = new Timestamp(now.getTime());
    }

    @PreUpdate
    protected void onUpdate() {
        Date now = new Date();
        updated = new Timestamp(now.getTime());
    }
}
