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

import io.kaleido.cordaconnector.model.common.SubscriptionData;
import net.corda.core.node.services.Vault;

import javax.persistence.*;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

@Entity
@Table(name = "subscriptions")
public class SubscriptionInfo implements Serializable {
    @Id
    @Size(max = 39)
    @Column(name = "id", unique = true)
    private String id;

    @Size(max = 60)
    @Column(name = "name")
    private String name;

    @ManyToOne
    @JoinColumn(name = "stream_id")
    private EventStreamInfo stream;

    @Column(name = "state_type")
    private String stateType;

    @Column(name = "state_status")
    @Enumerated(EnumType.STRING)
    private Vault.StateStatus stateStatus;

    @Column(name = "state_relevancy_status")
    @Enumerated(EnumType.STRING)
    private Vault.RelevancyStatus relevancyStatus;

    @Column(name = "from_time")
    private Timestamp fromTime;

    @Column(name = "last_checkpoint")
    private Timestamp lastCheckpoint;

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

    public SubscriptionInfo(String id, SubscriptionData subscriptionData) {
        this.id = id;
        this.name = subscriptionData.getName();
        this.stateStatus = subscriptionData.getFilter().getStateStatus();
        this.relevancyStatus = subscriptionData.getFilter().getRelevancyStatus();
        this.stateType = subscriptionData.getFilter().getStateType();
        this.fromTime = Timestamp.from(subscriptionData.getFromTime());
    }

    public SubscriptionInfo() {}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EventStreamInfo getStream() {
        return stream;
    }

    public void setStream(EventStreamInfo stream) {
        this.stream = stream;
    }

    public String getStateType() {
        return stateType;
    }

    public void setStateType(String stateType) {
        this.stateType = stateType;
    }

    public Vault.StateStatus getStateStatus() {
        return stateStatus;
    }

    public void setStateStatus(Vault.StateStatus stateStatus) {
        this.stateStatus = stateStatus;
    }

    public Vault.RelevancyStatus getRelevancyStatus() {
        return relevancyStatus;
    }

    public void setRelevancyStatus(Vault.RelevancyStatus relevancyStatus) {
        this.relevancyStatus = relevancyStatus;
    }

    public Timestamp getFromTime() {
        return fromTime;
    }

    public void setFromTime(Timestamp fromTime) {
        this.fromTime = fromTime;
    }

    public Timestamp getLastCheckpoint() {
        return lastCheckpoint;
    }

    public void setLastCheckpoint(Timestamp lastCheckpoint) {
        this.lastCheckpoint = lastCheckpoint;
    }

    public Timestamp getCreated() {
        return created;
    }


    public Timestamp getUpdated() {
        return updated;
    }
}

