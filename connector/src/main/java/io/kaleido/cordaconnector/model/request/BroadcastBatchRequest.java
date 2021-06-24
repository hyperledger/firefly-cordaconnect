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

package io.kaleido.cordaconnector.model.request;
import net.corda.core.identity.Party;
import java.util.List;
import java.util.UUID;

public class BroadcastBatchRequest {
    private String batchId;
    private String payloadRef;
    private List<Party> observers;
    private UUID groupId;

    public BroadcastBatchRequest() {
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getPayloadRef() {
        return payloadRef;
    }

    public void setPayloadRef(String payloadRef) {
        this.payloadRef = payloadRef;
    }

    public List<Party> getObservers() {
        return observers;
    }

    public void setObservers(List<Party> observers) {
        this.observers = observers;
    }

    public UUID getGroupId() {
        return groupId;
    }

    public void setGroupId(UUID groupId) {
        this.groupId = groupId;
    }
}
