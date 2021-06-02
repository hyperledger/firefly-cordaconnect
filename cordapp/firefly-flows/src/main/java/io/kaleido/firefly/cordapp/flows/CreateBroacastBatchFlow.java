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

package io.kaleido.firefly.cordapp.flows;

import io.kaleido.firefly.cordapp.states.BroadcastBatch;
import net.corda.core.flows.InitiatingFlow;
import net.corda.core.flows.StartableByRPC;
import net.corda.core.identity.Party;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@StartableByRPC
@InitiatingFlow
public class CreateBroacastBatchFlow extends CreateFireflyEventFlow<BroadcastBatch> {
    private final String payloadRef;
    private final String batchId;
    public CreateBroacastBatchFlow(String batchId, String payloadRef, List<Party> observers, UUID groupId) {
        super(observers, groupId);
        this.batchId = batchId;
        this.payloadRef = payloadRef;
    }

    @Override
    public BroadcastBatch getFireflyEvent(){
        List<Party> participants = new ArrayList<>(this.observers);
        participants.add(getOurIdentity());
        return new BroadcastBatch(getOurIdentity(), batchId, payloadRef, participants);
    }
}
