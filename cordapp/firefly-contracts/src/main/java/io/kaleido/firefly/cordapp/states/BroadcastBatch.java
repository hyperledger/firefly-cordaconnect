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

package io.kaleido.firefly.cordapp.states;

import io.kaleido.firefly.cordapp.contracts.FireflyContract;
import net.corda.core.contracts.BelongsToContract;
import net.corda.core.identity.AbstractParty;
import net.corda.core.identity.Party;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

@BelongsToContract(FireflyContract.class)
public class BroadcastBatch implements FireflyEvent {
    private final Party author;
    private final String batchId;
    private final String payloadRef;
    private final List<Party> participants;

    public BroadcastBatch(Party author, String batchId, String payloadRef, List<Party> participants) {
        this.author = author;
        this.batchId = batchId;
        this.payloadRef = payloadRef;
        this.participants = participants;
    }

    @NotNull
    @Override
    public List<AbstractParty> getParticipants() {
        return new ArrayList<>(participants);
    }

    @Override
    public String toString() {
        return String.format("BroadcastBatch(author=%s, batchId=%s, payloadRef=%s, participants=%s)", author, batchId, payloadRef, participants);
    }

    @Override
    public Party getAuthor() {
        return author;
    }


    public String getBatchId() {
        return batchId;
    }

    public String getPayloadRef() {
        return payloadRef;
    }
}
