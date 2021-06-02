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
import net.corda.core.contracts.LinearState;
import net.corda.core.contracts.UniqueIdentifier;
import net.corda.core.identity.AbstractParty;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@BelongsToContract(FireflyContract.class)
public class FireflyGroupNonce implements LinearState {
    private final UniqueIdentifier groupId;
    private final AbstractParty author;
    private final Set<AbstractParty> partiesForContext;
    private final long nonce;

    public FireflyGroupNonce(UniqueIdentifier groupId, AbstractParty author, Set<AbstractParty> partiesForContext, long nonce) {
        this.groupId = groupId;
        this.author = author;
        this.partiesForContext = partiesForContext;
        this.nonce = nonce;
    }

    public long getNonce() {
        return this.nonce;
    }

    public AbstractParty getAuthor() {return this.author; }

    @NotNull
    @Override
    public UniqueIdentifier getLinearId() {
        return this.groupId;
    }

    @NotNull
    @Override
    public List<AbstractParty> getParticipants() {
        return new ArrayList<>(partiesForContext);
    }
}
