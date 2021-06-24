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

package io.kaleido.firefly.cordapp.contracts;

import io.kaleido.firefly.cordapp.states.FireflyEvent;
import io.kaleido.firefly.cordapp.states.FireflyGroupNonce;
import net.corda.core.contracts.CommandData;
import net.corda.core.contracts.CommandWithParties;
import net.corda.core.contracts.Contract;
import net.corda.core.identity.AbstractParty;
import net.corda.core.transactions.LedgerTransaction;

import java.security.PublicKey;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static net.corda.core.contracts.ContractsDSL.requireSingleCommand;
import static net.corda.core.contracts.ContractsDSL.requireThat;

public class FireflyContract implements Contract {
    public static final String ID = "io.kaleido.firefly.cordapp.contracts.FireflyContract";

    @Override
    public void verify(LedgerTransaction tx) {
        final CommandWithParties<Commands> command = requireSingleCommand(tx.getCommands(), Commands.class);
        final Commands commandData = command.getValue();
        final Set<PublicKey> setOfSigners = new HashSet<>(command.getSigners());
        if(commandData instanceof Commands.FireflyEventCreate) {
            verifyFireflyEventCreate(tx, setOfSigners);
        } else if(commandData instanceof Commands.OrderingGroupCreate) {
            verifyOrderingGroupCreate(tx, setOfSigners);
        } else {
            throw new IllegalArgumentException("Unrecognised command.");
        }
    }

    private void verifyFireflyEventCreate(LedgerTransaction tx, Set<PublicKey> signers) {
        requireThat(require -> {
            List<FireflyGroupNonce> inContexts = tx.inputsOfType(FireflyGroupNonce.class);
            List<FireflyGroupNonce> outContexts = tx.outputsOfType(FireflyGroupNonce.class);
            List<FireflyEvent> eventStates = tx.outputsOfType(FireflyEvent.class);
            require.using("A a single firefly group nonce must be consumed when creating a firefly Event.",
                    tx.getInputs().size() == 1 && inContexts.size() == 1);
            require.using("One Firefly event state and a new Firefly group nonce be created.",
                    tx.getOutputs().size() == 2 && outContexts.size() == 1 && eventStates.size() == 1);
            final FireflyGroupNonce inContext = inContexts.get(0);
            final FireflyGroupNonce outContext = outContexts.get(0);
            final FireflyEvent outState = eventStates.get(0);
            require.using("The Firefly group nonce value must be incremented by 1.",
                    outContext.getNonce() == inContext.getNonce()+1);
            require.using("The output and input nonce groups must be same.",
                    outContext.getLinearId().equals(inContext.getLinearId()));
            require.using("participants of input group should be same as output group", inContext.getParticipants().equals(outContext.getParticipants()));
            require.using("author of output event must be same as author of output nonce",
                    outContext.getAuthor().getOwningKey().equals(outState.getAuthor().getOwningKey()));
            require.using("author must be a signer", signers.contains(outState.getAuthor().getOwningKey()));
            return null;
        });
    }

    private void verifyOrderingGroupCreate(LedgerTransaction tx, Set<PublicKey> signers) {
        requireThat(require -> {
            List<FireflyGroupNonce> outContexts = tx.outputsOfType(FireflyGroupNonce.class);
            require.using("No inputs should be consumed when creating an group nonce between parties.",
                    tx.getInputs().isEmpty());
            require.using("Only one output nonce should be created.",
                    tx.getOutputs().size() == 1 && outContexts.size() == 1);
            final FireflyGroupNonce out = outContexts.get(0);
            final List<PublicKey> keys = out.getParticipants().stream().map(AbstractParty::getOwningKey).collect(Collectors.toList());
            require.using("All of the participants must be signers.",
                    signers.containsAll(keys));
            require.using("author should be one of the participants", keys.contains(out.getAuthor().getOwningKey()));
            require.using("The nonce value must be 0.",
                    out.getNonce() == 0L);
            return null;
        });
    }

    public interface Commands extends CommandData {
        class FireflyEventCreate implements Commands {}
        class OrderingGroupCreate implements Commands {}
    }
}
