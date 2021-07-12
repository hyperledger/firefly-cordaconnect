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

import co.paralleluniverse.fibers.Suspendable;
import com.google.common.collect.ImmutableList;
import io.kaleido.firefly.cordapp.contracts.FireflyContract;
import io.kaleido.firefly.cordapp.states.FireflyGroupNonce;
import net.corda.core.contracts.Command;
import net.corda.core.contracts.ContractState;
import net.corda.core.contracts.StateAndRef;
import net.corda.core.contracts.UniqueIdentifier;
import net.corda.core.flows.*;
import net.corda.core.identity.AbstractParty;
import net.corda.core.identity.Party;
import net.corda.core.node.services.Vault;
import net.corda.core.node.services.vault.PageSpecification;
import net.corda.core.node.services.vault.QueryCriteria;
import net.corda.core.node.services.vault.Sort;
import net.corda.core.node.services.vault.SortAttribute;
import net.corda.core.transactions.SignedTransaction;
import net.corda.core.transactions.TransactionBuilder;
import net.corda.core.utilities.ProgressTracker;

import java.util.*;
import java.util.stream.Collectors;

import static net.corda.core.node.services.vault.QueryCriteriaUtils.DEFAULT_PAGE_NUM;

public class CreateFireflyEventFlow<T extends ContractState> extends FlowLogic<SignedTransaction> {
    protected final List<Party> observers;
    protected final UUID groupId;
    private final ProgressTracker.Step GENERATING_TRANSACTION = new ProgressTracker.Step("Generating transaction based on new firefly event.");
    private final ProgressTracker.Step VERIFYING_TRANSACTION = new ProgressTracker.Step("Verifying contract constraints.");
    private final ProgressTracker.Step SIGNING_TRANSACTION = new ProgressTracker.Step("Signing transaction with our private key.");
    private final ProgressTracker.Step FINALISING_TRANSACTION = new ProgressTracker.Step("Obtaining notary signature and recording transaction.") {
        @Override
        public ProgressTracker childProgressTracker() {
            return FinalityFlow.Companion.tracker();
        }
    };

    // The progress tracker checkpoints each stage of the flow and outputs the specified messages when each
    // checkpoint is reached in the code. See the 'progressTracker.currentStep' expressions within the call()
    // function.
    private final ProgressTracker progressTracker = new ProgressTracker(
            GENERATING_TRANSACTION,
            VERIFYING_TRANSACTION,
            SIGNING_TRANSACTION,
            FINALISING_TRANSACTION
    );

    public T getFireflyEvent(){
        return null;
    }

    @Override
    public ProgressTracker getProgressTracker() {
        return progressTracker;
    }

    public CreateFireflyEventFlow(List<Party> observers, UUID groupId) {
        this.observers = observers;
        this.groupId = groupId;
    }

    @Suspendable
    @Override
    public SignedTransaction call() throws FlowException {
        // Obtain a reference to the notary we want to use.
        final Party notary = getServiceHub().getNetworkMapCache().getNotaryIdentities().get(0);
        // Generate an unsigned transaction.
        progressTracker.setCurrentStep(GENERATING_TRANSACTION);
        Party me = getOurIdentity();
        final Command<FireflyContract.Commands.FireflyEventCreate> txCommand = new Command<>(
                new FireflyContract.Commands.FireflyEventCreate(),
                ImmutableList.of(me.getOwningKey()));
        final StateAndRef<FireflyGroupNonce> inContext = getGroupNonce(groupId);
        final T output = getFireflyEvent();
        final TransactionBuilder txBuilder = new TransactionBuilder(notary)
                .addInputState(inContext)
                .addOutputState(output, FireflyContract.ID)
                .addOutputState(updateGroupNonce(inContext.getState().getData()), FireflyContract.ID)
                .addCommand(txCommand);
        progressTracker.setCurrentStep(VERIFYING_TRANSACTION);
        txBuilder.verify(getServiceHub());

        progressTracker.setCurrentStep(SIGNING_TRANSACTION);
        final SignedTransaction signedTx = getServiceHub().signInitialTransaction(txBuilder);

        progressTracker.setCurrentStep(FINALISING_TRANSACTION);
        Set<FlowSession> flowSessions = observers.stream().map(this::initiateFlow).collect(Collectors.toSet());
        return subFlow(new FinalityFlow(signedTx, flowSessions));
    }

    public FireflyGroupNonce updateGroupNonce(FireflyGroupNonce oldNonce) {
        return new FireflyGroupNonce(oldNonce.getLinearId(), getOurIdentity(), new LinkedHashSet<>(oldNonce.getParticipants()), oldNonce.getNonce()+1);
    }

    @Suspendable
    private StateAndRef<FireflyGroupNonce> getGroupNonce(UUID groupId) throws FlowException {
        Set<AbstractParty> partiesInContext = new LinkedHashSet<>(this.observers);
        partiesInContext.add(getOurIdentity());
        UniqueIdentifier linearId = new UniqueIdentifier(null, groupId);
        QueryCriteria queryCriteria = new QueryCriteria.LinearStateQueryCriteria(
                null,
                ImmutableList.of(linearId),
                Vault.StateStatus.UNCONSUMED,
                null);
        Sort.SortColumn oldestFirst = new Sort.SortColumn(new SortAttribute.Standard(Sort.VaultStateAttribute.RECORDED_TIME), Sort.Direction.ASC);
        Vault.Page<FireflyGroupNonce> res = getServiceHub().getVaultService().queryBy(FireflyGroupNonce.class, queryCriteria, new PageSpecification(DEFAULT_PAGE_NUM, 1), new Sort(ImmutableList.of(oldestFirst)));
        if (res.getStates().isEmpty()) {
            return subFlow(new CreateGroupNonceFlow(linearId, partiesInContext));
        } else {
            return res.getStates().get(0);
        }
    }
}
