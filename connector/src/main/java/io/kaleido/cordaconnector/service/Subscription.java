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

package io.kaleido.cordaconnector.service;

import io.kaleido.cordaconnector.config.SpringContext;
import io.kaleido.cordaconnector.db.entity.SubscriptionInfo;
import io.kaleido.cordaconnector.exception.CordaConnectionException;
import io.kaleido.cordaconnector.model.common.Event;
import io.kaleido.cordaconnector.rpc.NodeRPCClient;
import net.corda.core.contracts.ContractState;
import net.corda.core.contracts.StateRef;
import net.corda.core.contracts.TransactionState;
import net.corda.core.node.services.Vault;
import net.corda.core.node.services.vault.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.corda.core.node.services.vault.QueryCriteriaUtils.DEFAULT_PAGE_NUM;

public class Subscription {
    public static final Instant BEGINNING = Instant.parse("2009-01-03T10:15:00.00Z");
    // query page size to be used for pushing past events
    private static final int PAGE_SIZE=1000;
    private static final Logger logger = LoggerFactory.getLogger(Subscription.class);
    // Information about subscription that is persisted
    private SubscriptionInfo subscriptionInfo;
    // Stream to which events from this subscription are pushed to
    private EventStream stream;
    // timestamp based HWM used for checkpointing
    private Instant timeHWM;
    // State class type for this subscription
    private Class<? extends ContractState> stateTypeClass;
    // Observable used for listening to future updates on the events the subscription is interested in
    private Observable<Vault.Update<ContractState>> cordaRx = null;

    private NodeRPCClient rpcClient;

    // Subscription to the observable used fo clean up
    private rx.Subscription cordaSx = null;
    private AtomicBoolean isPaused = new AtomicBoolean(false);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public Subscription(SubscriptionInfo subscriptionInfo) {
        this.subscriptionInfo = subscriptionInfo;
        this.timeHWM = subscriptionInfo.getFromTime()==null?Instant.now():subscriptionInfo.getFromTime().toInstant();
        this.rpcClient = SpringContext.getApplicationContext().getBean("nodeRPCClient", NodeRPCClient.class);
    }

    public boolean isPaused() {
        return isPaused.get();
    }

    // pushes all events for the subscription since fromTime by querying the node's vault to eventstream
    private void pushPastEvents(Instant fromTime) throws ClassNotFoundException {
        Class<? extends ContractState>  clazz = null;
        try {
            clazz = getCordaStateType();
        } catch (ClassNotFoundException e) {
            logger.error("{}", subscriptionInfo.getId(),e);
            throw e;
        }
        QueryCriteria.TimeCondition timeCondition = new QueryCriteria.TimeCondition(QueryCriteria.TimeInstantType.RECORDED, new ColumnPredicate.BinaryComparison(BinaryComparisonOperator.GREATER_THAN_OR_EQUAL, fromTime));
        QueryCriteria criteria = new QueryCriteria.VaultQueryCriteria()
                .withRelevancyStatus(subscriptionInfo.getRelevancyStatus())
                .withContractStateTypes(Set.of(clazz))
                .withStatus(subscriptionInfo.getStateStatus())
                .withTimeCondition(timeCondition);
        scheduler.submit(() -> {
            int currentPage=DEFAULT_PAGE_NUM;
            while(true) {
                Vault.Page<ContractState> res  = null;
                try {
                    res = rpcClient.getRpcProxy().vaultQueryByWithPagingSpec(
                            ContractState.class,
                            criteria,
                            new PageSpecification(currentPage, PAGE_SIZE)
                    );
                } catch (CordaConnectionException e) {
                    logger.error("{} error while querying vault for past events", subscriptionInfo.getId(), e);
                    onError();
                    return;
                }
                logger.debug("{} Pushing past events, query to vault finished for page {}", subscriptionInfo.getId(), currentPage);
                List<Event<TransactionState<ContractState>>> events = new ArrayList<>();
                for(int i=0; i<res.getStates().size(); i++) {
                    events.add(new Event<>(res.getStates().get(i).getState(), subscriptionInfo.getId(), res.getStates().get(i).getState().getData().getClass().getCanonicalName(), res.getStates().get(i).getRef(), res.getStatesMetadata().get(i).getRecordedTime(), res.getStatesMetadata().get(i).getConsumedTime()));
                }
                events.stream().forEach(event -> {
                    logger.info("{} pushing new event to stream {}, consumed at {}, recorded at {}", subscriptionInfo.getId(), stream.getEventStreamInfo().getId(), event.getConsumedTime(), event.getRecordedTime());
                    stream.pushNewEvent(event);
                });
                if(res.getStates().size() < PAGE_SIZE) return;
                currentPage++;
            }
        });
    }

    private Class<? extends ContractState> getCordaStateType() throws ClassNotFoundException {
        return Class.forName(this.subscriptionInfo.getStateType()).asSubclass(ContractState.class);
    }


    private void processUpdate(Vault.Update<ContractState> update) throws ClassNotFoundException {
        logger.debug("{} processing vault update, consumed:{}, produced:{}", subscriptionInfo.getId(), update.getConsumed().size(), update.getProduced().size());
        if(stateTypeClass == null) {
            try {
                stateTypeClass = getCordaStateType();
            } catch (ClassNotFoundException e) {
                logger.error("{} class for type is not in classpath {}", subscriptionInfo.getId(), subscriptionInfo.getStateType(), e);
                throw e;
            }
        }
        List<StateRef> stateRefs = new ArrayList<>();
        update.getConsumed().stream().forEach(state -> stateRefs.add(state.getRef()));
        update.getProduced().stream().forEach(state -> stateRefs.add(state.getRef()));
        logger.debug("{} querying vault to post process update, stateType {}, stateRefs {}, state status {}, state relevancy status {}", subscriptionInfo.getId(), stateTypeClass, stateRefs, subscriptionInfo.getStateStatus(), subscriptionInfo.getRelevancyStatus());
        QueryCriteria criteria = new QueryCriteria.VaultQueryCriteria()
                .withRelevancyStatus(subscriptionInfo.getRelevancyStatus())
                .withContractStateTypes(Set.of(stateTypeClass))
                .withStateRefs(stateRefs)
                .withStatus(subscriptionInfo.getStateStatus());

        scheduler.submit(() -> {
            int currentPage = DEFAULT_PAGE_NUM;
            while (true) {
                Vault.Page<ContractState> res;
                try {
                    res = rpcClient.getRpcProxy().vaultQueryByWithPagingSpec(
                            ContractState.class,
                            criteria,
                            new PageSpecification(currentPage, PAGE_SIZE)
                    );
                } catch (CordaConnectionException e) {
                    logger.error("{} error in processing update, failed to fetch states from vault",subscriptionInfo.getId(), e);
                    onError();
                    return;
                }
                logger.debug("{} Post process of update event,  query to vault finished for page {}, size {}", subscriptionInfo.getId(), currentPage, res.getStates().size());
                List<Event<TransactionState<ContractState>>> events = new ArrayList<>();
                for (int i = 0; i < res.getStates().size(); i++) {
                    events.add(new Event<>(res.getStates().get(i).getState(), subscriptionInfo.getId(), res.getStates().get(i).getState().getData().getClass().getCanonicalName(), res.getStates().get(i).getRef(), res.getStatesMetadata().get(i).getRecordedTime(), res.getStatesMetadata().get(i).getConsumedTime()));
                }
                events.stream().forEach(event -> {
                    // Discard past events if any
                    // stale consumed update
                    if(event.getConsumedTime() != null && (this.timeHWM.isAfter(event.getConsumedTime()) || this.timeHWM.equals(event.getConsumedTime()))) return;
                    // stale produced update
                    if(event.getConsumedTime() == null && (this.timeHWM.isAfter(event.getRecordedTime()) || this.timeHWM.equals(event.getRecordedTime()))) return;

                    logger.info("{} pushing new event to stream {}, consumed at {}, recorded at {}", subscriptionInfo.getId(), stream.getEventStreamInfo().getId(), event.getConsumedTime(), event.getRecordedTime());
                    stream.pushNewEvent(event);
                });
                if(res.getStates().size() < PAGE_SIZE) return;
                currentPage++;
            }
        });
    }

    private void _setupCordaObservable() throws IOException, CordaConnectionException {
        // Subscribe to future updates
        logger.info("{} creating future observables", subscriptionInfo.getId());
        QueryCriteria criteria = new QueryCriteria.VaultQueryCriteria()
                .withRelevancyStatus(subscriptionInfo.getRelevancyStatus())
                .withStatus(subscriptionInfo.getStateStatus());
        // we are interested only in updates, but need to specify pagination to avoid pagination exceptions when vault has more results than default page size (200)
        cordaRx = rpcClient.getRpcProxy().vaultTrackByWithPagingSpec(ContractState.class, criteria, new PageSpecification(1, 1)).getUpdates();
        cordaSx = cordaRx.subscribe(update -> {
            try {
                processUpdate(update);
            } catch (ClassNotFoundException e) {
                logger.error("{} failed to process update received from corda, pausing subscription", subscriptionInfo.getId(), e);
                onError();
            }
        });
    }

    private void _cleanupObservable() {
        if(this.cordaSx != null && !this.cordaSx.isUnsubscribed()) {
            logger.info("{} Unsubscribed from future updates", subscriptionInfo.getId());
            this.cordaSx.unsubscribe();
        }
    }

    public void initializeOrReset() {
        logger.info("{} initializing subscription...", subscriptionInfo.getId());
        // clear stream checkpoints if any
        stream.clearCheckpoints();
        Timestamp startTime = subscriptionInfo.getFromTime();
        Timestamp lastCheckPoint = subscriptionInfo.getLastCheckpoint();
        if(lastCheckPoint != null) startTime = lastCheckPoint;
        if (Instant.now().isAfter(startTime.toInstant())) {
            // Push past events;
            this.timeHWM = startTime.toInstant();
            try {
                pushPastEvents(startTime.toInstant());
            } catch (ClassNotFoundException e) {
                logger.error("{} Failed to initialize subscription", subscriptionInfo.getId(),e);
                onError();
                return;
            }
        }
        // cleanup existing observable
        _cleanupObservable();
        try {
            _setupCordaObservable();
        } catch (IOException | CordaConnectionException e) {
            logger.error("{} Failed to initialize subscription", subscriptionInfo.getId(),e);
            onError();
            return;
        }
        isPaused.set(false);
    }

    public void pauseSubscription(){
        logger.info("{} Pausing subscription", subscriptionInfo.getId());
        isPaused.set(true);
        _cleanupObservable();
    }

    public void resumeSubscription(){
        //push past events
        logger.info("{} Resuming subscription", subscriptionInfo.getId());
        try {
            pushPastEvents(this.timeHWM);
        } catch (ClassNotFoundException e) {
            logger.error("{} Failed to resume subscription", subscriptionInfo.getId(),e);
            onError();
            return;
        }
        try {
            _setupCordaObservable();
        } catch (IOException | CordaConnectionException e) {
            logger.error("{} Failed to resume subscription", subscriptionInfo.getId(),e);
            onError();
            return;
        }
        isPaused.set(false);
    }

    public SubscriptionInfo getSubscriptionInfo() {
        return subscriptionInfo;
    }

    public void setSubscriptionInfo(SubscriptionInfo subscriptionInfo) {
        this.subscriptionInfo = subscriptionInfo;
    }

    public void onError() {
        pauseSubscription();
    }

    public EventStream getStream() {
        return stream;
    }

    public void setStream(EventStream stream) {
        this.stream = stream;
    }

    public Instant getTimeHWM() {
        return timeHWM;
    }

    public void setTimeHWM(Instant timeHWM) {
        this.timeHWM = timeHWM;
    }
}

