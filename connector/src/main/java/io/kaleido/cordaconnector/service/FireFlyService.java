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

import io.kaleido.cordaconnector.exception.CordaConnectionException;
import io.kaleido.cordaconnector.model.common.BroadcastBatchData;
import io.kaleido.cordaconnector.rpc.NodeRPCClient;
import io.kaleido.firefly.cordapp.flows.CreateBroacastBatchFlow;
import net.corda.core.identity.CordaX500Name;
import net.corda.core.identity.Party;
import net.corda.core.transactions.SignedTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class FireFlyService {
    private static final Logger logger = LoggerFactory.getLogger(FireFlyService.class);
    @Autowired
    NodeRPCClient rpcClient;

    public FireFlyService() {
    }

    public Set<Party> validateAndGetParticipants(List<String> participants) throws CordaConnectionException {
        logger.debug("Validating participant list {}", participants);
        Set<Party> uniqueParticipants = new HashSet<>();
        for(String observer: participants) {
            Party party = rpcClient.getRpcProxy().wellKnownPartyFromX500Name(CordaX500Name.parse(observer));
            if(party == null) {
                logger.error("No party with name {} found", observer);
                throw new RuntimeException("No party with name "+observer+" found.");
            }
            uniqueParticipants.add(party);
        }
        logger.debug("Valid unique participant set {}", uniqueParticipants);
        return uniqueParticipants;
    }


    public String broadcastBatch(BroadcastBatchData batchData) throws CordaConnectionException, ExecutionException, InterruptedException {
        Set<Party> uniqueParticipants = validateAndGetParticipants(batchData.getObservers());
        Party me = rpcClient.getRpcProxy().nodeInfo().getLegalIdentities().get(0);
        uniqueParticipants.add(me);
        List<Party> otherObservers = uniqueParticipants.stream().filter(party -> !party.equals(me)).collect(Collectors.toList());
        SignedTransaction signedTransaction = rpcClient.getRpcProxy().startTrackedFlowDynamic(CreateBroacastBatchFlow.class, batchData.getBatchId(), batchData.getPayloadRef(), otherObservers, batchData.getGroupId()).getReturnValue().get();
        return signedTransaction.getId().toString();
    }
}
