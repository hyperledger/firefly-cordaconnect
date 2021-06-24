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

package io.kaleido.cordaconnector.model.common;

import net.corda.core.node.services.Vault;

public class StateFilter {
    private String stateType;
    private Vault.StateStatus stateStatus = Vault.StateStatus.ALL;
    private Vault.RelevancyStatus relevancyStatus = Vault.RelevancyStatus.ALL;

    public StateFilter() {
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
}
