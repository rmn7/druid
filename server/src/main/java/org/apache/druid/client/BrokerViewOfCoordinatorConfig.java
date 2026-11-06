/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import jakarta.validation.constraints.NotNull;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.selector.HistoricalFilter;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.server.BrokerDynamicConfigResource;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Broker view of the coordinator dynamic configuration, and its derived values such as target and source clone servers
 * and turbo loading servers.
 * This class is registered as a managed lifecycle to fetch the coordinator dynamic configuration on startup. Further
 * updates are handled through {@link BrokerDynamicConfigResource}.
 */
public class BrokerViewOfCoordinatorConfig extends BaseBrokerViewOfConfig<CoordinatorDynamicConfig> implements HistoricalFilter
{
  private final CoordinatorClient coordinatorClient;
  private final boolean preferNonTurboLoadingServers;

  // volatile, not synchronized: getCurrentServersToIgnore() is called per-segment during query planning.
  // Under high concurrency, synchronized causes monitor convoy with 100x throughput degradation.
  // Each field is an immutable Set reference, so volatile provides sufficient visibility.
  private volatile Set<String> targetCloneServers = Set.of();
  private volatile Set<String> sourceCloneServers = Set.of();
  private volatile Set<String> turboLoadingServers = Set.of();

  @Inject
  public BrokerViewOfCoordinatorConfig(
      final CoordinatorClient coordinatorClient,
      final BrokerSegmentWatcherConfig segmentWatcherConfig
  )
  {
    this(coordinatorClient, segmentWatcherConfig.isPreferNonTurboLoadingServers());
  }

  @VisibleForTesting
  public BrokerViewOfCoordinatorConfig(CoordinatorClient coordinatorClient)
  {
    this(coordinatorClient, false);
  }

  @VisibleForTesting
  public BrokerViewOfCoordinatorConfig(CoordinatorClient coordinatorClient, boolean preferNonTurboLoadingServers)
  {
    this.coordinatorClient = coordinatorClient;
    this.preferNonTurboLoadingServers = preferNonTurboLoadingServers;
  }

  @Override
  protected CoordinatorDynamicConfig fetchConfigFromClient() throws Exception
  {
    return coordinatorClient.getCoordinatorDynamicConfig().get();
  }

  @Override
  protected String getConfigTypeName()
  {
    return "coordinator";
  }

  /**
   * Update the config view with a new coordinator dynamic config snapshot. Also updates the source and target clone
   * servers and the turbo loading servers based on the new dynamic configuration.
   */
  @Override
  public void setDynamicConfig(@NotNull CoordinatorDynamicConfig updatedConfig)
  {
    super.setDynamicConfig(updatedConfig);
    final Map<String, String> cloneServers = updatedConfig.getCloneServers();
    this.targetCloneServers = ImmutableSet.copyOf(cloneServers.keySet());
    this.sourceCloneServers = ImmutableSet.copyOf(cloneServers.values());
    this.turboLoadingServers = ImmutableSet.copyOf(updatedConfig.getTurboLoadingNodes());
  }

  /**
   * Filters out servers based on the cloneQueryMode. If
   * {@link BrokerSegmentWatcherConfig#isPreferNonTurboLoadingServers()} is enabled, also filters out servers in turbo
   * loading mode, unless they are the only servers left, so that turbo loading servers are queried only as a last
   * resort.
   */
  @Override
  public Int2ObjectRBTreeMap<Set<QueryableDruidServer>> getQueryableServers(
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers,
      CloneQueryMode mode
  )
  {
    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> queryableServers =
        filterServers(historicalServers, getCurrentServersToIgnore(mode));

    if (!preferNonTurboLoadingServers) {
      return queryableServers;
    }

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> nonTurboServers =
        filterServers(queryableServers, turboLoadingServers);

    final boolean onlyTurboLoadingServersAvailable = nonTurboServers.values().stream().allMatch(Set::isEmpty);
    return onlyTurboLoadingServersAvailable ? queryableServers : nonTurboServers;
  }

  private static Int2ObjectRBTreeMap<Set<QueryableDruidServer>> filterServers(
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers,
      Set<String> serversToIgnore
  )
  {
    if (serversToIgnore.isEmpty()) {
      return historicalServers;
    }

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> filteredHistoricals = new Int2ObjectRBTreeMap<>();
    for (int priority : historicalServers.keySet()) {
      Set<QueryableDruidServer> servers = historicalServers.get(priority);
      filteredHistoricals.put(priority,
                              servers.stream()
                                     .filter(server -> !serversToIgnore.contains(server.getServer().getHost()))
                                     .collect(Collectors.toSet())
      );
    }

    return filteredHistoricals;
  }

  /**
   * Get the list of servers that should not be queried based on the cloneQueryMode parameter.
   * Each branch reads only one volatile field, so readers may see targetCloneServers and sourceCloneServers
   * from different setDynamicConfig() calls — this is acceptable since no CloneQueryMode needs both.
   */
  private Set<String> getCurrentServersToIgnore(CloneQueryMode cloneQueryMode)
  {
    switch (cloneQueryMode) {
      case PREFERCLONES:
        // Remove servers being cloned targets, so that clones are queried.
        return sourceCloneServers;
      case EXCLUDECLONES:
        // Remove clones, so that only source servers are queried.
        return targetCloneServers;
      case INCLUDECLONES:
        // Don't remove either.
        return Set.of();
      default:
        throw DruidException.defensive("Unexpected value of cloneQueryMode[%s]", cloneQueryMode);
    }
  }
}
