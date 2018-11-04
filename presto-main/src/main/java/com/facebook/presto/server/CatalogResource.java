/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.presto.client.Catalog;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.DiscoveryNodeManager;
import com.facebook.presto.metadata.ForNodeManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;

@Path("/v1/catalog")
public class CatalogResource
{
    private static final Logger logger = Logger.get(CatalogResource.class);
    private static final Duration MAX_AGE = new Duration(10, TimeUnit.SECONDS);
    private static final long SLEEP_TIME = 1000;
    private static final String CONNECTOR_IDS = "connectorIds";
    private final StaticCatalogStore staticCatalogStore;
    private final CatalogManager catalogManager;
    private final DiscoveryNodeManager discoveryNodeManager;
    private final HttpClient httpClient;
    private final Node currentNode;
    private final ConcurrentMap<String, Catalog> registerCatalogs = new ConcurrentHashMap<>();
    private final boolean includeCoordinator;
    private Announcer announcer;

    @Inject
    public CatalogResource(StaticCatalogStore staticCatalogStore, Announcer announcer, CatalogManager catalogManager,
                           DiscoveryNodeManager discoveryNodeManager, @ForNodeManager HttpClient httpClient,
                           NodeSchedulerConfig config)
    {
        this.staticCatalogStore = requireNonNull(staticCatalogStore, "staticCatalogStore is null");
        this.announcer = requireNonNull(announcer, "announcer is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.discoveryNodeManager = requireNonNull(discoveryNodeManager, "discoveryNodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.currentNode = discoveryNodeManager.getCurrentNode();
        this.includeCoordinator = config.isIncludeCoordinator();
    }

    private static <T> T setFinal(Class<?> clazz, Object object, String name, T value)
    {
        try {
            Field field = clazz.getDeclaredField(name);
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(object, value);

            return value;
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static <T> T futureGet(ListenableFuture<T> future)
    {
        try {
            return future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @GET
    @Path("/exist")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Set<String> catalogExist(@QueryParam("catalogs") String catalogs)
    {
        requireNonNull(catalogs, "catalogs is null");
        SetMultimap<ConnectorId, Node> activeNodesByConnectorId = discoveryNodeManager.getActiveConnectorNodes();
        Set<String> activeCatalogs = activeNodesByConnectorId.keySet().stream().map(ConnectorId::getCatalogName).collect(toImmutableSet());
        return Splitter.on(",")
                .trimResults()
                .omitEmptyStrings()
                .splitToList(catalogs)
                .stream()
                .filter(catalog -> !activeCatalogs.contains(catalog) || !catalogManager.getCatalog(catalog).isPresent())
                .collect(Collectors.toSet());
    }

    @POST
    @Path("coordinator/register")
    @Consumes(MediaType.APPLICATION_JSON)
    public void registerCatalogToCoordinator(List<Catalog> catalogs)
    {
        requireNonNull(catalogs, "catalogs is null");
        long registerCatalogTimestamp = System.nanoTime();
        Set<Node> nodes = discoveryNodeManager.getNodes(NodeState.ACTIVE);
        List<Catalog> registerOrUpdateCatalogs = getNeedRegisterOrUpdateCatalogs(catalogs);
        if (registerOrUpdateCatalogs.isEmpty()) {
            logger.info("coordinator %s: register %d catalogs, but need register or update catalogs is empty",
                    currentNode.getHttpUri(), catalogs.size());
        }
        else {
            synchronized (CatalogResource.class) {
                registerOrUpdateCatalogs = getNeedRegisterOrUpdateCatalogs(registerOrUpdateCatalogs);
                if (registerOrUpdateCatalogs.isEmpty()) {
                    logger.info("coordinator %s: register %d catalogs, but need register or update catalogs is empty",
                            currentNode.getHttpUri(), catalogs.size());
                    return;
                }
                logger.info("coordinator %s: register %d catalogs, but need register or update %d catalogs",
                        currentNode.getHttpUri(), catalogs.size(), registerOrUpdateCatalogs.size());
                registerCatalog(registerOrUpdateCatalogs, nodes);

                // force refresh node and update latest node info
                refreshNodes(registerOrUpdateCatalogs);
            }
        }
    }

    /**
     * batch register catalogs
     */
    @POST
    @Path("/register")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<Catalog> registerCatalog(List<Catalog> catalogs)
    {
        logger.info("worker %s: start register or update %d catalogs", currentNode.getHttpUri(), catalogs.size());
        requireNonNull("catalogs is null");
        catalogs.stream().forEach(catalog -> {
            String catalogName = requireNonNull(catalog.getCatalogName(), "catalogName is null");
            String connectorName = requireNonNull(catalog.getConnectorName(), "connectorName is null");
            if (!catalogManager.getCatalog(catalogName).isPresent() || catalog.isUpdateCatalog()) {
                if (catalogManager.getCatalog(catalogName).isPresent() && catalog.isUpdateCatalog()) {
                    staticCatalogStore.removeCatalog(catalogName);
                }
                staticCatalogStore.loadCatalog(catalogName, connectorName, catalog.getProperties());
            }
        });

        ListenableFuture<Duration> future = (ListenableFuture<Duration>) updateConnectorIdAnnouncement(catalogs);
        Duration duration = futureGet(future);
        logger.info("worker %s register or update %d catalogs finished, maxAge duration %s",
                currentNode.getHttpUri(), catalogs.size(), String.valueOf(duration));
        return catalogs;
    }

    /**
     * register catalogs to coordinators and workers
     */
    private void registerCatalog(List<Catalog> catalogs, Set<Node> nodes)
    {
        catalogs.forEach(catalog -> catalog.setUpdateCatalog(true));
        logger.info("coordinator %s: start register or update %d catalog to %d worker",
                currentNode.getHttpUri(), catalogs.size(), nodes.size());

        Map<Node, HttpClient.HttpResponseFuture<StatusResponseHandler.StatusResponse>> nodeResponseFutures
                = new HashMap<>();
        nodes.forEach(node -> {
            URI uri = uriBuilderFrom(node.getHttpUri()).replacePath("/v1/catalog/register").build();
            Request request = preparePost()
                    .setUri(uri)
                    .setHeader(HttpHeaders.CONTENT_TYPE, com.google.common.net.MediaType.JSON_UTF_8.toString())
                    .setBodyGenerator(jsonBodyGenerator(JsonCodec.listJsonCodec(Catalog.class), catalogs))
                    .build();
            logger.info("coordinator %s: start register or update %d catalogs to worker %s",
                    currentNode.getHttpUri(), catalogs.size(), node.getHttpUri());
            HttpClient.HttpResponseFuture<StatusResponseHandler.StatusResponse> response
                    = httpClient.executeAsync(request, createStatusResponseHandler());
            nodeResponseFutures.put(node, response);
        });

        nodeResponseFutures.forEach((node, response) -> {
            StatusResponseHandler.StatusResponse status = futureGet(response);
            if (familyForStatusCode(status.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                String errorMsg = String.format("coordinator %s: register or update %d catalogs " +
                                "to worker %s failed. statusCode %d, statusMessage %s",
                        currentNode.getHttpUri(),
                        catalogs.size(),
                        node.getHttpUri(),
                        status.getStatusCode(),
                        status.getStatusMessage());
                logger.info(errorMsg);
                throw new PrestoException(StandardErrorCode.PROCEDURE_CALL_FAILED,
                        "register or update catalogs to worker failed");
            }
            else {
                logger.info("coorinator %s: register or update %d catalogs to worker %s finished",
                        currentNode.getHttpUri(), catalogs.size(), node.getHttpUri());
            }
        });

        // add to registerCatalogs
        catalogs.forEach(catalog -> registerCatalogs.put(catalog.getCatalogName(), catalog));
        logger.info("coordinator %s: register or update %d catalogs to %d workers finished",
                currentNode.getHttpUri(), catalogs.size(), nodes.size());
    }

    private List<Catalog> getNeedRegisterOrUpdateCatalogs(List<Catalog> catalogs)
    {
        SetMultimap<ConnectorId, Node> activeNodesByConnectorId = discoveryNodeManager.getActiveConnectorNodes();
        return catalogs.stream().filter(catalog -> {
            String catalogName = requireNonNull(catalog.getCatalogName(), "catalogName is null");
            return !activeNodesByConnectorId.containsKey(new ConnectorId(catalogName))
                    || !Objects.equals(catalog, registerCatalogs.get(catalogName));
        }).collect(toImmutableList());
    }

    private void refreshServices()
    {
        ListenableFuture<List<ServiceDescriptor>> future = discoveryNodeManager.refreshServices();
        futureGet(future);
    }

    private ListenableFuture<?> updateConnectorIdAnnouncement(List<Catalog> catalogs)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = Strings.nullToEmpty(properties.get(CONNECTOR_IDS));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(",").trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.addAll(catalogs.stream().map(Catalog::getCatalogName).collect(toImmutableSet()));
        properties.put(CONNECTOR_IDS, Joiner.on(",").join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        ServiceAnnouncement newAnnouncement = ServiceAnnouncement.serviceAnnouncement(announcement.getType()).addProperties(properties).build();
        setFinal(ServiceAnnouncement.class, newAnnouncement, "id", announcement.getId());
        announcer.addServiceAnnouncement(newAnnouncement);
        return announcer.forceAnnounce();
    }

    private void refreshNodes(List<Catalog> catalogs)
    {
        int i = 0;
        long startRefreshTimestamp = System.nanoTime();

        refreshServices();
        discoveryNodeManager.refreshNodes();

        while (!isAtLeastOneNodeContainsCatalogs(catalogs)) {
            if (Duration.nanosSince(startRefreshTimestamp).compareTo(MAX_AGE) > 0) {
                logger.info("coordinator %s: do not receive connectorIds announcer to workers " +
                        "after refreshNodes %d times and total cost %s", currentNode.getHttpUri(), i, MAX_AGE.toString());
                return;
            }
            try {
                Thread.sleep(SLEEP_TIME);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }

            refreshServices();
            discoveryNodeManager.refreshNodes();

            logger.info("coordinator %s: refreshNodes %d times and total cost %s",
                    currentNode.getHttpUri(),
                    ++i,
                    Duration.nanosSince(startRefreshTimestamp).toString(TimeUnit.MILLISECONDS));
        }
        logger.info("coordinator %s: discoveryNodeManager has received connectorIds announcer of workers after %s",
                currentNode.getHttpUri(), Duration.nanosSince(startRefreshTimestamp).toString(TimeUnit.MILLISECONDS));
    }

    private ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new RuntimeException("Presto announcement not found: " + announcements);
    }

    private boolean isAtLeastOneNodeContainsCatalogs(List<Catalog> catalogs)
    {
        Set<String> coordinatorNodeIds = discoveryNodeManager.getCoordinators().stream()
                .map(Node::getNodeIdentifier)
                .collect(toImmutableSet());
        SetMultimap<ConnectorId, Node> activeNodesByConnectorId = discoveryNodeManager.getActiveConnectorNodes();
        return catalogs.stream().allMatch(catalog -> {
            Set<Node> activeNodes = activeNodesByConnectorId.get(new ConnectorId(catalog.getCatalogName()));
            return activeNodes.stream().anyMatch(node -> includeCoordinator || !coordinatorNodeIds.contains(node.getNodeIdentifier()));
        });
    }
}
