package com.solr.ltr.action;

import com.solr.ltr.stats.LTRStat;
import com.solr.ltr.stats.LTRStats;
import com.solr.ltr.stats.StatName;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TransportLTRStatsActionTests extends ESIntegTestCase {

    private TransportLTRStatsAction action;
    private LTRStats ltrStats;
    private Map<String, LTRStat> statsMap;

    @Before
    public void setup() throws Exception {
        super.setUp();

        statsMap = new HashMap<>();
        statsMap.put(StatName.PLUGIN_STATUS.getName(), new LTRStat(false, () -> "cluster_stat"));
        statsMap.put(StatName.CACHE.getName(), new LTRStat(true, () -> "node_stat"));

        ltrStats = new LTRStats(statsMap);

        action = new TransportLTRStatsAction(
                client().threadPool(),
                clusterService(),
                mock(TransportService.class),
                mock(ActionFilters.class),
                ltrStats
        );
    }

    public void testNewResponse() {
        String[] nodeIds = null;
        LTRStatsAction.LTRStatsNodesRequest ltrStatsRequest = new LTRStatsAction.LTRStatsNodesRequest(nodeIds);
        ltrStatsRequest.setStatsToBeRetrieved(ltrStats.getStats().keySet());

        List<LTRStatsAction.LTRStatsNodeResponse> responses = new ArrayList<>();
        List<FailedNodeException> failures = new ArrayList<>();

        LTRStatsAction.LTRStatsNodesResponse ltrStatsResponse = action.newResponse(ltrStatsRequest, responses, failures);
        assertEquals(1, ltrStatsResponse.getClusterStats().size());
    }

    public void testNodeOperation() {
        String[] nodeIds = null;
        LTRStatsAction.LTRStatsNodesRequest ltrStatsRequest = new LTRStatsAction.LTRStatsNodesRequest(nodeIds);
        ltrStatsRequest.setStatsToBeRetrieved(ltrStats.getStats().keySet());

        LTRStatsAction.LTRStatsNodeResponse response = action.nodeOperation(new LTRStatsAction.LTRStatsNodeRequest(ltrStatsRequest));

        Map<String, Object> stats = response.getStatsMap();

        assertEquals(1, stats.size());
    }
}
