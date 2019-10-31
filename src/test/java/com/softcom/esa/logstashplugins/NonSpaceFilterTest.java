package com.softcom.esa.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.FilterMatchListener;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstash.plugins.ContextImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class NonSpaceFilterTest {

    @Test
    public void testNonSpaceFilter() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("source", "message");
        configMap.put("discardSame", true);
        configMap.put("discardHiFrequentLogMessage", true);
        Configuration config = new ConfigurationImpl(configMap);
        Context context = new ContextImpl(null, null);
        NonSpaceFilter filter = new NonSpaceFilter("test-id", config, context);

        Event e = new org.logstash.Event();
        TestMatchListener matchListener = new TestMatchListener();
        e.setField("message", "14:43:15.948 ERROR      System: ****** HI FREQUENT LOG DETECTED! This text was logged " +
                "at least 5 times. It will not be logged again until a gap occurs of at least 1500 ms ******         " +
                "                            at (null:-1)    [Server-RoutingQueue-Thread]\n" +
                "                        routing 'InterestedObjectChangeEvent from -# for all: NEW - Notification: " +
                "[Oid=#,Src=-#,Time=May #, # #:#:# PM][ID=#,key=-#]' from 'DatabaseService' to: [RoutingClient " +
                "(NotificationService) <de.aerodata.mpa.notification" +
                ".NotificationService$InterestedObjectChangeEventDispatcher@#ff#>, RoutingClient (OwnSRUHandler) <de" +
                ".aerodata.mpa.flight.OwnSRUHandler$OwnChangeDispatcher@#ce#>, RoutingClient (DataSender) <de" +
                ".aerodata.mpa.database.distribution.DataSender$ConfigurationChangeDispatcher@#b#>, RoutingClient " +
                "(DataReceiver) <de.aerodata.mpa.database.distribution.DataReceiver$MessageChangeDispatcher@#b#>, " +
                "RoutingClient (Message client for data and events) <de.aerodata.mpa.message.email" +
                ".EMailHandler$InterestedObjectChangeEventDispatcher@#b#, de.aerodata.mpa.message" +
                ".AbstractMessageHandler$InterestedObjectChangeEventDispatcher@#b#>, RoutingClient (Report Data " +
                "Connector) <de.aerodata.mpa.report.ReportDataConnector$#@#f#c#e#>, RoutingClient (Report Handler) " +
                "<de.aerodata.mpa.report.ReportHandler$ObjectChangeDispatcher@#d#, de.aerodata.mpa.report" +
                ".ReportFormatProvider$ConfigChangeDispatcher@#b#>, RoutingClient (ImageHandler) <de.aerodata.mpa" +
                ".media.image.ImageHandler$ConfigurationChangeListener@#ed#b#>, RoutingClient (de.aerodata.mpa.media" +
                ".MediaHandler) <de.aerodata.mpa.media.MediaHandler$$Lambda$#@#ffc#>, RoutingClient (Message client " +
                "for data and events) <de.aerodata.mpa.message" +
                ".AbstractMessageHandler$InterestedObjectChangeEventDispatcher@#dc#c#>, RoutingClient (Message client" +
                " for data and events) <de.aerodata.mpa.message" +
                ".AbstractMessageHandler$InterestedObjectChangeEventDispatcher@#e#>, RoutingClient " +
                "(TrackFusProcessor) <de.aerodata.mpa.processing.trackfus.TrackFusProcessor$$Lambda$#@#b#dea#>, " +
                "RoutingClient (Equipment client for data and events) <de.aerodata.mpa.processing" +
                ".SynchronizedEventDispatcher@#ae#, de.aerodata.mpa.processing.SynchronizedEventDispatcher@#de#c#, de" +
                ".aerodata.mpa.processing.SynchronizedEventDispatcher@#d#a#a#, de.aerodata.mpa.processing" +
                ".SynchronizedEventDispatcher@#dc#c#, de.aerodata.mpa.processing.SynchronizedEventDispatcher@#d#a#e#," +
                " de.aerodata.mpa.processing.SynchronizedEventDispatcher@#d#a#, de.aerodata.mpa.processing" +
                ".SynchronizedEventDispatcher@#d#, de.aerodata.mpa.processing.SynchronizedEventDispatcher@#f#e#be#, " +
                "de.aerodata.mpa.processing.SynchronizedEventDispatcher@#d#c#a#, de.aerodata.mpa.processing" +
                ".SynchronizedEventDispatcher@#d#c#a#, de.aerodata.mpa.processing.SynchronizedEventDispatcher@#d#ab#," +
                " de.aerodata.mpa.processing.SynchronizedEventDispatcher@#c#f#, de.aerodata.mpa.processing" +
                ".SynchronizedEventDispatcher@#d#c#, de.aerodata.mpa.processing.SynchronizedEventDispatcher@#da#dde#," +
                " de.aerodata.mpa.processing.SynchronizedEventDispatcher@#f#a#e#, de.aerodata.mpa.processing" +
                ".SynchronizedEventDispatcher@#f#c#, de.aerodata.mpa.processing.SynchronizedEventDispatcher@#f#d#>, " +
                "RoutingClient (Cache) <de.aerodata.mpa.system.router.IDispatchListener$$Lambda$#@#d#de#>, " +
                "RoutingClient (DeploymentsInventoryService) <de.aerodata.mpa.system.router" +
                ".IDispatchListener$$Lambda$#@#c#cd#>, RoutingClient (DatabaseVesselHandler) <de.aerodata.mpa" +
                ".database.DatabaseVesselHandler$ObjectChangeDispatcher@#ac#>, RoutingClient (VesselEventGenerator) " +
                "<de.aerodata.mpa.notification.VesselEventGenerator$$Lambda$#@#ad#f#>, RoutingClient (DataTableModel " +
                "for Notification) <de.aerodata.mpa.tableclient.tablemodel.DataTableModel$$Lambda$#@#b#dda#>, " +
                "RoutingClient (DataTableModel for SystemStatus) <de.aerodata.mpa.tableclient.tablemodel" +
                ".DataTableModel$$Lambda$#@#efe#>, RoutingClient (DataTableModel for EquipmentStatus) <de.aerodata" +
                ".mpa.tableclient.tablemodel.DataTableModel$$Lambda$#@#e#>, RoutingClient (DataTableModel for " +
                "DataConnectionStatus) <de.aerodata.mpa.tableclient.tablemodel.DataTableModel$$Lambda$#@#f#>, " +
                "RoutingClient (remote client proxy for class 'InterestedObjectChangeEvent'), RoutingClient (remote " +
                "client proxy for class 'InterestedObjectChangeEvent'), RoutingClient (remote client proxy for class " +
                "'InterestedObjectChangeEvent')]                                       \n" +
                "                        ****** HI FREQUENT LOG DETECTED! This text was logged at least 5 times. It " +
                "will not be logged again until a gap occurs of at least 1500 ms ******                              ");
        Collection<Event> events = new ArrayList<>();
        events.add(e);

        Collection<Event> results = filter.filter(events, matchListener);

        Assert.assertEquals(0, results.size());

        // Check the removal of the similar message
        e.setField("message", "14:43:15.031 ERROR      System: routing 'DatabaseStatusContainer from -850033395 for " +
                "1407465844:");

        Event sameEventWithDifferentTime = new org.logstash.Event();
        sameEventWithDifferentTime.setField("message", "14:43:15.831 ERROR      System: routing " +
                "'DatabaseStatusContainer from -850033395 for 1407465844:");

        events.clear();
        events.add(e);
        events.add(sameEventWithDifferentTime);

        results = filter.filter(new ArrayList<>(events), matchListener);

        Assert.assertEquals(1, results.size());

        Event otherEvent = new org.logstash.Event();
        otherEvent.setField("message", "14:43:15.839 ERROR      System: routing 'DatabaseStatusContainer from " +
                "-850033395 for -135242813:");

        events.add(otherEvent);

        results = filter.filter(new ArrayList<>(events), matchListener);

        Assert.assertEquals(1, results.size());
    }
}

class TestMatchListener implements FilterMatchListener {

    private AtomicInteger matchCount = new AtomicInteger(0);

    @Override
    public void filterMatched(Event event) {
        matchCount.incrementAndGet();
    }

    public int getMatchCount() {
        return matchCount.get();
    }
}