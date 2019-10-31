package com.softcom.esa.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.Filter;
import co.elastic.logstash.api.FilterMatchListener;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

// class name must match plugin name
@LogstashPlugin(name = "non_space_filter")
public class NonSpaceFilter implements Filter {

    private static final PluginConfigSpec<String> SOURCE_CONFIG =
            PluginConfigSpec.stringSetting("source", "message");
    private static final PluginConfigSpec<Boolean> DISCARD_CONFIG =
            PluginConfigSpec.booleanSetting("discardSame", true);
    private static final PluginConfigSpec<Boolean> HI_FREQ_CONFIG =
            PluginConfigSpec.booleanSetting("discardHiFrequentLogMessage", true);
    private static final int TIME_FIELD_START_INDEX = 12;
    private static final CharSequence HI_FREQ_TEXT = "HI FREQUENT LOG DETECTED";
    private final boolean discardHiFreqLog;
    private final boolean discardSame;

    private String id;
    private String sourceField;
    /**
     * Since the events in the {@link NonSpaceFilter#filter} method came as batch
     * (https://www.elastic.co/guide/en/logstash/current/java-filter-plugin.html)
     * we have to keep a previous state as a field variable to compare for previous Event
     */
    private Event previousEvent = null;

    public NonSpaceFilter(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        this.sourceField = config.get(SOURCE_CONFIG);
        this.discardHiFreqLog = config.get(HI_FREQ_CONFIG);
        this.discardSame = config.get(DISCARD_CONFIG);
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {
        Iterator<Event> iterator = events.iterator();
        while (iterator.hasNext()) {
            Event e = iterator.next();
            // On first start do not process the event but add it to the previousEvent for next comparison
            if (previousEvent != null) {
                Object message = e.getField(sourceField);
                Object previousMessage = previousEvent.getField(sourceField);
                if (message instanceof String && previousMessage instanceof String) {
                    // Discard HI-FREQ messages
                    if (discardHiFreqLog && ((String) message).contains(HI_FREQ_TEXT)) {
                        System.out.println("Dropping -" + HI_FREQ_TEXT + "- message");
                        iterator.remove();
                        previousEvent = e;
                        continue;
                    }
                    // Discard messages that are same
                    if (discardSame &&
                            ((String) message).substring(TIME_FIELD_START_INDEX).equals(
                                    ((String) previousMessage).substring(TIME_FIELD_START_INDEX)
                            )
                    ) {
                        System.out.println("Dropping - SIMILAR - message");
                        iterator.remove();
                        previousEvent = e;
                        continue;
                    }
                    previousEvent = e;
                    matchListener.filterMatched(e);
                }
            } else {
                previousEvent = e;
                // Handle case where HI FREQ LOG is the first message that gets processed
                Object message = e.getField(sourceField);
                if (message instanceof String) {
                    // Discard HI-FREQ messages
                    if (discardHiFreqLog && ((String) message).contains(HI_FREQ_TEXT)) {
                        System.out.println("Dropping -" + HI_FREQ_TEXT + "- message");
                        iterator.remove();
                        previousEvent = null;
                    }
                }
            }
        }
        return events;
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        List<PluginConfigSpec<?>> configSchema = new ArrayList<>();
        configSchema.add(SOURCE_CONFIG);
        configSchema.add(DISCARD_CONFIG);
        configSchema.add(HI_FREQ_CONFIG);
        return configSchema;
    }

    @Override
    public String getId() {
        return this.id;
    }
}
