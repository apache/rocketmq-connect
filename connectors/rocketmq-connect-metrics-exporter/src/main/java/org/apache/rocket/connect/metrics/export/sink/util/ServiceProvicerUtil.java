package org.apache.rocket.connect.metrics.export.sink.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.rocket.connect.metrics.export.sink.connector.MetricsExporter;

/**
 * @author: ming
 */
public class ServiceProvicerUtil {

    public static List<MetricsExporter> getMetricsExporterServices(){
        List<MetricsExporter> metricsExporterList = new ArrayList<>();
        ServiceLoader<MetricsExporter> metricsExporters = ServiceLoader.load(MetricsExporter.class);
        Iterator<MetricsExporter> iterator = metricsExporters.iterator();
        while (iterator.hasNext()){
            metricsExporterList.add(iterator.next());
        }
        return metricsExporterList;
    }
}
