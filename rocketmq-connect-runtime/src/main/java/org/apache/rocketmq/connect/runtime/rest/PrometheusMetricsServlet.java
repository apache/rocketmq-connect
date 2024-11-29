/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.rest;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class PrometheusMetricsServlet extends HttpServlet {
    private CollectorRegistry registry;

    public PrometheusMetricsServlet() {
        this(CollectorRegistry.defaultRegistry);
    }

    public PrometheusMetricsServlet(CollectorRegistry registry) {
        this.registry = registry;
    }

    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setStatus(200);
        resp.setContentType("text/plain; version=0.0.4; charset=utf-8");
        StringWriter writer = new StringWriter();

        this.writeEscapedHelp(writer, registry);
        resp.getOutputStream().print(writer.toString());
    }

    public void writeEscapedHelp(StringWriter writer, CollectorRegistry registry) throws IOException {
        Enumeration<Collector.MetricFamilySamples> metricFamilySamplesEnumeration = registry.metricFamilySamples();
        List<Collector.MetricFamilySamples> list = new ArrayList<>();
        while (metricFamilySamplesEnumeration.hasMoreElements()) {
            Collector.MetricFamilySamples metricFamilySamples = metricFamilySamplesEnumeration.nextElement();
            list.add(metricFamilySamples);
        }
        writeEscapedHelp(writer, list);
    }

    public void writeEscapedHelp(StringWriter writer, List<Collector.MetricFamilySamples> mfs) throws IOException {
        if (Objects.nonNull(mfs) && mfs.size() != 0) {
            for (Collector.MetricFamilySamples metricFamilySamples : mfs) {
                for (Iterator var3 = metricFamilySamples.samples.iterator(); var3.hasNext(); writer.write(10)) {
                    Collector.MetricFamilySamples.Sample sample = (Collector.MetricFamilySamples.Sample) var3.next();
                    writer.write(sample.name);
                    if (sample.labelNames.size() > 0) {
                        writer.write(123);

                        for (int i = 0; i < sample.labelNames.size(); ++i) {
                            writer.write((String) sample.labelNames.get(i));
                            writer.write("=\"");
                            writeEscapedLabelValue(writer, (String) sample.labelValues.get(i));
                            writer.write("\",");
                        }

                        writer.write(125);
                    }

                    writer.write(32);
                    writer.write(Collector.doubleToGoString(sample.value));
                    if (sample.timestampMs != null) {
                        writer.write(32);
                        writer.write(sample.timestampMs.toString());
                    }
                }
            }
        }

    }

    private static void writeEscapedLabelValue(Writer writer, String s) throws IOException {
        for (int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            switch (c) {
                case '\n':
                    writer.append("\\n");
                    break;
                case '"':
                    writer.append("\\\"");
                    break;
                case '\\':
                    writer.append("\\\\");
                    break;
                default:
                    writer.append(c);
            }
        }

    }

    private Set<String> parse(HttpServletRequest req) {
        String[] includedParam = req.getParameterValues("name[]");
        return (Set) (includedParam == null ? Collections.emptySet() : new HashSet(Arrays.asList(includedParam)));
    }

    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doGet(req, resp);
    }
}
