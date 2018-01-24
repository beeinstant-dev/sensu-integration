package com.beeinstant.sensu;

import com.beeinstant.metrics.MetricsLogger;
import com.beeinstant.metrics.MetricsManager;
import com.beeinstant.metrics.TimerMetric;
import com.beeinstant.metrics.Unit;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SensuMonitor {

    static class SensuTailerListener extends TailerListenerAdapter {
        private final Pattern lineRe = Pattern.compile("^.+\"client\":\"([^\"]+)\".+\"name\":\"([^\"]+)\",\"issued\":(\\d+).+\"duration\":([\\d\\.]+).+\"output\":\"([^\"]+)\".+\"status\":([\\d\\.]+).+$");

        public void handle(String line) {
            final MetricsLogger rootMetricsLogger = MetricsManager.getRootMetricsLogger();

            if (line.contains("\"level\":\"info\",\"message\":\"publishing check result\"")) {
                try (TimerMetric timer = rootMetricsLogger.startTimer("ProcessingTime")) {

                    Matcher matcher = lineRe.matcher(line);
                    if (matcher.find()) {
                        int timestamp = Integer.parseInt(matcher.group(3));

                        if ((int) (System.currentTimeMillis() / 1000) - timestamp <= 60) {
                            String client = matcher.group(1);
                            String checkName = matcher.group(2);
                            double duration = Double.parseDouble(matcher.group(4));
                            int status = Integer.parseInt(matcher.group(6));

                            final MetricsLogger metricsLogger = MetricsManager.getMetricsLogger(
                                    "client=" + client + ",checkname=" + checkName);

                            metricsLogger.record("Duration", duration, Unit.SECOND);
                            metricsLogger.record("Status", status, Unit.NONE);

                            if (status == 0) {
                                metricsLogger.incCounter("OK", 1);
                            } else if (status == 1) {
                                metricsLogger.incCounter("WARNING", 1);
                            } else if (status == 2) {
                                metricsLogger.incCounter("CRITICAL", 1);
                            } else {
                                metricsLogger.incCounter("UNKNOWN", 1);
                            }

                            String[] outputLines = matcher.group(5).split("\\\\n");
                            for (int i = 0; i < outputLines.length; i++) {
                                String[] fields = outputLines[i].split("\\s");
                                if (fields.length == 3) {
                                    try {
                                        String metricName = fields[0];
                                        double value = Double.parseDouble(fields[1]);
                                        metricsLogger.record(metricName, value, Unit.NONE);
                                    } catch (Exception e) {
                                        rootMetricsLogger.incCounter("ParseGraphiteException", 1);
                                    }
                                } else {
                                    rootMetricsLogger.incCounter("InvalidGraphiteMessage", 1);
                                }
                            }
                        } else {
                            rootMetricsLogger.incCounter("OldEvent", 1);
                        }
                    }
                } catch (Exception e) {
                    rootMetricsLogger.incCounter("Exception", 1);
                }
            }
        }
    }

    static public void main(String[] args) throws InterruptedException {
        final String sensuClientLog = System.getProperty("sensu.client.log", "");
        final AtomicBoolean running = new AtomicBoolean(true);

        MetricsManager.init("Sensu");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        if (!sensuClientLog.isEmpty()) {
            TailerListener listener = new SensuTailerListener();

            System.out.println("Start tailing " + sensuClientLog);

            Tailer tailer = Tailer.create(new File(sensuClientLog), listener);

            while (running.get()) {
                Thread.sleep(1000);
            }

            tailer.stop();

            System.out.println("Stopped tailing... Bye");
        }

        MetricsManager.shutdown();
    }
}
