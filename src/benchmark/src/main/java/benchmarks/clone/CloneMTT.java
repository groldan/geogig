/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package benchmarks.clone;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Main class to run the clone "Maximum Theoretical Throughput" tests
 *
 */
@OutputTimeUnit(TimeUnit.SECONDS)
public class CloneMTT {

    public static void main(String args[]) {
        List<String> params = new ArrayList<>(Arrays.asList(args));
        if (params.contains("--help")) {
            System.err.printf("Usage: java -cp benchmarks.jar %s [--help]|[<regex>]\n" + //
                    "<regex>: regex for included tests (e.g. '.*.' , .*QueryAll.*, etc", //
                    CloneMTT.class.getName());
            System.exit(1);
        }
        params.remove("--help");
        String includes = CloneMTT.class.getPackage().getName() + ".*";
        if (!params.isEmpty()) {
            includes = params.get(0);
        }
        // if (args.length == 0) {
        // System.err.printf(
        // "Usage: java -cp benchmarks.jar <mainclass> <repository URI>\n<mainclass>: %s",
        // CloneMTT.class.getName());
        // System.exit(1);
        // }
        //
        // RepoState.repoURI = URI.create(args[0]);
        // System.err.printf("Running %s tests for repository %s...",
        // CloneMTT.class.getSimpleName(),
        // RepoState.repoURI);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss'Z'", Locale.US);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String filename = String.format("%s-%s.csv", CloneMTT.class.getSimpleName(),
                dateFormat.format(new Date()));

        Options opts = new OptionsBuilder()//
                .include(CloneMTT.class.getSimpleName())//
                // .warmupIterations(0)//
                // .measurementIterations(1)//
                // .jvmArgs("-server")//
                .include(includes)//
                .forks(0)// no forks
                .timeout(TimeValue.days(7)) // don't know how to disable timeout
                .resultFormat(ResultFormatType.CSV)//
                .result(filename)//
                .build();

        Collection<RunResult> records;
        try {
            records = new Runner(opts).run();
            for (RunResult r : records) {
                // r.getPrimaryResult().get
                // System.out.println("API replied benchmark score: " + r.getScore() + " "
                // + r.getScoreUnit() + " over " + r.getStatistics().getN() + " iterations");
            }
        } catch (RunnerException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
