/* Copyright (c) 2015 Boundless.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Erik Merkle (Boundless) - initial implementation
 */
package org.locationtech.geogig.test.util;

/**
 * Utility class for Tests.
 */
public class TestUtil {

    /**
     * System property set by Maven to enable database stress tests.
     */
    private static final String DB_STRESS_TESTS_PROP = "db.stress.tests";
    /**
     * System property set by Maven to enable Node Index stress tests.
     */
    private static final String NODE_STRESS_TESTS_PROP = "node.stress.tests";
    /**
     * System property set by Maven to enable Point Cache stress tests.
     */
    private static final String CACHE_STRESS_TESTS_PROP = "cache.stress.tests";
    /**
     * Enabled value.
     */
    private static final String ENABLED = "enabled";
    /**
     * Disabled value. Stress tests are disabled by default.
     */
    private static final String DISABLED = "disabled";
    private static final String DEFAULT = DISABLED;

    /**
     * Flag for enabling or skipping database stress tests. A stress test is one that might take a
     * long time to run and/or is very demanding on resources (CPU, RAM, Disk or some combination).
     * Stress tests are disabled by default. To enabled them through Maven, run Maven with the
     * "dbStressTests" profile activated. Example:
     *
     * mvn clean install -PdbStressTests
     */
    private static final boolean RUN_DB_STRESS_TESTS = ENABLED.equals(System.getProperty(
        DB_STRESS_TESTS_PROP, DEFAULT));

    /**
     * Flag for enabling or skipping Node Index stress tests. A stress test is one that might take a
     * long time to run and/or is very demanding on resources (CPU, RAM, Disk or some combination).
     * Stress tests are disabled by default. To enabled them through Maven, run Maven with the
     * "nodeStressTests" profile activated. Example:
     *
     * mvn clean install -PnodeStressTests
     */
    private static final boolean RUN_NODE_STRESS_TESTS = ENABLED.equals(System.getProperty(
        NODE_STRESS_TESTS_PROP, DEFAULT));

    /**
     * Flag for enabling or skipping Point Cache stress tests. A stress test is one that might take
     * a long time to run and/or is very demanding on resources (CPU, RAM, Disk or some
     * combination). Stress tests are disabled by default. To enabled them through Maven, run Maven
     * with the "cacheStressTests" profile activated. Example:
     *
     * mvn clean install -PcacheStressTests
     */
    private static final boolean RUN_CACHE_STRESS_TESTS = ENABLED.equals(System.getProperty(
        CACHE_STRESS_TESTS_PROP, DEFAULT));

    /**
     * Utility method for tests to use to determine if they should run DB stress tests. Instead of
     * using {@link org.junit.Ignore org.junit.Ignore} annotations on tests that may be inconvenient
     * to run normally, those tests can make a call to
     * {@link org.junit.Assume#assumeTrue(boolean) org.junit.Assume.assumeTrue(boolean)} with the
     * return value of this method as the argument. If this method returns false,
     * Assume.assumeTrue() will <b>skip</b> the test in which it is called, just as @Ignore would
     * do.
     *
     * Example:
     *
     * {@code
     * @Test
     * public void myTest() {
     *     Assume.assumeTrue(TestUtil.isDbStressTests());
     *     ...
     * }}
     *
     * The above test would run only if TestUtil.isDbStressTest() returns true. To keep from
     * skewing test result however, the above test would report as "skipped" if
     * TestUtil.isDbStressTests() returns false.
     *
     * @return True if DB stress tests should be run, false otherwise.
     */
    public static boolean isDbStressTests () {
        return TestUtil.RUN_DB_STRESS_TESTS;
    }

    /**
     * Utility method for tests to use to determine if they should run Node Index stress tests.
     * Instead of using {@link org.junit.Ignore org.junit.Ignore} annotations on tests that may be
     * inconvenient to run normally, those tests can make a call to
     * {@link org.junit.Assume#assumeTrue(boolean) org.junit.Assume.assumeTrue(boolean)} with the
     * return value of this method as the argument. If this method returns false,
     * Assume.assumeTrue() will <b>skip</b> the test in which it is called, just as @Ignore would
     * do.
     *
     * Example:
     *
     * {@code
     * @Test
     * public void myTest() {
     *     Assume.assumeTrue(TestUtil.isNodeStressTests());
     *     ...
     * }}
     *
     * The above test would run only if TestUtil.isNodeStressTests() returns true. To keep from
     * skewing test result however, the above test would report as "skipped" if
     * TestUtil.isNodeStressTests() returns false.
     *
     * @return True if Node Index stress tests should be run, false otherwise.
     */
    public static boolean isNodeStressTests () {
        return TestUtil.RUN_NODE_STRESS_TESTS;
    }

    /**
     * Utility method for tests to use to determine if they should run Point Cache stress tests.
     * Instead of using {@link org.junit.Ignore org.junit.Ignore} annotations on tests that may be
     * inconvenient to run normally, those tests can make a call to
     * {@link org.junit.Assume#assumeTrue(boolean) org.junit.Assume.assumeTrue(boolean)} with the
     * return value of this method as the argument. If this method returns false,
     * Assume.assumeTrue() will <b>skip</b> the test in which it is called, just as @Ignore would
     * do.
     *
     * Example:
     *
     * {@code
     * @Test
     * public void myTest() {
     *     Assume.assumeTrue(TestUtil.isCacheStressTests());
     *     ...
     * }}
     *
     * The above test would run only if TestUtil.isCacheStressTests() returns true. To keep from
     * skewing test result however, the above test would report as "skipped" if
     * TestUtil.isCacheStressTests() returns false.
     *
     * @return True if Point Cache stress tests should be run, false otherwise.
     */
    public static boolean isCacheStressTests () {
        return TestUtil.RUN_CACHE_STRESS_TESTS;
    }
}
