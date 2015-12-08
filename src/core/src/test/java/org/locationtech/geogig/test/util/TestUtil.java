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
     * System property set by Maven to enable large database tests.
     */
    private static final String LARGE_DB_TESTS_PROP = "geogig.large.db.tests";
    /**
     * System property set by Maven to enable large Node Index tests.
     */
    private static final String LARGE_NODE_TESTS_PROP = "geogig.large.node.tests";
    /**
     * System property set by Maven to enable large Point Cache tests.
     */
    private static final String LARGE_CACHE_TESTS_PROP = "geogig.large.cache.tests";
    /**
     * Enabled value.
     */
    private static final String ENABLED = "enabled";
    /**
     * Disabled value. Large tests are disabled by default.
     */
    private static final String DISABLED = "disabled";
    private static final String DEFAULT = DISABLED;

    /**
     * Flag for enabling or skipping "large" database tests. A large test is one that might take a
     * long time to run and/or is very demanding on resources (CPU, RAM, Disk or some combination).
     * Large tests are disabled by default. To enabled them through Maven, run Maven with the
     * "runLargeDbTests" profile activated. Example:
     *
     * mvn clean install -PrunLargeDbTests
     */
    private static final boolean RUN_LARGE_DB_TESTS = ENABLED.equals(System.getProperty(
        LARGE_DB_TESTS_PROP, DEFAULT));

    /**
     * Flag for enabling or skipping "large" Node Index tests. A large test is one that might take a
     * long time to run and/or is very demanding on resources (CPU, RAM, Disk or some combination).
     * Large tests are disabled by default. To enabled them through Maven, run Maven with the
     * "runLargeNodeTests" profile activated. Example:
     *
     * mvn clean install -PrunLargeNodeTests
     */
    private static final boolean RUN_LARGE_NODE_TESTS = ENABLED.equals(System.getProperty(
        LARGE_NODE_TESTS_PROP, DEFAULT));

    /**
     * Flag for enabling or skipping "large" Point Cache tests. A large test is one that might take
     * a long time to run and/or is very demanding on resources (CPU, RAM, Disk or some
     * combination). Large tests are disabled by default. To enabled them through Maven, run Maven
     * with the "runLargeCacheTests" profile activated. Example:
     *
     * mvn clean install -PrunLargeCacheTests
     */
    private static final boolean RUN_LARGE_CACHE_TESTS = ENABLED.equals(System.getProperty(
        LARGE_CACHE_TESTS_PROP, DEFAULT));

    /**
     * Utility method for tests to use to determine if they should run large DB tests. Instead of
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
     *     Assume.assumeTrue(TestUtil.isRunLargeDbTests());
     *     ...
     * }}
     *
     * The above test would run only if TestUtil.isRunLargeDbTest() returns true. To keep from
     * skewing test result however, the above test would report as "skipped" if
     * TestUtil.isRunLargeTests() returns false.
     *
     * @return True if large DB tests should be run, false otherwise.
     */
    public static boolean isRunLargeDbTests () {
        return TestUtil.RUN_LARGE_DB_TESTS;
    }

    /**
     * Utility method for tests to use to determine if they should run large Node Index tests.
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
     *     Assume.assumeTrue(TestUtil.isRunLargeNodeTests());
     *     ...
     * }}
     *
     * The above test would run only if TestUtil.isRunLargeNodeTest() returns true. To keep from
     * skewing test result however, the above test would report as "skipped" if
     * TestUtil.isRunLargeNodeTests() returns false.
     *
     * @return True if large Node Index tests should be run, false otherwise.
     */
    public static boolean isRunLargeNodeTests () {
        return TestUtil.RUN_LARGE_NODE_TESTS;
    }

    /**
     * Utility method for tests to use to determine if they should run large Point Cache tests.
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
     *     Assume.assumeTrue(TestUtil.isRunLargeCacheTests());
     *     ...
     * }}
     *
     * The above test would run only if TestUtil.isRunLargeCacheTest() returns true. To keep from
     * skewing test result however, the above test would report as "skipped" if
     * TestUtil.isRunLargeCacheTests() returns false.
     *
     * @return True if large Point Cache tests should be run, false otherwise.
     */
    public static boolean isRunLargeCacheTests () {
        return TestUtil.RUN_LARGE_CACHE_TESTS;
    }
}
