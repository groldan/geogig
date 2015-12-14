package org.locationtech.geogig.cli.test.functional.erik;

import org.junit.runner.RunWith;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;

/**
 * Single cucumber test runner. Its sole purpose is to serve as an entry point for junit. Step
 * definitions and hooks are defined in their own classes so they can be reused across features.
 *
 */
@RunWith(Cucumber.class)
@CucumberOptions(//
    monochrome = true//
    , format = {"pretty", "html:target/cucumber-report"}//
    , strict = true//
    , glue = {"org.locationtech.geogig.cli.test.functional.general"}// where step definitions are
//, features = { "src/test/resources/org/locationtech/geogig/cli/test/functional/porcelain/Clean.feature" }//
)
public class ErikTest {
}
