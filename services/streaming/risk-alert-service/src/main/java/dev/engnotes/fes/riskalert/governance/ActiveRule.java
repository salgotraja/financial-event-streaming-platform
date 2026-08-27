package dev.engnotes.fes.riskalert.governance;

import java.util.Map;

/**
 * A governed rule version resolved as in force for one instant, with the parameters that version
 * carried. {@code version} is 0 for the ungoverned bootstrap rule from {@code application.yml}.
 */
public record ActiveRule(String ruleId, String ruleType, long version, Map<String, String> parameters) {
}
