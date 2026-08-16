package dev.engnotes.fes.corporateactionproducer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topic destination topic. The workload identity for this service is authorised to write
 *              this topic and nothing else, so changing it requires an IAM policy change too.
 */
@ConfigurationProperties(prefix = "fes.corporate-action-producer")
public record CorporateActionProducerProperties(@DefaultValue("corporate-actions") String topic) {
}
