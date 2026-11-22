# Behaviour catalogue

This catalogue is generated from the test names by `make docs`; do not edit it by hand. Each line is a rule the test suite proves, grouped by application and capability.

## agent

### describe machine

- Description reports static info and when it was observed ([test_machine_describer.py:9](../tests/agent/describe_machine/test_machine_describer.py#L9))

### sample metrics

- Metrics are built from system stats ([test_metrics_sampler.py:9](../tests/agent/sample_metrics/test_metrics_sampler.py#L9))

## anomaly_detector

### detectors

- Ewma has an upper band only ([test_ewma.py:9](../tests/anomaly_detector/detectors/test_ewma.py#L9))
- Ewma flags a spike above the upper band ([test_ewma.py:17](../tests/anomaly_detector/detectors/test_ewma.py#L17))
- Ewma does not flag a drop ([test_ewma.py:24](../tests/anomaly_detector/detectors/test_ewma.py#L24))
- Ewma refuses to score until it has enough values ([test_ewma.py:28](../tests/anomaly_detector/detectors/test_ewma.py#L28))
- Default floor does not hide a tiny rise in bytes ([test_ewma.py:33](../tests/anomaly_detector/detectors/test_ewma.py#L33))
- Relative floor ignores rises below it ([test_ewma.py:37](../tests/anomaly_detector/detectors/test_ewma.py#L37))
- Flat series without a floor flags only a rise ([test_ewma.py:45](../tests/anomaly_detector/detectors/test_ewma.py#L45))
- Upper band is at least the floor above the average ([test_ewma.py:53](../tests/anomaly_detector/detectors/test_ewma.py#L53))
- Average is seeded with the first value ([test_ewma.py:59](../tests/anomaly_detector/detectors/test_ewma.py#L59))
- Flat series with a small floor ignores a rise below it ([test_ewma.py:65](../tests/anomaly_detector/detectors/test_ewma.py#L65))
- Reading exactly at the floor is not an anomaly ([test_ewma.py:72](../tests/anomaly_detector/detectors/test_ewma.py#L72))
- An anomaly after warmup only nudges the average ([test_ewma.py:76](../tests/anomaly_detector/detectors/test_ewma.py#L76))
- A reading exactly at the threshold is not damped ([test_ewma.py:83](../tests/anomaly_detector/detectors/test_ewma.py#L83))
- Value close to the median is not an anomaly ([test_mad.py:13](../tests/anomaly_detector/detectors/test_mad.py#L13))
- Clear outlier is an anomaly ([test_mad.py:17](../tests/anomaly_detector/detectors/test_mad.py#L17))
- Moderate deviation below the threshold is not an anomaly ([test_mad.py:21](../tests/anomaly_detector/detectors/test_mad.py#L21))
- Flags a drop as well as a spike ([test_mad.py:25](../tests/anomaly_detector/detectors/test_mad.py#L25))
- Band is symmetric around the median ([test_mad.py:29](../tests/anomaly_detector/detectors/test_mad.py#L29))
- Band spans the threshold in modified z scores ([test_mad.py:38](../tests/anomaly_detector/detectors/test_mad.py#L38))
- Mad of zero falls back to scaled mean absolute deviation ([test_mad.py:47](../tests/anomaly_detector/detectors/test_mad.py#L47))
- Mad of zero with last value on the median is not an anomaly ([test_mad.py:55](../tests/anomaly_detector/detectors/test_mad.py#L55))
- Mad of zero with last value off the median is an anomaly ([test_mad.py:59](../tests/anomaly_detector/detectors/test_mad.py#L59))
- Band collapses to the median when every value is equal ([test_mad.py:63](../tests/anomaly_detector/detectors/test_mad.py#L63))
- Min deviation is the smallest deviation that reaches the threshold ([test_mad.py:70](../tests/anomaly_detector/detectors/test_mad.py#L70))
- Min relative floors the band at a fraction of the median ([test_mad.py:79](../tests/anomaly_detector/detectors/test_mad.py#L79))
- Floor does not narrow a wider spread ([test_mad.py:87](../tests/anomaly_detector/detectors/test_mad.py#L87))
- Floor gives equal values a band ([test_mad.py:91](../tests/anomaly_detector/detectors/test_mad.py#L91))

### score samples

- Every detector evaluates every sample once warm ([test_metric_evaluator.py:31](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L31))
- Evaluation is keyed by the metric sample ([test_metric_evaluator.py:38](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L38))
- Mad flags a spike and reports its band ([test_metric_evaluator.py:46](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L46))
- Mad flags a sudden drop ([test_metric_evaluator.py:63](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L63))
- Ewma has no lower band ([test_metric_evaluator.py:67](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L67))
- Detectors are skipped until they have enough values ([test_metric_evaluator.py:74](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L74))
- Cpu moving less than ten points on an idle machine is not an anomaly ([test_metric_evaluator.py:81](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L81))
- Cpu rise of more than ten points on an idle machine is an anomaly ([test_metric_evaluator.py:88](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L88))
- Memory moving a few pages is not an anomaly ([test_metric_evaluator.py:95](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L95))
- Memory jumping more than two percent is an anomaly ([test_metric_evaluator.py:103](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L103))
- Nab series are evaluated without floors ([test_metric_evaluator.py:110](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L110))
- Flat nab series is scored without an error ([test_metric_evaluator.py:119](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L119))
- Each detectors name and duration is reported to the observer ([test_metric_evaluator.py:126](../tests/anomaly_detector/score_samples/test_metric_evaluator.py#L126))
- Only metrics with detectors are scored ([test_scoring_policy.py:11](../tests/anomaly_detector/score_samples/test_scoring_policy.py#L11))
- Cpu and memory are each scored by mad and ewma ([test_scoring_policy.py:16](../tests/anomaly_detector/score_samples/test_scoring_policy.py#L16))
- Unscored metric has no detectors ([test_scoring_policy.py:21](../tests/anomaly_detector/score_samples/test_scoring_policy.py#L21))
- Nab series are scored by detectors without floors ([test_scoring_policy.py:25](../tests/anomaly_detector/score_samples/test_scoring_policy.py#L25))
- First value starts a window of one ([test_series_window.py:4](../tests/anomaly_detector/score_samples/test_series_window.py#L4))
- Newest value is last ([test_series_window.py:8](../tests/anomaly_detector/score_samples/test_series_window.py#L8))
- Window below the limit keeps every value ([test_series_window.py:12](../tests/anomaly_detector/score_samples/test_series_window.py#L12))
- Window keeps only the last 360 values ([test_series_window.py:18](../tests/anomaly_detector/score_samples/test_series_window.py#L18))
- Window is counted in values not in time ([test_series_window.py:27](../tests/anomaly_detector/score_samples/test_series_window.py#L27))
- Previous values are not modified ([test_series_window.py:31](../tests/anomaly_detector/score_samples/test_series_window.py#L31))

## clickhouse_writer

- Main runs every sink and stops them all when one fails ([test_main.py:8](../tests/clickhouse_writer/test_main.py#L8))

### adapters

- Insert batch inserts rows once into the sink table ([test_clickhouse_repository.py:19](../tests/clickhouse_writer/adapters/test_clickhouse_repository.py#L19))
- Insert batch skips empty batch ([test_clickhouse_repository.py:28](../tests/clickhouse_writer/adapters/test_clickhouse_repository.py#L28))
- Insert batch retries with backoff then succeeds ([test_clickhouse_repository.py:36](../tests/clickhouse_writer/adapters/test_clickhouse_repository.py#L36))
- Insert batch raises after exhausting retries ([test_clickhouse_repository.py:47](../tests/clickhouse_writer/adapters/test_clickhouse_repository.py#L47))
- Consumer subscribes to its topic in its group ([test_kafka_records_consumer.py:8](../tests/clickhouse_writer/adapters/test_kafka_records_consumer.py#L8))
- Poll returns the records of every partition ([test_kafka_records_consumer.py:16](../tests/clickhouse_writer/adapters/test_kafka_records_consumer.py#L16))

### load topics into tables

- Evaluation row matches columns and keeps a missing band as null ([test_clickhouse_evaluations_sink.py:11](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_evaluations_sink.py#L11))
- Evaluation values are stored exactly as sent ([test_clickhouse_evaluations_sink.py:25](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_evaluations_sink.py#L25))
- Parse skips invalid evaluations ([test_clickhouse_evaluations_sink.py:33](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_evaluations_sink.py#L33))
- Machine row converts both times to utc ([test_clickhouse_machines_sink.py:10](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_machines_sink.py#L10))
- Parse skips invalid machines ([test_clickhouse_machines_sink.py:23](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_machines_sink.py#L23))
- Metric row matches columns ([test_clickhouse_metrics_sink.py:8](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_metrics_sink.py#L8))
- Missing attributes are stored as an empty map ([test_clickhouse_metrics_sink.py:21](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_metrics_sink.py#L21))
- Clickhouse sink base is abstract ([test_clickhouse_sink_base.py:25](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L25))
- Columns are the record fields ([test_clickhouse_sink_base.py:31](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L31))
- Each sink has its own group ([test_clickhouse_sink_base.py:35](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L35))
- Topics follow the producers environment ([test_clickhouse_sink_base.py:41](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L41))
- Parse ignores unknown fields ([test_clickhouse_sink_base.py:52](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L52))
- Parse skips records without a valid metric id ([test_clickhouse_sink_base.py:61](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L61))
- Parse skips malformed records ([test_clickhouse_sink_base.py:67](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L67))
- Run flushes when the batch is full ([test_clickhouse_sink_base.py:96](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L96))
- Run flushes on timeout before batch is full ([test_clickhouse_sink_base.py:108](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L108))
- Run does not flush empty batch ([test_clickhouse_sink_base.py:120](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L120))
- Run does not flush when every record is malformed ([test_clickhouse_sink_base.py:131](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L131))
- Stop flushes pending batch ([test_clickhouse_sink_base.py:142](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L142))
- Flush does not commit offsets when the write fails ([test_clickhouse_sink_base.py:157](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L157))
- Flush commits offsets only after the write ([test_clickhouse_sink_base.py:167](../tests/clickhouse_writer/load_topics_into_tables/test_clickhouse_sink_base.py#L167))

## collector

### read agent list

- Metrics url is under the agent url ([test_agent_urls.py:4](../tests/collector/read_agent_list/test_agent_urls.py#L4))
- Agents are read in order ([test_agents_file_loader.py:14](../tests/collector/read_agent_list/test_agents_file_loader.py#L14))
- The repo default is valid ([test_agents_file_loader.py:19](../tests/collector/read_agent_list/test_agents_file_loader.py#L19))
- Missing file raises os error ([test_agents_file_loader.py:23](../tests/collector/read_agent_list/test_agents_file_loader.py#L23))
- Invalid file raises a value error naming the problem ([test_agents_file_loader.py:41](../tests/collector/read_agent_list/test_agents_file_loader.py#L41))

### scrape agents

- Each agent metric becomes its own message ([test_collector.py:50](../tests/collector/scrape_agents/test_collector.py#L50))
- Each published metric gets its own uuid7 ([test_collector.py:58](../tests/collector/scrape_agents/test_collector.py#L58))
- Malformed response is skipped ([test_collector.py:66](../tests/collector/scrape_agents/test_collector.py#L66))
- Metric keeps the timestamp it was sampled at ([test_collector.py:73](../tests/collector/scrape_agents/test_collector.py#L73))
- Machine url is next to the metrics url ([test_collector.py:80](../tests/collector/scrape_agents/test_collector.py#L80))
- Machine info is published on first scrape ([test_collector.py:84](../tests/collector/scrape_agents/test_collector.py#L84))
- Machine info is refreshed only after the interval ([test_collector.py:96](../tests/collector/scrape_agents/test_collector.py#L96))
- Failed machine publish is retried on next scrape and metrics still flow ([test_collector.py:111](../tests/collector/scrape_agents/test_collector.py#L111))
- Agent without machine endpoint still has its metrics collected ([test_collector.py:121](../tests/collector/scrape_agents/test_collector.py#L121))
- Invalid metrics and machine are counted and dead lettered as received ([test_collector.py:149](../tests/collector/scrape_agents/test_collector.py#L149))
- An unreachable agent is not dead lettered ([test_collector.py:164](../tests/collector/scrape_agents/test_collector.py#L164))
- A failed dead letter does not stop the scrape ([test_collector.py:170](../tests/collector/scrape_agents/test_collector.py#L170))

### split snapshots

- Metrics are parsed from the agent response ([test_metrics_parser.py:15](../tests/collector/split_snapshots/test_metrics_parser.py#L15))
- Malformed metrics raise a value error ([test_metrics_parser.py:25](../tests/collector/split_snapshots/test_metrics_parser.py#L25))
- A body that is not json raises a value error ([test_metrics_parser.py:30](../tests/collector/split_snapshots/test_metrics_parser.py#L30))

## contracts

- Malformed machine raises a value error ([test_machine.py:13](../tests/contracts/test_machine.py#L13))
- A body that is not json raises a value error ([test_machine.py:18](../tests/contracts/test_machine.py#L18))

## whole system

- Behaviour catalogue is up to date ([test_behaviour_catalogue.py:14](../tests/test_behaviour_catalogue.py#L14))
- A test name becomes a sentence ([test_behaviour_catalogue.py:20](../tests/test_behaviour_catalogue.py#L20))
- Tests are grouped by app and capability ([test_behaviour_catalogue.py:26](../tests/test_behaviour_catalogue.py#L26))
- Runtime entrypoints import ([test_metrics_schema.py:1](../tests/test_metrics_schema.py#L1))
