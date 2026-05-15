import argparse
import unittest
from pathlib import Path

from benchmarks import nsq_benchmark as bench


class BenchmarkUnitTest(unittest.TestCase):

    def test_address_and_scenario_parsing(self):
        self.assertEqual(
            bench.parse_addresses(
                'tcp://127.0.0.1:4150,localhost', 4150),
            (
                bench.Address('127.0.0.1', 4150),
                bench.Address('localhost', 4150),
            ),
        )
        self.assertEqual(bench.parse_scenarios('pub,mpub'),
                         ('pub', 'mpub'))
        self.assertEqual(bench.parse_scenarios('all'), bench.ALL_SCENARIOS)
        with self.assertRaises(argparse.ArgumentTypeError):
            bench.parse_scenarios('pub,bad')

    def test_payload_round_trip_and_percentiles(self):
        payload = bench.payload_factory(64)(42)
        seq, sent_ns = bench.parse_payload(payload)
        self.assertEqual(seq, 42)
        self.assertGreater(sent_ns, 0)
        with self.assertRaises(ValueError):
            bench.payload_factory(8)
        with self.assertRaises(ValueError):
            bench.parse_payload(b'too-small')

        values = bench.array('Q', [10, 20, 30, 40])
        self.assertEqual(bench.percentile_ns(values, 50), 0.00002)
        self.assertEqual(bench.percentile_ns(values, 95), 0.00004)

    def test_markdown_and_json_report_shape(self):
        config = bench.BenchmarkConfig(
            profile='quick',
            run_id='abc12345',
            nsqd_tcp_addresses=(bench.Address('127.0.0.1', 4150),),
            nsqd_http_addresses=(bench.Address('127.0.0.1', 4151),),
            scenarios=('pub',),
            messages=100,
            payload_size=64,
            concurrency=8,
            batch_size=10,
            max_in_flight=16,
            graceful_messages=4,
            output_buffer_timeout_ms=25,
            writer_connections=1,
            consumer_processes=1,
            timeout=5.0,
            warmup_messages=0,
            snappy=False,
            deflate=False,
            tls_v1=False,
            cleanup=True,
            markdown_path=Path('benchmark.md'),
            json_path=None,
        )
        result = bench.make_result(
            'TCP PUB ack', 100, 64, None, 8, 0.5,
            bench.array('Q', [1_000_000, 2_000_000]), 0,
            'per-message publish ACK latency',
        )

        markdown = bench.markdown_report(config, [result])
        self.assertIn('# asyncnsq Benchmark Report', markdown)
        self.assertIn('| TCP PUB ack |', markdown)
        self.assertIn('| 0 | per-message publish ACK latency |', markdown)

        payload = bench.json_report(config, [result])
        self.assertEqual(payload['run_id'], 'abc12345')
        self.assertEqual(payload['results'][0]['errors'], 0)

    def test_resolve_config_uses_profile_defaults(self):
        parser = bench.build_parser()
        args = parser.parse_args(['--profile', 'quick'])
        config = bench.resolve_config(args)

        self.assertEqual(config.messages, bench.PROFILES['quick']['messages'])
        self.assertEqual(config.scenarios, bench.ALL_SCENARIOS)
        self.assertEqual(config.writer_connections,
                         len(config.nsqd_tcp_addresses))


if __name__ == '__main__':
    unittest.main()
