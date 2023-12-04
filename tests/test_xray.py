import unittest
import json
import os

from unittest.mock import MagicMock, patch

from ddtrace.context import Context
from datadog_lambda.constants import XraySubsegment

from datadog_lambda.xray import (
    get_xray_host_port,
    build_segment_payload,
    build_segment,
    parse_xray_header,
    send_segment,
)


class TestXRay(unittest.TestCase):
    def tearDown(self):
        if os.environ.get("_X_AMZN_TRACE_ID"):
            os.environ.pop("_X_AMZN_TRACE_ID")
        if os.environ.get("AWS_XRAY_DAEMON_ADDRESS"):
            os.environ.pop("AWS_XRAY_DAEMON_ADDRESS")
        return super().tearDown()

    def test_get_xray_host_port_empty_(self):
        result = get_xray_host_port("")
        self.assertIsNone(result)

    def test_get_xray_host_port_invalid_value(self):
        result = get_xray_host_port("myVar")
        self.assertIsNone(result)

    def test_get_xray_host_port_success(self):
        result = get_xray_host_port("mySuperHost:1000")
        self.assertEqual("mySuperHost", result[0])
        self.assertEqual(1000, result[1])

    def test_send_segment_sampled_out(self):
        os.environ["AWS_XRAY_DAEMON_ADDRESS"] = "fake-agent.com:8080"
        os.environ[
            "_X_AMZN_TRACE_ID"
        ] = "Root=1-5e272390-8c398be037738dc042009320;Parent=94ae789b969f1cc5;Sampled=0;Lineage=c6c5b1b9:0"

        with patch(
            "datadog_lambda.xray.send", MagicMock(return_value=None)
        ) as mock_send:
            # XRay trace won't be sampled according to the trace header.
            send_segment("my_key", {"data": "value"})
            self.assertFalse(mock_send.called)

    def test_send_segment_sampled(self):
        os.environ["AWS_XRAY_DAEMON_ADDRESS"] = "fake-agent.com:8080"
        os.environ[
            "_X_AMZN_TRACE_ID"
        ] = "Root=1-5e272390-8c398be037738dc042009320;Parent=94ae789b969f1cc5;Sampled=1;Lineage=c6c5b1b9:0"
        with patch(
            "datadog_lambda.xray.send", MagicMock(return_value=None)
        ) as mock_send:
            # X-Ray trace will be sampled according to the trace header.
            send_segment("my_key", {"data": "value"})
            self.assertTrue(mock_send.called)

    def test_build_segment_payload_ok(self):
        exected_text = '{"format": "json", "version": 1}\nmyPayload'
        self.assertEqual(exected_text, build_segment_payload("myPayload"))

    def test_build_segment_payload_no_payload(self):
        self.assertIsNone(build_segment_payload(None))

    @patch("time.time", MagicMock(return_value=1111))
    @patch(
        "datadog_lambda.xray.generate_random_id",
        MagicMock(return_value="1234abcd"),
    )
    def test_build_segment(self):
        context = {
            "trace_id": 111000111,
            "parent_id": 222000222,
        }

        value = json.dumps({"a": "aaa", "b": "bbb"})
        result = build_segment(context, "myKey", "myValue")
        jsonResult = json.loads(result)
        metadataJson = jsonResult["metadata"]

        self.assertEqual("1234abcd", jsonResult["id"])
        self.assertEqual(1111, jsonResult["start_time"])
        self.assertEqual(1111, jsonResult["end_time"])
        self.assertEqual(111000111, jsonResult["trace_id"])
        self.assertEqual(222000222, jsonResult["parent_id"])
        self.assertEqual("datadog-metadata", jsonResult["name"])
        self.assertEqual("subsegment", jsonResult["type"])
        self.assertEqual("myValue", metadataJson["datadog"]["myKey"])

    def test_replicate_runtime_error(self):
        xray_context = parse_xray_header("Root=1-656d4445-0060ba5279ca50be0db44474;Parent=06d7ef844a0c5c64;Sampled=1")
        dd_context = Context(
            trace_id=1894532486775893021,
            span_id=8491654256788888308,
            meta={
                "traceparent": "00-00000000000000001a4abb4251b6ec1d-75d86a8e4b55daf4-01",
                "tracestate": "dd=s:1;t.dm:-1",
                "_dd.p.dm": "-1",
            },
            metrics={"_sampling_priority_v1": 1},
            span_links=[],
        )
        build_segment(xray_context, XraySubsegment.TRACE_KEY, dd_context)
