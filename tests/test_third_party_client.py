import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FakeResponse:
    def __init__(self, status_code, payload=None, json_error=None):
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error
        self.text = "response"

    def json(self):
        if self._json_error:
            raise self._json_error
        return self._payload


class ThirdPartyClientTests(unittest.TestCase):
    def test_real_http_batches_and_retries_with_identical_payload(self):
        from third_party_client import ThirdPartyClient

        calls = []
        attempts = {}

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                payload = json.loads(raw.decode("utf-8"))
                request_id = payload["requestId"]
                attempts[request_id] = attempts.get(request_id, 0) + 1
                calls.append(
                    {
                        "path": self.path,
                        "content_type": self.headers.get("Content-Type"),
                        "raw": raw,
                        "payload": payload,
                    }
                )
                if len(attempts) == 1 and attempts[request_id] == 1:
                    self.send_response(500)
                    self.end_headers()
                    return
                response = json.dumps(
                    {
                        "code": 0,
                        "successCount": len(payload["records"]),
                        "failedCount": 0,
                    }
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

            def log_message(self, format, *args):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = ThirdPartyClient(
                {
                    "base_url": f"http://127.0.0.1:{server.server_port}",
                    "batch_size": 100,
                    "timeout": 2,
                    "retry_interval": 0,
                    "max_retries": 3,
                },
                sleep=lambda _: None,
            )
            records = [
                {
                    "operation": "INSERT",
                    "businessKey": {"unid": str(index)},
                    "data": {"unid": str(index)},
                }
                for index in range(205)
            ]
            result = client.push_records("FUND_RECEIPT", records)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertTrue(result["success"])
        self.assertEqual([100, 100, 5], [item["record_count"] for item in result["batches"]])
        self.assertEqual(4, result["attempts"])
        self.assertEqual({"/api/v1/rms/batch-import"}, {item["path"] for item in calls})
        self.assertEqual(
            {"application/json; charset=utf-8"},
            {item["content_type"] for item in calls},
        )
        first_request_id = calls[0]["payload"]["requestId"]
        first_batch_calls = [
            item for item in calls if item["payload"]["requestId"] == first_request_id
        ]
        self.assertEqual(2, len(first_batch_calls))
        self.assertEqual(first_batch_calls[0]["raw"], first_batch_calls[1]["raw"])

    def test_retry_reuses_request_id_and_payload_until_success(self):
        from third_party_client import ThirdPartyClient

        calls = []
        sleeps = []
        responses = [
            FakeResponse(500),
            FakeResponse(200, json_error=ValueError("invalid json")),
            FakeResponse(
                200,
                {
                    "code": 0,
                    "message": "success",
                    "requestId": "ignored-by-test",
                    "successCount": 1,
                    "failedCount": 0,
                    "results": [],
                },
            ),
        ]

        def post(url, json, timeout, headers):
            calls.append(json)
            self.assertEqual("application/json; charset=utf-8", headers["Content-Type"])
            return responses.pop(0)

        client = ThirdPartyClient(
            {
                "base_url": "http://third-party",
                "batch_size": 100,
                "timeout": 60,
                "retry_interval": 3,
                "max_retries": 3,
            },
            post=post,
            sleep=sleeps.append,
        )
        result = client.push_records(
            "CONTRACT",
            [
                {
                    "operation": "INSERT",
                    "businessKey": {"contractId": "C1", "fundId": "F1"},
                    "data": {"contractId": "C1", "fundId": "F1"},
                }
            ],
        )

        self.assertTrue(result["success"])
        self.assertEqual(3, result["attempts"])
        self.assertEqual([3, 3], sleeps)
        self.assertEqual(3, len(calls))
        self.assertEqual(1, len({call["requestId"] for call in calls}))
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(calls[1], calls[2])

    def test_partial_failure_is_retried_four_total_attempts(self):
        from third_party_client import ThirdPartyClient

        calls = []

        def post(url, json, timeout, headers):
            calls.append(json)
            return FakeResponse(
                200,
                {
                    "code": 0,
                    "message": "partial",
                    "requestId": json["requestId"],
                    "successCount": 1,
                    "failedCount": 1,
                    "results": [],
                },
            )

        client = ThirdPartyClient(
            {
                "base_url": "http://third-party",
                "batch_size": 100,
                "timeout": 60,
                "retry_interval": 0,
                "max_retries": 3,
            },
            post=post,
            sleep=lambda _: None,
        )
        records = [
            {"operation": "INSERT", "businessKey": {"unid": "1"}, "data": {}},
            {"operation": "INSERT", "businessKey": {"unid": "2"}, "data": {}},
        ]
        result = client.push_records("FUND_RECEIPT", records)

        self.assertFalse(result["success"])
        self.assertEqual(4, result["attempts"])
        self.assertEqual(4, len(calls))

    def test_boolean_response_counts_are_rejected(self):
        from third_party_client import ThirdPartyClient

        calls = []

        def post(url, json, timeout, headers):
            calls.append(json)
            return FakeResponse(
                200,
                {
                    "code": False,
                    "successCount": True,
                    "failedCount": False,
                },
            )

        client = ThirdPartyClient(
            {
                "base_url": "http://third-party",
                "retry_interval": 0,
                "max_retries": 3,
            },
            post=post,
            sleep=lambda _: None,
        )
        result = client.push_records(
            "FUND_RECEIPT",
            [{"operation": "INSERT", "businessKey": {"unid": "1"}, "data": {}}],
        )

        self.assertFalse(result["success"])
        self.assertEqual(4, len(calls))


if __name__ == "__main__":
    unittest.main()
