import unittest


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

        def post(url, json, timeout):
            calls.append(json)
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

        def post(url, json, timeout):
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


if __name__ == "__main__":
    unittest.main()
