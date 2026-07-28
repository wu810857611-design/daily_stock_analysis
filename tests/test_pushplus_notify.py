import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.pushplus_notify import PUSHPLUS_ENDPOINT, PushPlusError, build_payload, main, send_markdown


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode()


class PushPlusNotifyTests(unittest.TestCase):
    def test_payload_uses_markdown_and_optional_topic(self):
        payload = build_payload("secret", "title", "body", "group")
        self.assertEqual(payload["template"], "markdown")
        self.assertEqual(payload["topic"], "group")

    @patch("scripts.pushplus_notify.request.urlopen")
    def test_sends_to_https_endpoint(self, urlopen):
        urlopen.return_value = _Response({"code": 200, "msg": "success"})
        result = send_markdown(token="secret", title="title", content="body")
        outgoing = urlopen.call_args.args[0]
        self.assertEqual(outgoing.full_url, PUSHPLUS_ENDPOINT)
        self.assertTrue(outgoing.full_url.startswith("https://"))
        self.assertEqual(result["code"], 200)

    @patch("scripts.pushplus_notify.request.urlopen")
    def test_rejected_message_raises(self, urlopen):
        urlopen.return_value = _Response({"code": 500, "msg": "denied"})
        with self.assertRaisesRegex(PushPlusError, "denied"):
            send_markdown(token="secret", title="title", content="body")

    def test_cli_without_token_keeps_generated_report(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.md"
            report.write_text("# report", encoding="utf-8")
            with patch.dict("os.environ", {}, clear=True), patch("sys.stdout", new=io.StringIO()):
                self.assertEqual(main(["--file", str(report), "--title", "title"]), 0)


if __name__ == "__main__":
    unittest.main()
