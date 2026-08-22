import json
import os
import tempfile

from time import time
from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient

from neon_data_models.enum import NotificationPermission, NotificationScope
from neon_data_models.models.base.notifications import Notification
from neon_data_models.models.user import User
from neon_data_models.models.user.database import PermissionsConfig

_REGISTRY_DIR = tempfile.TemporaryDirectory()

_TEST_CONFIG = {
    "node_registry_path": os.path.join(_REGISTRY_DIR.name, "node_registry.json"),
    "mq_default_timeout": 10,
    "access_token_ttl": 86400,  # 1 day
    "refresh_token_ttl": 604800,  # 1 week
    # The whole suite shares one `testclient` rate-limit bucket, so this is
    # set well above the suite's request count rather than at a realistic
    # per-client limit
    "requests_per_minute": 1000,
    "auth_requests_per_minute": 1000,
    "access_token_secret": "a800445648142061fc238d1f84e96200da87f4f9f784108ac90db8b4391b117b",
    "refresh_token_secret": "833d369ac73d883123743a44b4a7fe21203cffc956f4c8a99be6e71aafa8e1aa",
    "server_host": "0.0.0.0",
    "server_port": 8080,
    "fastapi_title": "Test Client Title",
    "fastapi_summary": "Test Client Summary",
    "stt_max_length_encoded": 500000,
    "tts_max_words": 128,
    "enable_email": True
}


class TestHanaApp(TestCase):
    test_app: TestClient = None
    tokens: dict = None

    @classmethod
    @patch("ovos_config.config.Configuration")
    @patch("neon_hana.mq_websocket_api.MQWebsocketAPI")
    def setUpClass(cls, ws_api, config):
        config.return_value = {"hana": _TEST_CONFIG}
        from neon_hana.app import create_app
        app = create_app(_TEST_CONFIG)
        cls.test_app = TestClient(app)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def _get_tokens(self, send_request):
        valid_user = User(username="guest", password_hash="password")
        send_request.return_value = {"user": valid_user.model_dump(),
                                     "success": True}
        if not self.tokens:
            response = self.test_app.post("/auth/login",
                                          json={"username": "guest",
                                                "password": "password"})
            self.tokens = response.json()
            self.assertIn("access_token", self.tokens, self.tokens)
        return self.tokens

    @patch("neon_hana.mq_service_api.send_mq_request")
    def _get_admin_tokens(self, send_request):
        admin_user = User(
            username="admin", password_hash="password",
            permissions=PermissionsConfig(hub=30, core=20, diana=20,
                                         node=20, llm=20))
        send_request.return_value = {"user": admin_user.model_dump(),
                                     "success": True}
        response = self.test_app.post("/auth/login",
                                      json={"username": "admin",
                                            "password": "password"})
        tokens = response.json()
        self.assertIn("access_token", tokens, tokens)
        return tokens

    def test_app_init(self):
        self.assertEqual(self.test_app.app.title, _TEST_CONFIG["fastapi_title"])
        self.assertEqual(self.test_app.app.summary,
                         _TEST_CONFIG["fastapi_summary"])

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_auth_login(self, send_request):
        valid_user = User(username="guest", password_hash="password")
        send_request.return_value = {"user": valid_user.model_dump(),
                                     "success": True}

        # Valid Login
        response = self.test_app.post("/auth/login",
                                      json={"username": valid_user.username,
                                            "password": valid_user.password_hash})
        response_data = response.json()
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response_data['username'], "guest")
        self.assertIsInstance(response_data['access_token'], str)
        self.assertIsInstance(response_data['refresh_token'], str)
        self.assertGreater(response_data['expiration'], time())

        # Invalid Login
        send_request.return_value = {"code": 404, "error": "User not found"}
        response = self.test_app.post("/auth/login",
                                      json={"username": valid_user.username,
                                            "password": valid_user.password_hash})
        self.assertEqual(response.status_code, 404, response.status_code)
        self.assertEqual(response.json()['detail'],
                         "User not found", response.text)

        # Invalid Request
        self.assertEqual(self.test_app.post("/auth/login").status_code, 422)
        self.assertEqual(self.test_app.post("/auth/login",
                                            json={"username": None}).status_code,
                         422)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_auth_login_node_auth(self, send_request):
        admin_user = User(
            username="admin", password_hash="password",
            permissions=PermissionsConfig(hub=30, core=20, diana=20,
                                         node=20, llm=20))
        send_request.return_value = {"user": admin_user.model_dump(),
                                     "success": True}
        response = self.test_app.post("/auth/login",
                                      json={"username": "admin",
                                            "password": "password",
                                            "node_auth": True})
        self.assertEqual(response.status_code, 200, response.text)
        token = response.json()["access_token"]

        # Verify token has NODE permissions, not the user's ADMIN
        perms_response = self.test_app.post(
            "/auth/permissions",
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(perms_response.status_code, 200, perms_response.text)
        perms = perms_response.json()
        self.assertEqual(perms["hub"], -1)
        self.assertEqual(perms["core"], -1)
        self.assertEqual(perms["node"], -1)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_auth_refresh(self, send_request):
        valid_user = User(username="guest", password_hash="password")
        send_request.return_value = {"user": valid_user.model_dump(),
                                     "success": True}

        valid_tokens = self._get_tokens()

        # Valid request
        response = self.test_app.post("/auth/refresh", json=valid_tokens)
        self.assertEqual(response.status_code, 200, response.text)
        response_data = response.json()
        self.assertNotEqual(response_data, valid_tokens)

        # Refresh with old tokens fails (mocked return from users service)
        send_request.return_value = {"code": 422,
                                     "detail": "Invalid token",
                                     "success": False}
        response = self.test_app.post("/auth/refresh", json=valid_tokens)
        self.assertEqual(response.status_code, 422, response.text)

        # TODO: Test with expired token

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_assist_get_stt(self, send_request):
        send_request.return_value = {"data": {"transcripts": ["test"],
                                              "parser_data": {"test": True}}}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/neon/get_stt",
                                      json={"encoded_audio": "MOCK_B64_AUDIO",
                                            "lang_code": "en-us"},
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), send_request.return_value['data'])

        # Invalid missing auth
        response = self.test_app.post("/neon/get_stt",
                                      json={"encoded_audio": "MOCK_B64_AUDIO",
                                            "lang_code": "en-us"})
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/neon/get_stt",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_assist_get_tts(self, send_request):
        send_request.return_value = {"data": {
            "en-us": {"audio": {"female": "MOCK_B64_AUDIO"}}}}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/neon/get_tts",
                                      json={"to_speak": "test",
                                            "lang_code": "en-us"},
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['encoded_audio'], "MOCK_B64_AUDIO")

        # Invalid missing auth
        response = self.test_app.post("/neon/get_tts",
                                      json={"to_speak": "test",
                                            "lang_code": "en-us"})
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/neon/get_tts",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_assist_get_response(self, send_request):
        send_request.return_value = {
            "data": {"responses": {"en-us": {"sentence": "mock_response"}}},
            "context": {"session": {"new_session": True}}}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/neon/get_response",
                                      json={"utterance": "test",
                                            "lang_code": "en-us"},
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['answer'], "mock_response")
        self.assertEqual(response.json()['lang_code'], "en-us")

        # Invalid missing auth
        response = self.test_app.post("/neon/get_response",
                                      json={"utterance": "test",
                                            "lang_code": "en-us"})
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/neon/get_response",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_proxy_weather(self, send_request):
        send_request.return_value = {"status_code": 200,
                                     "content": json.dumps(
                                         {"lat": 47.6815,
                                          "lon": -122.2087,
                                          "timezone": "America/Los_Angeles",
                                          "timezone_offset": -28800,
                                          "current": {},
                                          "minutely": [],
                                          "hourly": [],
                                          "daily": []})}
        valid_request = {"lat": 47.6815,
                         "lon": -122.2087,
                         "unit": "metric"}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/proxy/weather",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(),
                         json.loads(send_request.return_value['content']),
                         response.json())

        # Invalid missing auth
        response = self.test_app.post("/proxy/weather",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/proxy/weather",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_proxy_stock_symbol(self, send_request):
        send_request.return_value = {"status_code": 200,
                                     "content": json.dumps(
                                         {"bestMatches": []})}
        valid_request = {"company": "microsoft",
                         "region": "United States"}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/proxy/stock/symbol",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['bestMatches'],
                         json.loads(send_request.return_value['content'])['bestMatches'],
                         response.json())
        self.assertEqual(response.json()['provider'], "alpha_vantage")

        # Invalid missing auth
        response = self.test_app.post("/proxy/stock/symbol",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/proxy/stock/symbol",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

        # TODO test region filtering

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_proxy_stock_quote(self, send_request):
        send_request.return_value = {"status_code": 200,
                                     "content": json.dumps(
                                         {"Global Quote": {"test": "True"}})}
        valid_request = {"symbol": "GOOG"}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/proxy/stock/quote",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["Global Quote"],
                         json.loads(send_request.return_value['content'])["Global Quote"],
                         response.json())

        # Invalid missing auth
        response = self.test_app.post("/proxy/stock/quote",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/proxy/stock/quote",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_proxy_geocode(self, send_request):
        send_request.return_value = {"status_code": 200,
                                     "content": json.dumps(
                                         {"place_id": 0,
                                          "licence": "test",
                                          "osm_type": "test",
                                          "osm_id": 0,
                                          "boundingbox": ["0", "0", "0", "0"],
                                          "lat": "47.6815",
                                          "lon": "-122.2087",
                                          "display_name": "test",
                                          "class": "amenity",
                                          "type": "post_office",
                                          "importance": 1.0,
                                          "alternate_results": []})}
        valid_request = {"address": "1100 Bellevue Way NE Bellevue, WA"}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/proxy/geolocation/geocode",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(),
                         json.loads(send_request.return_value['content']),
                         response.json())

        # Invalid missing auth
        response = self.test_app.post("/proxy/geolocation/geocode",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/proxy/geolocation/geocode",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_proxy_geocode_reverse(self, send_request):
        send_request.return_value = {"status_code": 200,
                                     "content": json.dumps(
                                         {"place_id": 0,
                                          "licence": "test",
                                          "osm_type": "test",
                                          "osm_id": 0,
                                          "boundingbox": ["0", "0", "0", "0"],
                                          "lat": "47.6815",
                                          "lon": "-122.2087",
                                          "display_name": "test",
                                          "address": {}})}

        valid_request = {"lat": 47.6815, "lon": -122.2087}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/proxy/geolocation/reverse",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(),
                         json.loads(send_request.return_value['content']),
                         response.json())

        # Invalid missing auth
        response = self.test_app.post("/proxy/geolocation/reverse",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/proxy/geolocation/reverse",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_proxy_wolfram(self, send_request):
        send_request.return_value = {"status_code": 200,
                                     "content": "answer"}
        valid_request = {"api": "spoken", "lat": 47.6815, "lon": -122.2087,
                         "query": "how far away is the moon"}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/proxy/wolframalpha",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(),
                         {"answer": send_request.return_value['content']},
                         response.json())

        # Invalid missing auth
        response = self.test_app.post("/proxy/wolframalpha",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/proxy/wolframalpha",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_backend_email(self, send_request):
        send_request.return_value = {"success": True}
        valid_request = {"recipient": "developers@neon.ai",
                         "subject": "API test",
                         "body": "This is a test.\nGenerated from OpenAPI.",
                         "attachments": {
                             "test.txt": "VGhpcyBpcyBhIHRlc3QgZmlsZQo="}}

        token = self._get_tokens()["access_token"]
        # Valid request
        response = self.test_app.post("/email",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)

        # Invalid missing auth
        response = self.test_app.post("/email",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/email",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

        # Valid request failed
        send_request.return_value = {"success": False,
                                     "error": "Something has failed"}
        response = self.test_app.post("/email",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 500, response.text)
        self.assertEqual(response.json()['detail'], "Something has failed")

        # TODO: Test disabled service

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_backend_metrics(self, send_request):
        send_request.return_value = {}
        valid_request = {"metric_name": "Unit Test",
                         "timestamp": str(time()),
                         "metric_data": {"test": True}}
        token = self._get_tokens()["access_token"]

        # Valid request
        response = self.test_app.post("/metrics/upload",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)

        # Invalid missing auth
        response = self.test_app.post("/metrics/upload",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/metrics/upload",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_backend_ccl(self, send_request):
        send_request.return_value = {"parsed_file": "MOCK_NCS_DATA"}
        valid_request = {"script": "MOCK_SCRIPT_DATA"}
        token = self._get_tokens()["access_token"]

        # Valid request
        response = self.test_app.post("/ccl/parse",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['ncs'], "MOCK_NCS_DATA")

        # Invalid missing auth
        response = self.test_app.post("/ccl/parse",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid request
        self.assertEqual(self.test_app.post(
            "/ccl/parse",
            headers={"Authorization": f"Bearer {token}"}).status_code,
                         422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_backend_coupons(self, send_request):
        send_request.return_value = {"success": True, "brands": [],
                                     "coupons": []}
        token = self._get_tokens()["access_token"]

        # Valid request
        response = self.test_app.post("/coupons",
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), send_request.return_value)

        # Invalid missing auth
        response = self.test_app.post("/coupons")
        self.assertIn(response.status_code, [401, 403], response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_llm(self, send_request):
        send_request.return_value = {"response": "MOCK_LLM_RESPONSE"}
        valid_request = {"query": "how are you?",
                         "history": [("user", "hello"),
                                     ("llm", "Hi, how can I help you today?")]}
        # Responses are lists instead of tuples because Pydantic will auto-cast
        # for JSON encoding
        valid_response = {"response": "MOCK_LLM_RESPONSE",
                          "history": [["user", "hello"],
                                      ["llm", "Hi, how can I help you today?"],
                                      ["user", "how are you?"],
                                      ["llm", "MOCK_LLM_RESPONSE"]]}
        token = self._get_tokens()["access_token"]
        # ChatGPT
        response = self.test_app.post("/llm/chatgpt",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), valid_response)

        # Fastchat
        response = self.test_app.post("/llm/fastchat",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), valid_response)

        # Claude
        response = self.test_app.post("/llm/claude",
                                      json=valid_request,
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), valid_response)

        # Invalid requests
        response = self.test_app.post("/llm/chatgpt",
                                      json=valid_request)
        self.assertIn(response.status_code, [401, 403], response.text)

        response = self.test_app.post("/llm/chatgpt",
                                      headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 422, response.text)

    def test_util_is_ipv4(self):
        from neon_hana.app.routers.util import _is_ipv4
        self.assertTrue(_is_ipv4("127.0.0.1"))
        self.assertTrue(_is_ipv4("10.0.0.10"))
        self.assertTrue(_is_ipv4("1.1.1.1"))
        self.assertFalse(_is_ipv4("ai.neon.api.1"))
        self.assertFalse(_is_ipv4("host.local"))
        self.assertFalse(_is_ipv4("localhost"))
        self.assertFalse(_is_ipv4("1.0.0.300"))

    def test_util_client_ip(self):
        response = self.test_app.get("/util/client_ip")
        self.assertEqual(response.text, "127.0.0.1")

    def test_util_headers(self):
        test_headers = {"X-Auth-Token": "Token",
                        "Authorization": "Test Auth",
                        "My Custom Header": "Value"}
        response = self.test_app.get("/util/headers", headers=test_headers)
        for key, val in test_headers.items():
            self.assertEqual(response.json()[key.lower()], val, response.json())

    def test_hub_identity_get(self):
        # Public endpoint, no auth required
        response = self.test_app.get("/hub/identity")
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()
        self.assertTrue(data["hub_id"], "hub_id should be non-empty")

    def test_hub_identity_stable(self):
        # hub_id must not change across calls
        id1 = self.test_app.get("/hub/identity").json()["hub_id"]
        id2 = self.test_app.get("/hub/identity").json()["hub_id"]
        id3 = self.test_app.get("/hub/identity").json()["hub_id"]
        self.assertEqual(id1, id2)
        self.assertEqual(id2, id3)

    @patch("neon_hana.app.routers.hub.update_mycroft_config")
    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_hub_identity_update(self, send_request, mock_config_write):
        from neon_hana.app.dependencies import config
        self.addCleanup(config.pop, "hub_display_name", None)

        token = self._get_admin_tokens()["access_token"]
        original = self.test_app.get("/hub/identity").json()

        # Valid update
        response = self.test_app.post(
            "/hub/identity",
            json={"display_name": "Kitchen Hub"},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()
        self.assertEqual(data["display_name"], "Kitchen Hub")
        self.assertEqual(data["hub_id"], original["hub_id"])
        mock_config_write.assert_called()

        # Verify persistence in memory
        data = self.test_app.get("/hub/identity").json()
        self.assertEqual(data["display_name"], "Kitchen Hub")

        # Whitespace is stripped from valid input
        response = self.test_app.post(
            "/hub/identity",
            json={"display_name": "  Living Room Hub  "},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.json()["display_name"], "Living Room Hub")

        # Boundary: 1-char name accepted
        response = self.test_app.post(
            "/hub/identity",
            json={"display_name": "X"},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)

        # Boundary: 128-char name accepted
        response = self.test_app.post(
            "/hub/identity",
            json={"display_name": "x" * 128},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_hub_identity_update_auth_required(self, send_request):
        # Missing auth
        response = self.test_app.post(
            "/hub/identity",
            json={"display_name": "No Auth Hub"})
        self.assertIn(response.status_code, [401, 403], response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_hub_identity_update_insufficient_permissions(self, send_request):
        # Guest user (hub: 0) should be rejected
        token = self._get_tokens()["access_token"]
        response = self.test_app.post(
            "/hub/identity",
            json={"display_name": "Unauthorized Hub"},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 403, response.text)

    @patch("neon_hana.app.routers.hub.update_mycroft_config")
    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_hub_identity_update_validation(self, send_request,
                                            mock_config_write):
        token = self._get_admin_tokens()["access_token"]

        # Empty display_name
        response = self.test_app.post(
            "/hub/identity",
            json={"display_name": ""},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 422, response.text)

        # Whitespace-only display_name
        response = self.test_app.post(
            "/hub/identity",
            json={"display_name": "   "},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 422, response.text)

        # Too long display_name
        response = self.test_app.post(
            "/hub/identity",
            json={"display_name": "x" * 129},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 422, response.text)

        # Missing body
        response = self.test_app.post(
            "/hub/identity",
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 422, response.text)

        # Config was never written for invalid requests
        mock_config_write.assert_not_called()

    @patch("neon_hana.app.routers.hub._read_neon_yaml")
    def test_hub_config_with_explicit_config(self, mock_read):
        mock_read.return_value = {
            "tts": {"module": "ovos-tts-plugin-mimic"},
            "stt": {"module": "ovos-stt-plugin-vosk"},
        }
        response = self.test_app.get("/hub/config")
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()
        self.assertEqual(data["tts"]["module"], "ovos-tts-plugin-mimic")
        self.assertEqual(data["stt"]["module"], "ovos-stt-plugin-vosk")
        self.assertEqual(data["llm"]["persona_name"], "Neon Classic")

    @patch("neon_hana.app.routers.hub._read_neon_yaml")
    def test_hub_config_defaults(self, mock_read):
        # neon.yaml exists but has no tts/stt keys — returns defaults
        mock_read.return_value = {"lang": "en-us"}
        response = self.test_app.get("/hub/config")
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()
        self.assertEqual(data["tts"]["module"], "neon-tts-plugin-coqui")
        self.assertEqual(data["stt"]["module"], "neon-stt-plugin-nemo")

    @patch("neon_hana.app.routers.hub._read_neon_yaml")
    def test_hub_config_no_neon_yaml(self, mock_read):
        # neon.yaml doesn't exist (non-Hub deployment)
        mock_read.return_value = None
        response = self.test_app.get("/hub/config")
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()
        self.assertIsNone(data["tts"])
        self.assertIsNone(data["stt"])
        self.assertEqual(data["llm"]["persona_name"], "Neon Classic")

    @patch("neon_hana.app.routers.hub._read_neon_yaml")
    def test_hub_config_key_without_module(self, mock_read):
        # tts/stt keys exist but module sub-key is absent — returns defaults
        mock_read.return_value = {"tts": {"lang": "en-us"}, "stt": {"lang": "en-us"}}
        response = self.test_app.get("/hub/config")
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()
        self.assertEqual(data["tts"]["module"], "neon-tts-plugin-coqui")
        self.assertEqual(data["stt"]["module"], "neon-stt-plugin-nemo")

    @patch("neon_hana.app.routers.hub._read_neon_yaml")
    def test_hub_config_none_values(self, mock_read):
        # tts/stt keys exist but are explicitly None — returns defaults
        mock_read.return_value = {"tts": None, "stt": None}
        response = self.test_app.get("/hub/config")
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()
        self.assertEqual(data["tts"]["module"], "neon-tts-plugin-coqui")
        self.assertEqual(data["stt"]["module"], "neon-stt-plugin-nemo")

    def test_hub_config_no_auth_required(self):
        # Public discovery endpoint — must not require authentication
        response = self.test_app.get("/hub/config")
        self.assertNotIn(response.status_code, [401, 403],
                         "GET /hub/config should not require authentication")

    def test_read_neon_yaml_permission_error(self):
        import os
        from neon_hana.app.routers.hub import _read_neon_yaml
        with patch.dict(os.environ, {"XDG_CONFIG_HOME": "/tmp/_neon_test_perm"}):
            with patch("builtins.open",
                       side_effect=PermissionError("Permission denied")):
                result = _read_neon_yaml()
        self.assertIsNone(result)

    def test_read_neon_yaml_is_directory(self):
        import tempfile, os
        from neon_hana.app.routers.hub import _read_neon_yaml
        with tempfile.TemporaryDirectory() as tmpdir:
            neon_dir = os.path.join(tmpdir, "neon")
            os.makedirs(neon_dir)
            # Create neon.yaml as a directory instead of a file
            os.makedirs(os.path.join(neon_dir, "neon.yaml"))
            with patch.dict(os.environ, {"XDG_CONFIG_HOME": tmpdir}):
                result = _read_neon_yaml()
            self.assertIsNone(result)

    def test_read_neon_yaml_malformed_yaml(self):
        import tempfile, os
        from neon_hana.app.routers.hub import _read_neon_yaml
        with tempfile.TemporaryDirectory() as tmpdir:
            neon_dir = os.path.join(tmpdir, "neon")
            os.makedirs(neon_dir)
            yaml_path = os.path.join(neon_dir, "neon.yaml")
            with open(yaml_path, "w") as f:
                f.write("tts: [invalid\n  bad: yaml:")
            with patch.dict(os.environ, {"XDG_CONFIG_HOME": tmpdir}):
                result = _read_neon_yaml()
            self.assertIsNone(result)

    def test_read_neon_yaml_non_dict(self):
        import tempfile, os
        from neon_hana.app.routers.hub import _read_neon_yaml
        with tempfile.TemporaryDirectory() as tmpdir:
            neon_dir = os.path.join(tmpdir, "neon")
            os.makedirs(neon_dir)
            yaml_path = os.path.join(neon_dir, "neon.yaml")
            with open(yaml_path, "w") as f:
                f.write("- item1\n- item2\n")
            with patch.dict(os.environ, {"XDG_CONFIG_HOME": tmpdir}):
                result = _read_neon_yaml()
            self.assertIsNone(result)

    def test_read_neon_yaml_empty_file(self):
        import tempfile, os
        from neon_hana.app.routers.hub import _read_neon_yaml
        with tempfile.TemporaryDirectory() as tmpdir:
            neon_dir = os.path.join(tmpdir, "neon")
            os.makedirs(neon_dir)
            yaml_path = os.path.join(neon_dir, "neon.yaml")
            with open(yaml_path, "w") as f:
                f.write("")
            with patch.dict(os.environ, {"XDG_CONFIG_HOME": tmpdir}):
                result = _read_neon_yaml()
            self.assertEqual(result, {})

    @staticmethod
    def _bus_request(send_request) -> dict:
        """The serialized Message HANA handed to `send_mq_request`"""
        return send_request.call_args[0][1]

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_notifications_list(self, send_request):
        notification = Notification(notification_id="n1",
                                    skill_id="skill-test", text="hello",
                                    scope=NotificationScope.USER,
                                    target="user-a",
                                    permission=NotificationPermission.PERSONAL)
        send_request.return_value = {
            "msg_type": "ovos.notification.api.list.response",
            "data": {"notifications": [notification.model_dump(mode="json")],
                     "states": {"n1": "active"}},
            "context": {}}
        token = self._get_tokens()["access_token"]

        # Valid request
        response = self.test_app.get(
            "/notifications",
            params={"since": "2026-08-20T00:00:00Z", "state": "active"},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()
        self.assertEqual(data["notifications"][0]["notification_id"], "n1")
        self.assertEqual(data["states"], {"n1": "active"})
        self.assertIsInstance(data["server_time"], str)

        request = self._bus_request(send_request)
        self.assertEqual(request["msg_type"], "ovos.notification.api.list")
        self.assertEqual(request["data"]["state"], "active")
        self.assertEqual(request["data"]["since"], "2026-08-20T00:00:00Z")
        self.assertEqual(request["data"]["node_id"],
                         request["context"]["node"]["node_id"])
        self.assertEqual(request["data"]["user_id"],
                         request["context"]["user_id"])
        self.assertEqual(request["context"]["session"]["session_id"],
                         request["data"]["node_id"])

        # No state filter and no cursor: neither key is sent, so the manager
        # returns dismissed/expired tombstones for reconciliation
        response = self.test_app.get(
            "/notifications", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        request = self._bus_request(send_request)
        self.assertNotIn("state", request["data"])
        self.assertNotIn("since", request["data"])

        # Catch-up cursor alone still sends no state filter
        response = self.test_app.get(
            "/notifications", params={"since": "2026-08-20T00:00:00Z"},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        request = self._bus_request(send_request)
        self.assertNotIn("state", request["data"])

        # Invalid missing auth
        response = self.test_app.get("/notifications")
        self.assertIn(response.status_code, [401, 403], response.text)

        # Invalid cursor
        response = self.test_app.get(
            "/notifications", params={"since": "yesterday"},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 422, response.text)

        # Invalid lifecycle state
        response = self.test_app.get(
            "/notifications", params={"state": "unread"},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 422, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_notifications_list_no_manager_response(self, send_request):
        send_request.return_value = {}
        token = self._get_tokens()["access_token"]
        response = self.test_app.get(
            "/notifications", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 504, response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_notifications_dismiss(self, send_request):
        send_request.return_value = {
            "msg_type": "ovos.notification.api.remove.response",
            "data": {"notification_ids": ["n1"], "status": "ok"},
            "context": {}}
        token = self._get_tokens()["access_token"]

        # Valid request
        response = self.test_app.post(
            "/notifications/n1/dismiss",
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(),
                         {"notification_id": "n1", "status": "dismissed"})
        request = self._bus_request(send_request)
        self.assertEqual(request["msg_type"], "ovos.notification.api.remove")
        self.assertEqual(request["data"]["notification_id"], "n1")
        self.assertEqual(request["data"]["dismissed_by"],
                         request["context"]["node"]["node_id"])

        # Refused by the Notification Manager
        send_request.return_value = {
            "data": {"notification_ids": [], "status": "refused",
                     "reason": "removable_by_user is False"}}
        response = self.test_app.post(
            "/notifications/n1/dismiss",
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 403, response.text)
        self.assertEqual(response.json()["detail"],
                         "removable_by_user is False")

        # Unknown notification
        send_request.return_value = {
            "data": {"notification_ids": [], "status": "not_found"}}
        response = self.test_app.post(
            "/notifications/n1/dismiss",
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 404, response.text)

        # No response from the Notification Manager
        send_request.return_value = {}
        response = self.test_app.post(
            "/notifications/n1/dismiss",
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 504, response.text)

        # Invalid missing auth
        response = self.test_app.post("/notifications/n1/dismiss")
        self.assertIn(response.status_code, [401, 403], response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_notifications_snooze(self, send_request):
        send_request.return_value = {
            "msg_type": "ovos.notification.api.snooze.response",
            "data": {"notification_id": "n1", "status": "ok",
                     "renotify_at": "2026-08-21T08:00:00+00:00"},
            "context": {}}
        token = self._get_tokens()["access_token"]

        # Valid request
        response = self.test_app.post(
            "/notifications/n1/snooze", json={"duration": 600},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["notification_id"], "n1")
        self.assertEqual(response.json()["renotify_at"],
                         "2026-08-21T08:00:00Z")
        request = self._bus_request(send_request)
        self.assertEqual(request["msg_type"], "ovos.notification.api.snooze")
        self.assertEqual(request["data"],
                         {"notification_id": "n1", "duration": 600})

        # Refused by the Notification Manager
        send_request.return_value = {
            "data": {"notification_id": "n1", "status": "refused",
                     "reason": "not snoozable"}}
        response = self.test_app.post(
            "/notifications/n1/snooze", json={"duration": 600},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 403, response.text)

        # Accepted without a `renotify_at` the client can act on
        send_request.return_value = {
            "data": {"notification_id": "n1", "status": "ok"}}
        response = self.test_app.post(
            "/notifications/n1/snooze", json={"duration": 600},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 504, response.text)

        # Invalid request
        for body in ({"duration": 0}, {"duration": "soon"}, {}):
            response = self.test_app.post(
                "/notifications/n1/snooze", json=body,
                headers={"Authorization": f"Bearer {token}"})
            self.assertEqual(response.status_code, 422, response.text)

        # Invalid missing auth
        response = self.test_app.post("/notifications/n1/snooze",
                                      json={"duration": 600})
        self.assertIn(response.status_code, [401, 403], response.text)

    @patch("neon_hana.mq_service_api.send_mq_request")
    def test_notifications_interaction(self, send_request):
        send_request.return_value = {}
        token = self._get_tokens()["access_token"]

        # Valid request is accepted without waiting for the producer
        response = self.test_app.post(
            "/notifications/n1/interaction",
            json={"action_id": "open",
                  "callback_data": {"report_date": "2026-08-20"}},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 202, response.text)
        self.assertEqual(response.json(),
                         {"notification_id": "n1", "status": "accepted"})
        self.assertFalse(send_request.call_args.kwargs["expect_response"])
        request = self._bus_request(send_request)
        self.assertEqual(request["msg_type"],
                         "ovos.notification.api.interaction")
        self.assertEqual(request["data"],
                         {"notification_id": "n1", "action_id": "open",
                          "callback_data": {"report_date": "2026-08-20"}})

        # callback_data defaults to empty when not supplied
        response = self.test_app.post(
            "/notifications/n1/interaction", json={"action_id": "snooze"},
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 202, response.text)
        self.assertEqual(self._bus_request(send_request)["data"],
                         {"notification_id": "n1", "action_id": "snooze",
                          "callback_data": {}})

        # Invalid request
        for body in ({"callback_data": {}}, {}):
            response = self.test_app.post(
                "/notifications/n1/interaction", json=body,
                headers={"Authorization": f"Bearer {token}"})
            self.assertEqual(response.status_code, 422, response.text)

        # Invalid missing auth
        response = self.test_app.post("/notifications/n1/interaction",
                                      json={"action_id": "open"})
        self.assertIn(response.status_code, [401, 403], response.text)

# TODO: Define node endpoint tests
