# NEON AI (TM) SOFTWARE, Software Development Kit & Application Development System
# All trademark and other rights reserved by their respective owners
# Copyright 2008-2021 Neongecko.com Inc.
# BSD-3
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS  BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS;  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE,  EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import json
import os
import tempfile
import unittest

from asyncio import run
from unittest.mock import AsyncMock, MagicMock, patch

from neon_utils.socket_utils import dict_to_b64
from ovos_bus_client.message import Message

from neon_data_models.enum import NotificationPermission, NotificationScope
from neon_data_models.models.api.messagebus.notifications import (
    NeonNotificationDismiss, NeonNotificationNotify, NeonNotificationSnoozed)

from neon_hana.mq_websocket_api import (MQWebsocketAPI, NOTIFICATIONS_CONSUMER,
                                        NOTIFICATIONS_EXCHANGE)
from neon_hana.node_registry import NodeRegistry


TEST_SESSION = "node-test"

VALID_HELLO = {"msg_type": "node.hello",
               "data": {"node_id": TEST_SESSION,
                        "node_name": "Kitchen Phone",
                        "capabilities": {"launch_camera_app": True,
                                         "launch_sms_app": False}}}


def _make_api() -> MQWebsocketAPI:
    with patch("neon_hana.mq_websocket_api.NeonAIClient.__init__",
               return_value=None):
        api = MQWebsocketAPI({})
    api.client_name = "test_client"
    api._uid = "test-uid"
    api._connection = MagicMock()
    api._connection.create_unique_id.return_value = "test-mid"
    api._send_message = MagicMock()
    return api


def _seed_session(api: MQWebsocketAPI, session_id: str = TEST_SESSION,
                  site_id: str = "kitchen"):
    socket = MagicMock()
    socket.send_text = AsyncMock()
    api._sessions[session_id] = {
        "session": {"session_id": session_id, "site_id": site_id},
        "socket": socket,
        "user": {"user": {"username": "tester"}}}
    return socket


class TestNodeHello(unittest.TestCase):
    def test_hello_caches_snapshot(self):
        api = _make_api()
        _seed_session(api)
        api.handle_client_input(dict(VALID_HELLO), TEST_SESSION)
        cached = api._sessions[TEST_SESSION]["node"]
        self.assertEqual(cached["node_id"], TEST_SESSION)
        self.assertEqual(cached["node_name"], "Kitchen Phone")
        self.assertEqual(cached["capabilities"],
                         {"launch_camera_app": True, "launch_sms_app": False})
        # The hello is still forwarded to the bus
        api._send_message.assert_called_once()

    def test_hello_session_identity_is_authoritative(self):
        api = _make_api()
        _seed_session(api)
        hello = {"msg_type": "node.hello",
                 "data": {**VALID_HELLO["data"], "node_id": "someone-else"}}
        api.handle_client_input(hello, TEST_SESSION)
        # The token-derived session ID wins over the self-reported node_id
        self.assertEqual(api._sessions[TEST_SESSION]["node"]["node_id"],
                         TEST_SESSION)

    def test_invalid_hello_ignored(self):
        api = _make_api()
        _seed_session(api)
        for bad_data in ({},  # missing node_id
                         {"node_id": TEST_SESSION,
                          "node_name": "x" * 129}):  # name over cap
            api.handle_client_input({"msg_type": "node.hello",
                                     "data": bad_data}, TEST_SESSION)
            self.assertNotIn("node", api._sessions[TEST_SESSION])

    def test_hello_updates_on_repeat(self):
        api = _make_api()
        _seed_session(api)
        api.handle_client_input(dict(VALID_HELLO), TEST_SESSION)
        renamed = {"msg_type": "node.hello",
                   "data": {**VALID_HELLO["data"], "node_name": "Den Phone"}}
        api.handle_client_input(renamed, TEST_SESSION)
        self.assertEqual(api._sessions[TEST_SESSION]["node"]["node_name"],
                         "Den Phone")

    @staticmethod
    def _hello_acks(socket) -> list:
        return [m for m in
                (json.loads(c.args[0])
                 for c in socket.send_text.call_args_list)
                if m["type"] == "node.hello.response"]

    def test_hello_acknowledged_with_normalized_snapshot(self):
        api = _make_api()
        socket = _seed_session(api)
        api.handle_client_input(dict(VALID_HELLO), TEST_SESSION)
        ack = self._hello_acks(socket)[0]
        self.assertEqual(ack["data"]["status"], "success")
        self.assertEqual(ack["data"]["node"]["node_id"], TEST_SESSION)
        self.assertEqual(ack["data"]["node"]["node_name"], "Kitchen Phone")
        self.assertEqual(ack["data"]["node"]["capabilities"],
                         {"launch_camera_app": True, "launch_sms_app": False})

    def test_hello_ack_echoes_session_identity_not_claimed_id(self):
        # The ack reports what context.node will actually carry, so a Node
        # that claimed a different node_id learns the hub overrode it
        api = _make_api()
        socket = _seed_session(api)
        hello = {"msg_type": "node.hello",
                 "data": {**VALID_HELLO["data"], "node_id": "someone-else"}}
        api.handle_client_input(hello, TEST_SESSION)
        ack = self._hello_acks(socket)[0]
        self.assertEqual(ack["data"]["node"]["node_id"], TEST_SESSION)

    def test_rejected_hello_gets_error_response(self):
        # A rejection is otherwise invisible to the Node -- it only shows up
        # later as capability gating silently doing nothing
        api = _make_api()
        socket = _seed_session(api)
        api.handle_client_input({"msg_type": "node.hello", "data": {}},
                                TEST_SESSION)
        ack = self._hello_acks(socket)[0]
        self.assertEqual(ack["data"]["status"], "error")
        self.assertIn("node_id", ack["data"]["error"]["message"])
        self.assertNotIn("node", ack["data"])


class TestNodeContextEnrichment(unittest.TestCase):
    def test_context_includes_node_after_hello(self):
        api = _make_api()
        _seed_session(api)
        api.handle_client_input(dict(VALID_HELLO), TEST_SESSION)
        context = api._get_message_context(
            Message("neon.audio_input", {}, {}), TEST_SESSION)
        self.assertEqual(context["node"],
                         {"node_id": TEST_SESSION,
                          "node_name": "Kitchen Phone",
                          "site_id": "kitchen",
                          "capabilities": {"launch_camera_app": True,
                                           "launch_sms_app": False}})

    def test_context_without_hello_has_no_node(self):
        api = _make_api()
        _seed_session(api)
        context = api._get_message_context(
            Message("neon.audio_input", {}, {}), TEST_SESSION)
        self.assertNotIn("node", context)

    def test_context_carries_registered_owner_as_user_id(self):
        # The default profile's `username` is not the caller; the JWT owner
        # recorded at connect is what hub services must see
        api = _make_api()
        api._node_registry = _make_registry(self)
        api._node_registry.upsert_connect(TEST_SESSION, "user-42")
        _seed_session(api)
        context = api._get_message_context(
            Message("ovos.notification.api.sync.request", {}, {}),
            TEST_SESSION)
        self.assertEqual(context["user_id"], "user-42")
        self.assertEqual(context["username"], "tester")

    def test_context_omits_user_id_for_unknown_node(self):
        api = _make_api()
        api._node_registry = _make_registry(self)
        _seed_session(api)
        context = api._get_message_context(
            Message("neon.audio_input", {}, {}), TEST_SESSION)
        self.assertNotIn("user_id", context)


class TestInvokeNativeDispatch(unittest.TestCase):
    def _invoke_body(self, session_id: str = TEST_SESSION) -> bytes:
        return dict_to_b64(
            {"msg_type": "node.invoke_native",
             "data": {"action": "launch_camera_app"},
             "context": {"session": {"session_id": session_id}}})

    def test_invoke_native_routed_to_client(self):
        api = _make_api()
        socket = _seed_session(api)
        channel = MagicMock()
        method = MagicMock()
        method.delivery_tag = 1
        api.handle_neon_response(channel, method, None, self._invoke_body())
        channel.basic_ack.assert_called_once_with(delivery_tag=1)
        socket.send_text.assert_called_once()
        # `Message.serialize` uses `type` on the wire (see handle_client_input)
        sent = json.loads(socket.send_text.call_args[0][0])
        self.assertEqual(sent["type"], "node.invoke_native")
        self.assertEqual(sent["data"]["action"], "launch_camera_app")

    def test_other_messages_delegate_to_iris(self):
        api = _make_api()
        _seed_session(api)
        body = dict_to_b64({"msg_type": "klat.response", "data": {},
                            "context": {"session":
                                        {"session_id": TEST_SESSION}}})
        channel = MagicMock()
        method = MagicMock()
        with patch("neon_hana.mq_websocket_api.NeonAIClient."
                   "handle_neon_response") as delegate:
            api.handle_neon_response(channel, method, None, body)
        delegate.assert_called_once_with(channel, method, None, body)
        channel.basic_ack.assert_not_called()

    def test_invoke_native_unknown_session_logged_not_raised(self):
        api = _make_api()
        message = Message("node.invoke_native",
                          {"action": "launch_camera_app"},
                          {"session": {"session_id": "node-gone"}})
        # Must not raise; the skill's own timeout handles the failure path
        api.handle_node_invoke_native(message)


class TestInvokeNativeResponsePassthrough(unittest.TestCase):
    def test_response_forwarded_to_bus(self):
        api = _make_api()
        _seed_session(api)
        api.handle_client_input(
            {"msg_type": "node.invoke_native.response",
             "data": {"action": "launch_camera_app", "status": "success"},
             "context": {}}, TEST_SESSION)
        api._send_message.assert_called_once()
        forwarded = api._send_message.call_args[0][0]
        self.assertEqual(forwarded.msg_type, "node.invoke_native.response")
        self.assertEqual(forwarded.data["status"], "success")
        self.assertEqual(forwarded.context["session"]["session_id"],
                         TEST_SESSION)


def _make_registry(test_case: unittest.TestCase) -> NodeRegistry:
    tmp = tempfile.TemporaryDirectory()
    test_case.addCleanup(tmp.cleanup)
    return NodeRegistry(os.path.join(tmp.name, "node_registry.json"))


def _event_message(event) -> Message:
    """Serialize a data-model event the way the fanout exchange carries it"""
    payload = event.model_dump(mode="json")
    return Message(payload["msg_type"], payload["data"], payload["context"])


def _notify(scope: NotificationScope, target,
            permission: NotificationPermission = NotificationPermission.PRIVATE,
            notification_id: str = "n1") -> Message:
    return _event_message(NeonNotificationNotify(
        data={"notification": {"notification_id": notification_id,
                               "skill_id": "skill-test",
                               "text": "hello",
                               "scope": scope,
                               "target": target,
                               "permission": permission}},
        context={"source": "notification-manager"}))


def _delivered(socket) -> list:
    return [json.loads(c.args[0]) for c in socket.send_text.call_args_list]


class TestSendToClientGuard(unittest.TestCase):
    def test_unknown_session_is_noop(self):
        api = _make_api()
        # Must not raise: an offline client catches up over REST
        run(api.send_to_client(Message("klat.response", {},
                                       {"session": {"session_id": "ghost"}})))

    def test_missing_session_context_is_noop(self):
        api = _make_api()
        run(api.send_to_client(Message("klat.response", {}, {})))

    def test_explicit_session_id_overrides_context(self):
        api = _make_api()
        socket_a = _seed_session(api, "node-a")
        socket_b = _seed_session(api, "node-b")
        message = Message("ovos.notification.api.notify", {},
                          {"session": {"session_id": "node-a"}})
        run(api.send_to_client(message, "node-b"))
        socket_b.send_text.assert_called_once()
        socket_a.send_text.assert_not_called()


class TestNotificationFanOut(unittest.TestCase):
    def setUp(self):
        self.api = _make_api()
        self.registry = _make_registry(self)
        self.api._node_registry = self.registry
        self.sockets = {}
        for node_id, user_id in (("phone-a", "user-a"),
                                 ("tablet-a", "user-a"),
                                 ("phone-b", "user-b")):
            self.registry.upsert_connect(node_id, user_id)
            self.sockets[node_id] = _seed_session(self.api, node_id)
        # Registered but currently offline
        self.registry.upsert_connect("laptop-a", "user-a")

    def _recipients(self) -> set:
        return {node_id for node_id, socket in self.sockets.items()
                if socket.send_text.called}

    def test_client_scope_routes_to_target_only(self):
        self.api.handle_notification(
            _notify(NotificationScope.CLIENT, "phone-a"))
        self.assertEqual(self._recipients(), {"phone-a"})
        sent = _delivered(self.sockets["phone-a"])[0]
        self.assertEqual(sent["type"], "ovos.notification.api.notify")
        self.assertEqual(sent["data"]["notification"]["notification_id"],
                         "n1")

    def test_client_scope_offline_target_is_dropped(self):
        self.api.handle_notification(
            _notify(NotificationScope.CLIENT, "laptop-a"))
        self.assertEqual(self._recipients(), set())

    def test_client_scope_unregistered_target_is_dropped(self):
        self.api.handle_notification(
            _notify(NotificationScope.CLIENT, "nobody"))
        self.assertEqual(self._recipients(), set())

    def test_user_scope_routes_to_connected_nodes_of_user(self):
        self.api.handle_notification(
            _notify(NotificationScope.USER, "user-a"))
        self.assertEqual(self._recipients(), {"phone-a", "tablet-a"})

    def test_user_scope_unknown_user_is_dropped(self):
        self.api.handle_notification(_notify(NotificationScope.USER, "nobody"))
        self.assertEqual(self._recipients(), set())

    def test_global_scope_routes_to_every_connected_session(self):
        self.api.handle_notification(
            _notify(NotificationScope.GLOBAL, None,
                    permission=NotificationPermission.PUBLIC))
        self.assertEqual(self._recipients(),
                         {"phone-a", "tablet-a", "phone-b"})

    def _stale_registry(self) -> MagicMock:
        """
        A registry that (wrongly) resolves a USER scope to another user's node
        exercises the owner check independently of `resolve`
        """
        stale = MagicMock(spec=NodeRegistry)
        stale.resolve.return_value = ["phone-a", "phone-b"]
        stale.owner.side_effect = lambda n: "user-b" if n == "phone-b" \
            else "user-a"
        self.api._node_registry = stale
        return stale

    def test_personal_notification_withheld_from_other_users_sessions(self):
        self._stale_registry()
        self.api.handle_notification(
            _notify(NotificationScope.USER, "user-a",
                    permission=NotificationPermission.PERSONAL))
        self.assertEqual(self._recipients(), {"phone-a"})

    def test_private_notification_withheld_from_other_users_sessions(self):
        self._stale_registry()
        self.api.handle_notification(
            _notify(NotificationScope.USER, "user-a",
                    permission=NotificationPermission.PRIVATE))
        self.assertEqual(self._recipients(), {"phone-a"})

    def test_public_notification_not_owner_gated(self):
        self._stale_registry()
        self.api.handle_notification(
            _notify(NotificationScope.USER, "user-a",
                    permission=NotificationPermission.PUBLIC))
        self.assertEqual(self._recipients(), {"phone-a", "phone-b"})

    def test_private_client_notification_withheld_without_owner(self):
        self.registry.upsert_connect("kiosk", "")
        self.sockets["kiosk"] = _seed_session(self.api, "kiosk")
        self.api.handle_notification(
            _notify(NotificationScope.CLIENT, "kiosk",
                    permission=NotificationPermission.PRIVATE))
        self.assertEqual(self._recipients(), set())
        self.api.handle_notification(
            _notify(NotificationScope.CLIENT, "kiosk",
                    permission=NotificationPermission.PUBLIC))
        self.assertEqual(self._recipients(), {"kiosk"})

    def test_dismiss_uses_top_level_scope_and_target(self):
        self.api.handle_notification(_event_message(NeonNotificationDismiss(
            data={"notification_id": "n1", "skill_id": "skill-test",
                  "scope": NotificationScope.USER, "target": "user-b",
                  "dismissed_by": "phone-b"}, context={})))
        self.assertEqual(self._recipients(), {"phone-b"})
        self.assertEqual(_delivered(self.sockets["phone-b"])[0]["type"],
                         "ovos.notification.api.dismiss")

    @staticmethod
    def _snoozed(**address) -> Message:
        return _event_message(NeonNotificationSnoozed(
            data={"notification_id": "n1",
                  "renotify_at": "2026-08-21T08:00:00+00:00",
                  **address}, context={}))

    def test_snoozed_client_scope_routes_to_target_only(self):
        self.api.handle_notification(
            self._snoozed(scope=NotificationScope.CLIENT, target="tablet-a"))
        self.assertEqual(self._recipients(), {"tablet-a"})
        self.assertEqual(_delivered(self.sockets["tablet-a"])[0]["type"],
                         "ovos.notification.api.snoozed")

    def test_snoozed_user_scope_routes_to_connected_nodes_of_user(self):
        self.api.handle_notification(
            self._snoozed(scope=NotificationScope.USER, target="user-a"))
        self.assertEqual(self._recipients(), {"phone-a", "tablet-a"})

    def test_snoozed_global_scope_routes_to_every_connected_session(self):
        self.api.handle_notification(
            self._snoozed(scope=NotificationScope.GLOBAL, target=None))
        self.assertEqual(self._recipients(),
                         {"phone-a", "tablet-a", "phone-b"})

    def test_snoozed_without_scope_falls_back_to_broadcast(self):
        # `scope`/`target` are optional on `snoozed`; an id-only lifecycle
        # event is broadcast and consumers drop ids they do not hold
        self.api.handle_notification(self._snoozed())
        self.assertEqual(self._recipients(),
                         {"phone-a", "tablet-a", "phone-b"})

    def test_notify_missing_required_field_is_dropped(self):
        # `skill_id` and `text` are required; an invalid payload is dropped
        message = Message("ovos.notification.api.notify",
                          {"notification": {"notification_id": "n1"}}, {})
        self.api.handle_notification(message)
        self.assertEqual(self._recipients(), set())

    def test_notify_invalid_scope_is_dropped(self):
        message = Message("ovos.notification.api.notify",
                          {"notification": {"notification_id": "n1",
                                            "skill_id": "skill-test",
                                            "text": "hello",
                                            "scope": 99}}, {})
        self.api.handle_notification(message)
        self.assertEqual(self._recipients(), set())

    def test_dismiss_missing_required_field_is_dropped(self):
        # `skill_id` and `scope` are required on a dismiss event
        message = Message("ovos.notification.api.dismiss",
                          {"notification_id": "n1"}, {})
        self.api.handle_notification(message)
        self.assertEqual(self._recipients(), set())

    def test_notify_without_scope_defaults_to_client_scope(self):
        # The model defaults scope to CLIENT and permission to PRIVATE, so an
        # unaddressed notification reaches nobody rather than everybody
        message = Message("ovos.notification.api.notify",
                          {"notification": {"notification_id": "n1",
                                            "skill_id": "skill-test",
                                            "text": "no address"}}, {})
        self.api.handle_notification(message)
        self.assertEqual(self._recipients(), set())

    def test_unexpected_msg_type_ignored(self):
        self.api.handle_notification(Message("klat.response", {}, {}))
        self.assertEqual(self._recipients(), set())

    def test_failed_socket_does_not_block_others(self):
        self.sockets["phone-a"].send_text.side_effect = RuntimeError("gone")
        self.api.handle_notification(
            _notify(NotificationScope.USER, "user-a"))
        self.sockets["tablet-a"].send_text.assert_called_once()

    def test_broadcast_callback_acks_and_routes(self):
        channel = MagicMock()
        method = MagicMock()
        method.delivery_tag = 7
        notify = _notify(NotificationScope.CLIENT, "phone-b")
        body = dict_to_b64({"msg_type": notify.msg_type,
                            "data": notify.data,
                            "context": {}})
        self.api.handle_notification_broadcast(channel, method, None, body)
        channel.basic_ack.assert_called_once_with(delivery_tag=7)
        self.assertEqual(self._recipients(), {"phone-b"})

    def test_broadcast_callback_bad_body_acks_and_drops(self):
        channel = MagicMock()
        method = MagicMock()
        method.delivery_tag = 8
        self.api.handle_notification_broadcast(channel, method, None,
                                               b"not base64 json")
        channel.basic_ack.assert_called_once_with(delivery_tag=8)
        self.assertEqual(self._recipients(), set())


class TestNodeHelloRegistry(unittest.TestCase):
    def test_hello_updates_registry(self):
        api = _make_api()
        registry = _make_registry(self)
        api._node_registry = registry
        _seed_session(api)
        registry.upsert_connect(TEST_SESSION, "user-a")
        api.handle_client_input(dict(VALID_HELLO), TEST_SESSION)
        record = registry.get(TEST_SESSION)
        self.assertEqual(record["node_name"], "Kitchen Phone")
        self.assertEqual(record["capabilities"],
                         {"launch_camera_app": True, "launch_sms_app": False})
        self.assertEqual(record["user_id"], "user-a")

    def test_hello_for_unregistered_node_is_ignored(self):
        api = _make_api()
        registry = _make_registry(self)
        api._node_registry = registry
        _seed_session(api)
        api.handle_client_input(dict(VALID_HELLO), TEST_SESSION)
        self.assertIsNone(registry.get(TEST_SESSION))


class TestNotificationSubscription(unittest.TestCase):
    def test_init_mq_connection_registers_fanout_subscriber(self):
        api = _make_api()
        api._vhost = "/neon_chat_api"
        connection = MagicMock()
        with patch("neon_hana.mq_websocket_api.NeonAIClient."
                   "_init_mq_connection", return_value=connection):
            self.assertIs(api._init_mq_connection(), connection)
        connection.register_subscriber.assert_called_once_with(
            NOTIFICATIONS_CONSUMER, "/neon_chat_api",
            api.handle_notification_broadcast,
            exchange=NOTIFICATIONS_EXCHANGE, auto_ack=False)
        connection.run_consumers.assert_called_once_with(
            names=(NOTIFICATIONS_CONSUMER,))
        self.assertEqual(NOTIFICATIONS_EXCHANGE, "neon_notifications")


if __name__ == '__main__':
    unittest.main()
