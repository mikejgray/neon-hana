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

from asyncio import run, get_event_loop
from os import makedirs
from queue import Queue
from time import time, sleep
from typing import List, NamedTuple, Optional
from fastapi import WebSocket
from neon_data_models.models.api.node_v1 import NodeHello
from neon_iris.client import NeonAIClient
from neon_utils.socket_utils import b64_to_dict
from ovos_bus_client.message import Message
from pydantic import ValidationError
from threading import RLock
from ovos_utils.log import LOG

from neon_data_models.enum import NotificationPermission, NotificationScope
from neon_data_models.models.api.messagebus.notifications import (
    NeonNotificationDismiss, NeonNotificationNotify, NeonNotificationSnoozed)

from neon_hana.node_registry import NodeRegistry, default_registry_path

# Fanout exchange the messagebus-MQ connector publishes Notification Manager
# events to. Must stay in sync with `NOTIFICATIONS_EXCHANGE` in
# neon-messagebus-mq-connector.
NOTIFICATIONS_EXCHANGE = "neon_notifications"
NOTIFICATIONS_CONSUMER = "neon_notifications_handler"
NOTIFICATION_MODELS = {
    NeonNotificationNotify.model_fields["msg_type"].default:
        NeonNotificationNotify,
    NeonNotificationDismiss.model_fields["msg_type"].default:
        NeonNotificationDismiss,
    NeonNotificationSnoozed.model_fields["msg_type"].default:
        NeonNotificationSnoozed,
}


class ClientNotKnown(RuntimeError):
    """
    Exception raised when a client tries to do something before authenticating
    """


class NotificationAddress(NamedTuple):
    """
    Where a notification event is addressed. `scope` is None only on a
    `snoozed` event that omits it, which is broadcast by notification_id.
    """
    scope: Optional[NotificationScope]
    target: Optional[str]
    permission: NotificationPermission


def _notification_address(event) -> NotificationAddress:
    """
    Read the address from a parsed notification event. `notify` nests it in
    the notification itself; `dismiss` and `snoozed` carry `scope`/`target`
    at the top level of `data` and are not permission-gated, since an
    id-only lifecycle event discloses no notification content.
    @param event: parsed NeonNotificationNotify/Dismiss/Snoozed
    @return: scope, target and the permission required to display the event
    """
    if isinstance(event, NeonNotificationNotify):
        notification = event.data.notification
        return NotificationAddress(notification.scope, notification.target,
                                   notification.permission)
    return NotificationAddress(event.data.scope, event.data.target,
                               NotificationPermission.PUBLIC)


class MQWebsocketAPI(NeonAIClient):
    def __init__(self, config: dict,
                 node_registry: Optional[NodeRegistry] = None):
        """
        Creates an MQWebsocketAPI to serve multiple client WS connections.
        @param config: `hana` configuration
        @param node_registry: registry used to resolve notification targets;
            created from `config["node_registry_path"]` when not supplied
        """
        self._node_registry = node_registry or NodeRegistry(
            config.get("node_registry_path") or default_registry_path())
        mq_config = config.get("MQ") or dict()
        config_dir = "/tmp/hana"
        makedirs(config_dir, exist_ok=True)
        NeonAIClient.__init__(self, mq_config, config_dir=config_dir)
        self._sessions = dict()
        self._session_lock = RLock()
        self._client = "neon_node_websocket"

    @property
    def node_registry(self) -> NodeRegistry:
        return self._node_registry

    def _init_mq_connection(self):
        mq_connection = super()._init_mq_connection()
        self._subscribe_to_notifications(mq_connection)
        return mq_connection

    def _subscribe_to_notifications(self, mq_connection):
        """
        Bind an instance-unique queue to the `neon_notifications` fanout
        exchange so hub-originated notification events reach this instance.
        `NeonAIClient._init_mq_connection` returns only after the
        connector's `run()` has started every consumer registered at that
        time, so registering afterwards means this consumer is started exactly
        once, here.
        @param mq_connection: connected IrisConnector
        """
        mq_connection.register_subscriber(NOTIFICATIONS_CONSUMER, self._vhost,
                                          self.handle_notification_broadcast,
                                          exchange=NOTIFICATIONS_EXCHANGE,
                                          auto_ack=False)
        mq_connection.run_consumers(names=(NOTIFICATIONS_CONSUMER,))

    def check_health(self) -> bool:
        """
        Check if the MQ connection is healthy.
        @return: True if the connection is healthy, False otherwise
        """
        return self.connection.check_health()

    def new_connection(self, ws: WebSocket, session_id: str):
        """
        Record a new client connection to associate the WebSocket with the
        session_id for response routing.
        @param ws: Client WebSocket object
        @param session_id: Session ID of the client
        """
        self._sessions[session_id] = {"session": {"session_id": session_id},
                                      "socket": ws,
                                      "user": self.user_config}

    def new_stream(self, ws: WebSocket, session_id: str):
        """
        Establish a new streaming connection, associated with an existing session.
        @param ws: Client WebSocket that handles byte audio
        @param session_id: Session ID the websocket is associated with
        """
        timeout = time() + 5
        while session_id not in self._sessions and time() < timeout:
            # Handle problem clients that don't explicitly wait for the Node WS
            # to connect before starting a stream
            sleep(1)
        with self._session_lock:
            if session_id not in self._sessions:
                raise ClientNotKnown(f"Stream cannot be established for {session_id}")
            from neon_hana.streaming_client import RemoteStreamHandler, StreamMicrophone
            if not self._sessions[session_id].get('stream'):
                LOG.info(f"starting stream for session {session_id}")
                audio_queue = Queue()
                stream = RemoteStreamHandler(StreamMicrophone(audio_queue), session_id,
                                             input_audio_callback=self.handle_client_input,
                                             ww_callback=self.handle_ww_detected,
                                             client_socket=ws)
                self._sessions[session_id]['stream'] = stream
                try:
                    stream.start()
                except RuntimeError:
                    pass

    def end_session(self, session_id: str):
        """
        End a client connection upon WS disconnection
        """
        with self._session_lock:
            session: Optional[dict] = self._sessions.pop(session_id, None)
        if not session:
            LOG.error(f"Ended session is not established {session_id}")
            return
        stream = session.get('stream')
        if stream:
            stream.shutdown()
            stream.join()
            LOG.info(f"Ended stream handler for: {session_id}")

    def get_session(self, session_id: str) -> dict:
        """
        Get the latest session context for the given session_id.
        @param session_id: Session ID to get context for
        @return: dict context for the given session_id (may be empty)
        """
        with self._session_lock:
            sess = dict(self._sessions.get(session_id, {}).get("session", {}))
        return sess

    def get_user_config(self, session_id: str) -> dict:
        """
        Get a dict user configuration for the given session_id
        @param session_id: Session to get user configuration for
        @return: dict user configuration
        """
        with self._session_lock:
            config = dict(self._sessions.get(session_id, {}).get("user") or
                          self.user_config)
        return config

    def _get_message_context(self, message: Message, session_id: str) -> dict:
        """
        Build message context for a Node input message.
        @param message: Input message to include context from
        @param session_id: Session ID associated with the message
        @return: dict context for this input
        """
        user_config = self.get_user_config(session_id)
        default_context = {"client_name": self.client_name,
                           "client": self._client,
                           "ident": str(time()),
                           "username": user_config['user']['username'],
                           "user_profiles": [user_config],
                           "neon_should_respond": True,
                           "timing": dict(),
                           "mq": {"routing_key": self.uid,
                                  "message_id": self.connection.
                                  create_unique_id()}}
        with self._session_lock:
            node_context = self._sessions.get(session_id, {}).get("node")
        if node_context:
            # Stamp the cached `node.hello` snapshot onto the outbound message
            # so skills read capabilities synchronously from `context.node`
            default_context["node"] = {
                **node_context,
                "site_id": self.get_session(session_id).get("site_id")}
        # The JWT owner recorded at connect is the authoritative user for
        # this Node; `username` above is the hub's default profile, not the
        # caller, so hub services (e.g. the Notification Manager) read this
        owner = self._node_registry.owner(session_id)
        if owner:
            default_context["user_id"] = owner
        return {**message.context, **default_context}

    def _update_session_data(self, message: Message):
        """
        Update the local session data and user profile from the latest response
        message's context.
        @param message: Response message containing updated context
        """
        session_data = message.context.get('session')
        if session_data:
            user_config = message.context.get('user_profiles', [None])[0]
            session_id = session_data.get('session_id')
            with self._session_lock:
                self._sessions[session_id]['session'] = session_data
                if user_config:
                    self._sessions[session_id]['user'] = user_config

    def handle_audio_input_stream(self, audio: bytes, session_id: str):
        self._sessions[session_id]['stream'].mic.queue.put(audio)

    def handle_ww_detected(self, ww_context: dict, session_id: str):
        session = self.get_session(session_id)
        message = Message("neon.ww_detected", ww_context,
                          {"session": session})
        run(self.send_to_client(message))

    def _handle_node_hello(self, data: dict, session_id: str):
        """
        Cache the Node identity and capabilities advertised in a `node.hello`
        message so outbound bus messages from this session can carry them in
        `context.node`.
        @param data: Decoded `node.hello` message from the client WebSocket
        @param session_id: Session ID associated with the client connection
        """
        try:
            hello = NodeHello(**{"context": {}, **data})
        except ValidationError as e:
            LOG.warning(f"Ignoring invalid node.hello from session "
                        f"{session_id}: {e}")
            self._send_hello_response(session_id, error=str(e))
            return
        if hello.data.node_id != session_id:
            # The token-derived session_id is authoritative for identity; a
            # client cannot claim another Node's ID via its hello payload
            LOG.warning(f"node.hello node_id ({hello.data.node_id}) does not "
                        f"match session ({session_id}); using session identity")
        # model_dump() serializes capability keys back to wire strings; the
        # validated model keys them by NodeNativeAction, which is not
        # JSON-serializable in bus context
        hello_data = hello.data.model_dump()
        node = {"node_id": session_id,
                "node_name": hello_data["node_name"],
                "capabilities": hello_data["capabilities"]}
        with self._session_lock:
            if session_id in self._sessions:
                self._sessions[session_id]["node"] = node
        self._node_registry.update_hello(session_id, node["node_name"],
                                         node["capabilities"])
        self._send_hello_response(session_id, node=node)

    def _send_hello_response(self, session_id: str, node: dict = None,
                             error: str = None):
        """
        Acknowledge a `node.hello` with the hub-normalized snapshot, or the
        validation error that made the hub drop it. A hub without this
        handler sends nothing, which lets Nodes flag an outdated hub.
        @param session_id: Session ID to acknowledge
        @param node: normalized node snapshot as cached (success path)
        @param error: validation error message (rejection path)
        """
        data = {"status": "error", "error": {"message": error}} if error \
            else {"status": "success", "node": node}
        try:
            message = Message("node.hello.response", data,
                              {"session": self.get_session(session_id)})
            run(self.send_to_client(message))
        except Exception as e:
            # Best-effort: a failed ack must not take down hello handling
            LOG.error(f"Failed to send node.hello.response to "
                      f"{session_id}: {e}")

    def handle_client_input(self, data: dict, session_id: str):
        """
        Handle some client input data.
        @param data: Decoded input from client WebSocket
        @param session_id: Session ID associated with the client connection
        """
        # Handle `Message.serialize` data sent over WS in addition to proper
        # dict representations
        data['msg_type'] = data.pop("type", data.get("msg_type"))
        if data['msg_type'] == "node.hello":
            self._handle_node_hello(data, session_id)
        message = Message(**data)
        message.context = self._get_message_context(message, session_id)
        message.context["session"] = self.get_session(session_id)
        # Send raw message, skipping any validation by iris
        self._send_message(message)

    def handle_neon_response(self, channel, method, properties, body):
        """
        Dispatch `node.*` messages that `NeonAIClient` does not know about,
        delegating everything else to its dispatcher unchanged.
        """
        # Peek at the msg_type; iris deserializes again on delegation. That
        # cost is accepted to avoid duplicating its dispatch/timing logic here.
        try:
            response = b64_to_dict(body)
        except Exception as e:
            LOG.error(f"Failed to peek at MQ message: {e}")
            response = None
        if response and response.get("msg_type") == "node.invoke_native":
            channel.basic_ack(delivery_tag=method.delivery_tag)
            message = Message(response.get("msg_type"), response.get("data"),
                              response.get("context"))
            self.handle_node_invoke_native(message)
        else:
            super().handle_neon_response(channel, method, properties, body)

    def handle_node_invoke_native(self, message: Message):
        """
        Forward a `node.invoke_native` request from a skill to the target Node.
        The message is relayed as-is: the Node re-validates the requested
        action and reports `not_supported` itself, which reaches the skill
        faster than a hub-side drop would (drop = skill waits out its timeout).
        @param message: `node.invoke_native` message from the bus
        """
        try:
            run(self.send_to_client(message))
        except Exception as e:
            LOG.exception(e)

    def handle_notification_broadcast(self, channel, method, _, body):
        """
        Consumer callback for the `neon_notifications` exchange. Decodes the
        MQ payload and hands it to `handle_notification`. The exchange only
        carries the three Notification Manager event types, so this bypasses
        iris' per-session dispatch chain (and works with iris releases that
        predate `handle_notification`).
        """
        channel.basic_ack(delivery_tag=method.delivery_tag)
        try:
            payload = b64_to_dict(body)
            message = Message(payload.get("msg_type"), payload.get("data"),
                              payload.get("context"))
        except Exception as e:
            LOG.error("Dropping undecodable notification broadcast: %s", e)
            return
        try:
            self.handle_notification(message)
        except Exception as e:
            LOG.exception(e)

    @staticmethod
    def _parse_notification(message: Message):
        """
        Validate a notification event against its data model.
        @param message: notification event from the bus
        @return: parsed NeonNotificationNotify/Dismiss/Snoozed, or None if the
            message type is unexpected or the payload fails validation
        """
        model = NOTIFICATION_MODELS.get(message.msg_type)
        if model is None:
            LOG.warning("Ignoring unexpected message on %s: %s",
                        NOTIFICATIONS_EXCHANGE, message.msg_type)
            return None
        try:
            return model(data=message.data or {},
                         context=message.context or {})
        except ValidationError as e:
            LOG.warning("Dropping invalid %s payload: %s",
                        message.msg_type, e)
            return None

    def handle_notification(self, message: Message):
        """
        Route a Notification Manager event (`notify`/`dismiss`/`snoozed`) to
        every connected Node it is addressed to: CLIENT scope targets one
        node_id, USER scope every node owned by the target user, GLOBAL scope
        every connected session. Nodes that are offline catch up over REST,
        so a miss is not an error.
        @param message: notification event from the bus
        """
        event = self._parse_notification(message)
        if event is None:
            return
        scope, target, permission = _notification_address(event)
        if scope is None:
            # A `snoozed` event may omit scope/target. An id-only lifecycle
            # event is safe to broadcast; consumers drop ids they do not hold.
            scope = NotificationScope.GLOBAL
        targets = self._notification_targets(scope, target, permission)
        LOG.debug("Routing %s scope=%s target=%s to %s", message.msg_type,
                  scope.name, target, targets)
        for session_id in targets:
            try:
                run(self.send_to_client(message, session_id))
            except Exception as e:
                LOG.error("Failed to deliver %s to %s: %s",
                          message.msg_type, session_id, e)

    def _notification_targets(self, scope: NotificationScope,
                              target: Optional[str],
                              permission: NotificationPermission) -> List[str]:
        """
        Connected sessions a notification may be delivered to: registry
        resolution intersected with a locked snapshot of live sessions.
        PERSONAL and PRIVATE notifications are delivered only to sessions
        owned by the target user; sessions authenticated as a different user
        are withheld.
        """
        with self._session_lock:
            connected = list(self._sessions)
        if scope == NotificationScope.GLOBAL:
            return connected
        candidates = [node_id for node_id in
                      self._node_registry.resolve(scope, target)
                      if node_id in connected]
        if permission < NotificationPermission.PERSONAL:
            return candidates
        return self._owned_by_target_user(candidates, scope, target,
                                          permission)

    def _owned_by_target_user(self, candidates: List[str],
                              scope: NotificationScope, target: Optional[str],
                              permission: NotificationPermission) -> List[str]:
        """
        Restrict `candidates` to sessions owned by the notification's target
        user. With no known owner the notification is withheld entirely.
        """
        target_user = target if scope == NotificationScope.USER else \
            self._node_registry.owner(target)
        if not target_user:
            LOG.warning("Withholding %s notification: no owner known for "
                        "target %s", permission.name, target)
            return []
        allowed = [node_id for node_id in candidates
                   if self._node_registry.owner(node_id) == target_user]
        withheld = sorted(set(candidates) - set(allowed))
        if withheld:
            LOG.warning("Withheld %s notification from sessions not owned by "
                        "%s: %s", permission.name, target_user, withheld)
        return allowed

    def handle_klat_response(self, message: Message):
        """
        Handle a Neon text+audio response to a user input.
        @param message: `klat.response` message from Neon
        """
        try:
            self._update_session_data(message)
            run(self.send_to_client(message))
            session_id = message.context.get('session', {}).get('session_id')
            if stream := self._sessions.get(session_id, {}).get('stream'):
                LOG.info("Stream response audio")
                stream.on_response_audio(message.data)
            LOG.debug(message.context.get("timing"))
        except Exception as e:
            LOG.exception(e)

    def handle_complete_intent_failure(self, message: Message):
        """
        Handle a Neon error response to a user input.
        @param message: `complete.intent.failure` message from Neon
        """
        self._update_session_data(message)
        run(self.send_to_client(message))

    def handle_api_response(self, message: Message):
        """
        Handle a Neon API response to an input.
        @param message: `<msg_type>.response` message from Neon
        """
        if message.msg_type == "neon.audio_input.response":
            LOG.info(message.data.get("transcripts"))
        LOG.debug(message.context.get("timing"))
        run(self.send_to_client(message))

    def handle_error_response(self, message: Message):
        """
        Handle an MQ error response to a user input.
        @param message: `klat.error` response message
        """
        run(self.send_to_client(message))

    def clear_caches(self, message: Message):
        """
        Handle a Neon request to clear cached data.
        @param message: `neon.clear_data` message from Neon
        """
        run(self.send_to_client(message))

    def clear_media(self, message: Message):
        """
        Handle a Neon request to clear media data.
        @param message: `neon.clear_data` message from Neon
        """
        run(self.send_to_client(message))

    def handle_alert(self, message: Message):
        """
        Handle an expired alert from Neon.
        @param message: `neon.alert_expired` message from Neon
        """
        run(self.send_to_client(message))

    async def send_to_client(self, message: Message,
                             session_id: Optional[str] = None):
        """
        Asynchronously forward a message from Neon/MQ to a WebSocket client.
        A client that is not connected is a normal condition (it catches up
        over REST), so a miss is logged rather than raised.
        @param message: Message to forward to a WebSocket client
        @param session_id: Target session; defaults to
            `message.context.session.session_id`
        """
        # TODO: Drop context?
        session_id = session_id or \
            (message.context.get("session") or {}).get("session_id")
        with self._session_lock:
            session = self._sessions.get(session_id)
        if not session:
            LOG.debug("No connected session %s for %s", session_id,
                      message.msg_type)
            return
        await session["socket"].send_text(message.serialize())

    def shutdown(self, *_, **__):
        """
        Shutdown the event loop and prepare this object for destruction.
        """
        loop = get_event_loop()
        loop.call_soon_threadsafe(loop.stop)
        LOG.info("Stopped Event Loop")
        super().shutdown()
