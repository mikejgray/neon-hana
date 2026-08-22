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

from asyncio import Event
from signal import signal, SIGINT
from typing import Optional, Union

from fastapi import APIRouter, WebSocket, HTTPException
from ovos_utils import LOG
from starlette.websockets import WebSocketDisconnect

from neon_hana.app.dependencies import config, client_manager, node_registry
from neon_hana.mq_websocket_api import MQWebsocketAPI, ClientNotKnown

from neon_data_models.enum import AccessRoles
from neon_data_models.models.user.database import PermissionsConfig
from neon_data_models.models.api.node_v1 import (NodeAudioInput, NodeGetStt,
                                                 NodeGetTts, NodeKlatResponse,
                                                 NodeAudioInputResponse,
                                                 NodeGetSttResponse,
                                                 NodeGetTtsResponse,
                                                 NodeHello,
                                                 NodeInvokeNative,
                                                 NodeInvokeNativeResponse,
                                                 CoreWWDetected,
                                                 CoreIntentFailure,
                                                 CoreErrorResponse,
                                                 CoreClearData,
                                                 CoreAlertExpired,
                                                 CoreNotification,
                                                 CoreNotificationDismiss,
                                                 CoreNotificationSnoozed,
                                                 NodeNotificationRemove,
                                                 NodeNotificationSnooze,
                                                 NodeNotificationInteraction,
                                                 NodeNotificationSync)

node_route = APIRouter(prefix="/node", tags=["node"])

socket_api = MQWebsocketAPI(config, node_registry)
signal(SIGINT, socket_api.shutdown)


@node_route.websocket("/v1")
async def node_v1_endpoint(websocket: WebSocket, token: str):
    client_id = client_manager.get_client_id(token)
    if not client_manager.validate_auth(token, client_id):
        raise HTTPException(status_code=403,
                            detail="Invalid or expired token.")
    token_data = client_manager.get_token_data(token)
    permissions = PermissionsConfig.from_roles(token_data.roles)
    if not any((permissions.node > AccessRoles.GUEST,
                permissions.node == AccessRoles.NODE,
                config.get("disable_auth"))):
        raise HTTPException(status_code=401,
                            detail=f"Client not authorized for node access "
                                   f"({client_id})")
    # JWT claims are authoritative for node identity and ownership
    node_registry.upsert_connect(client_id, token_data.sub)
    await websocket.accept()
    disconnect_event = Event()

    socket_api.new_connection(websocket, client_id)
    while not disconnect_event.is_set():
        try:
            client_in: dict = await websocket.receive_json()
            socket_api.handle_client_input(client_in, client_id)
        except WebSocketDisconnect:
            disconnect_event.set()
    socket_api.end_session(session_id=client_id)


@node_route.websocket("/v1/stream")
async def node_v1_stream_endpoint(websocket: WebSocket, token: str):
    client_id = client_manager.get_client_id(token)

    if not client_manager.check_connect_stream():
        raise HTTPException(status_code=503,
                            detail=f"Server is not accepting any more streams")
    try:
        await websocket.accept()
        disconnect_event = Event()
        socket_api.new_stream(websocket, client_id)
        while not disconnect_event.is_set():
            try:
                client_in: bytes = await websocket.receive_bytes()
                socket_api.handle_audio_input_stream(client_in, client_id)
            except WebSocketDisconnect:
                disconnect_event.set()
    except ClientNotKnown as e:
        LOG.error(e)
        raise HTTPException(status_code=401,
                            detail=f"Client not known ({client_id})")
    except Exception as e:
        LOG.exception(e)
    finally:
        client_manager.disconnect_stream()


@node_route.get("/v1/doc")
async def node_v1_doc(_: Optional[Union[NodeAudioInput, NodeGetStt,
                                        NodeGetTts, NodeHello,
                                        NodeInvokeNativeResponse,
                                        NodeNotificationRemove,
                                        NodeNotificationSnooze,
                                        NodeNotificationInteraction,
                                        NodeNotificationSync]]) -> \
        Optional[Union[NodeKlatResponse, NodeAudioInputResponse,
                       NodeGetSttResponse, NodeGetTtsResponse,
                       NodeInvokeNative,
                       CoreWWDetected, CoreIntentFailure, CoreErrorResponse,
                       CoreClearData, CoreAlertExpired,
                       CoreNotification, CoreNotificationDismiss,
                       CoreNotificationSnoozed]]:
    """
    The node endpoint (`/node/v1`) accepts and returns JSON objects representing
    Messages. All inputs and responses will contain keys:
    `msg_type`, `data`, `context`. Only the inputs and responses documented here
    are explicitly supported. Other messages sent or received on this socket are
    not guaranteed to be stable.
    """
    pass


@node_route.get("/v1/stream/doc")
async def node_v1_stream_doc():
    """
    The stream endpoint accepts input audio as raw bytes. It expects inputs to
    have:
        - sample_rate=16000
        - sample_width=2
        - sample_channels=1
        - chunk_size=4096

    Response audio is WAV audio as raw bytes, with each message containing one
    full audio file. A client should queue all responses for playback.

    Any client accessing the stream endpoint (`/node/v1/stream`), must first
    establish a connection to the node endpoint (`/node/v1`).
    """
    pass
