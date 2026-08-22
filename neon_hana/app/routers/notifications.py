# NEON AI (TM) SOFTWARE, Software Development Kit & Application Development System
# All trademark and other rights reserved by their respective owners
# Copyright 2008-2026 Neongecko.com Inc.
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

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from neon_data_models.enum import NotificationState
from neon_data_models.models.api.http import (
    NotificationDismissResponse, NotificationInteractionRequest,
    NotificationInteractionResponse, NotificationListResponse,
    NotificationSnoozeRequest, NotificationSnoozeResponse)

from neon_hana.app.dependencies import jwt_bearer, mq_connector, node_registry
from neon_hana.mq_service_api import NotificationRequester

notifications_route = APIRouter(
    prefix="/notifications", tags=["notifications"],
    dependencies=[Depends(jwt_bearer)]
)

# Notification Manager `status` values that map onto HTTP errors
_REFUSED_STATUS_CODES = {"refused": 403, "not_found": 404}


def _requester(token: str) -> NotificationRequester:
    """
    Identity of the caller from its JWT: the `client_id` claim is the static
    node_id and `sub` is the user_id.
    """
    hana_token = jwt_bearer.client_manager.get_token_data(token)
    return NotificationRequester(hana_token.client_id, hana_token.sub)


def _raise_if_refused(data: dict):
    """
    Surface a Notification Manager refusal as the matching HTTP error.
    """
    status = data.get("status")
    if status in _REFUSED_STATUS_CODES:
        raise HTTPException(status_code=_REFUSED_STATUS_CODES[status],
                            detail=data.get("reason") or
                            f"Notification request {status}")


@notifications_route.get("")
async def list_notifications(
    since: Optional[datetime] = Query(
        default=None,
        description="Only return notifications created or updated after "
                    "this ISO 8601 timestamp. Pass the previous response's "
                    "`server_time`"),
    state: Optional[NotificationState] = Query(
        default=None,
        description="Optional lifecycle state filter. Omit it for catch-up "
                    "so that dismissed/expired tombstones are returned in "
                    "`states`"),
    token: str = Depends(jwt_bearer),
) -> NotificationListResponse:
    """
    Catch-up for Nodes that were offline.

    Returns the union of GLOBAL, USER (the caller's user_id) and CLIENT
    (the caller's node_id) notifications, permission-filtered by the
    Notification Manager. Without a `state` filter the response includes
    recently dismissed/expired notifications as tombstones in `states`, so
    a client can reconcile local state. Clients deduplicate against
    WebSocket pushes by `notification_id`.
    """
    requester = _requester(token)
    listed = await mq_connector.list_notifications(requester, since, state)
    node_registry.touch_catch_up(requester.node_id)
    return NotificationListResponse(
        notifications=listed.notifications,
        states=listed.states,
        server_time=datetime.now(timezone.utc))


@notifications_route.post("/{notification_id}/dismiss")
async def dismiss_notification(
    notification_id: str,
    token: str = Depends(jwt_bearer),
) -> NotificationDismissResponse:
    """
    Dismiss a notification. The Notification Manager is authoritative: a
    notification with `removable_by_user=False`, or a caller outside the
    dismiss allowlist, is refused with a reason (HTTP 403).
    """
    data = await mq_connector.remove_notification(_requester(token),
                                                  notification_id)
    _raise_if_refused(data)
    return NotificationDismissResponse(notification_id=notification_id,
                                       status="dismissed")


@notifications_route.post("/{notification_id}/snooze")
async def snooze_notification(
    notification_id: str,
    request: NotificationSnoozeRequest,
    token: str = Depends(jwt_bearer),
) -> NotificationSnoozeResponse:
    """
    Hide a notification for `duration` seconds. The Notification Manager owns
    the snooze timer and re-emits the notification at `renotify_at`.
    """
    data = await mq_connector.snooze_notification(
        _requester(token), notification_id, request.duration)
    _raise_if_refused(data)
    renotify_at = data.get("renotify_at")
    if renotify_at is None:
        raise HTTPException(
            status_code=504,
            detail="Notification Manager reported no `renotify_at`")
    return NotificationSnoozeResponse(notification_id=notification_id,
                                      renotify_at=renotify_at)


@notifications_route.post("/{notification_id}/interaction", status_code=202)
async def notification_interaction(
    notification_id: str,
    request: NotificationInteractionRequest,
    token: str = Depends(jwt_bearer),
) -> NotificationInteractionResponse:
    """
    Report that the user activated a notification action. The interaction is
    emitted on the bus for the producing skill; this endpoint does not wait
    for the producer to handle it.
    """
    await mq_connector.send_notification_interaction(
        _requester(token), notification_id, request.action_id,
        request.callback_data)
    return NotificationInteractionResponse(notification_id=notification_id,
                                           status="accepted")
