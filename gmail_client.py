import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict, Iterable, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


class GmailError(Exception):
    pass


@dataclass
class GmailAttachment:
    filename: str
    content: bytes
    message_id: str
    received_at: str
    subject: str


class GmailInboxClient:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, user_id: str = "me") -> None:
        if not client_id or not client_secret or not refresh_token:
            raise ValueError("Gmail OAuth client_id, client_secret, and refresh_token are required.")

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[
                "https://www.googleapis.com/auth/gmail.readonly",
                "https://www.googleapis.com/auth/gmail.modify",
                "https://www.googleapis.com/auth/gmail.send",
            ],
        )
        creds.refresh(Request())

        self.user_id = user_id
        self.service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    def fetch_latest_attachment(
        self,
        subject_contains: str,
        allowed_extensions: Iterable[str],
        query: Optional[str] = None,
        max_messages: int = 15,
    ) -> GmailAttachment:
        search_query = query or f'in:inbox subject:"{subject_contains}" has:attachment -label:processed'
        resp = (
            self.service.users()
            .messages()
            .list(userId=self.user_id, q=search_query, maxResults=max_messages)
            .execute()
        )
        messages = resp.get("messages", [])
        if not messages:
            raise GmailError(f"No inbox emails found for query: {search_query}")

        normalized_exts = tuple(ext.lower() for ext in allowed_extensions)

        for msg_ref in messages:
            msg_id = msg_ref.get("id")
            if not msg_id:
                continue

            msg = (
                self.service.users()
                .messages()
                .get(userId=self.user_id, id=msg_id, format="full")
                .execute()
            )

            payload = msg.get("payload", {})
            headers = payload.get("headers", [])
            subject = self._header_value(headers, "Subject")
            internal_date = msg.get("internalDate")
            received_at = self._format_internal_date(internal_date)

            for part in self._iter_parts(payload):
                filename = str(part.get("filename") or "").strip()
                if not filename:
                    continue
                if not filename.lower().endswith(normalized_exts):
                    continue

                body = part.get("body", {})
                att_id = body.get("attachmentId")
                if not att_id:
                    continue

                attachment = (
                    self.service.users()
                    .messages()
                    .attachments()
                    .get(userId=self.user_id, messageId=msg_id, id=att_id)
                    .execute()
                )
                data = attachment.get("data")
                if not data:
                    continue

                raw = self._decode_base64url(data)
                return GmailAttachment(
                    filename=filename,
                    content=raw,
                    message_id=msg_id,
                    received_at=received_at,
                    subject=subject,
                )

        raise GmailError(
            f"Found matching emails but no supported attachment types ({', '.join(normalized_exts)})."
        )

    def ensure_label(self, label_name: str) -> str:
        label_name = label_name.strip()
        if not label_name:
            raise GmailError("Label name must not be empty.")

        resp = self.service.users().labels().list(userId=self.user_id).execute()
        labels = resp.get("labels", [])
        for label in labels:
            if str(label.get("name", "")).strip().lower() == label_name.lower():
                return str(label.get("id", ""))

        created = (
            self.service.users()
            .labels()
            .create(
                userId=self.user_id,
                body={
                    "name": label_name,
                    "labelListVisibility": "labelShow",
                    "messageListVisibility": "show",
                },
            )
            .execute()
        )
        label_id = str(created.get("id", ""))
        if not label_id:
            raise GmailError(f"Could not create label '{label_name}'.")
        return label_id

    def mark_read_and_label(self, message_id: str, label_id: str) -> None:
        if not message_id:
            raise GmailError("message_id is required.")
        if not label_id:
            raise GmailError("label_id is required.")

        self.service.users().messages().modify(
            userId=self.user_id,
            id=message_id,
            body={"removeLabelIds": ["UNREAD"], "addLabelIds": [label_id]},
        ).execute()

    def send_email(
        self,
        to_email: str,
        subject: str,
        body_text: str,
        attachments: Optional[Dict[str, bytes]] = None,
    ) -> str:
        if not to_email.strip():
            raise GmailError("Recipient email is required.")

        msg = EmailMessage()
        msg["To"] = to_email.strip()
        msg["Subject"] = subject.strip()
        msg.set_content(body_text)

        for filename, content in (attachments or {}).items():
            msg.add_attachment(
                content,
                maintype="text",
                subtype="csv",
                filename=filename,
            )

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        sent = (
            self.service.users()
            .messages()
            .send(userId=self.user_id, body={"raw": raw})
            .execute()
        )
        return str(sent.get("id", ""))

    @staticmethod
    def _header_value(headers: Iterable[Dict[str, Any]], key: str) -> str:
        target = key.lower()
        for h in headers:
            if str(h.get("name", "")).lower() == target:
                return str(h.get("value", ""))
        return ""

    @staticmethod
    def _format_internal_date(internal_date: Optional[str]) -> str:
        if not internal_date:
            return ""
        try:
            dt = datetime.fromtimestamp(int(internal_date) / 1000, tz=timezone.utc)
            return dt.isoformat()
        except Exception:
            return ""

    @staticmethod
    def _iter_parts(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        stack = [payload]
        while stack:
            current = stack.pop()
            parts = current.get("parts", [])
            if parts:
                stack.extend(parts)
            else:
                yield current

    @staticmethod
    def _decode_base64url(data: str) -> bytes:
        padding = "=" * ((4 - len(data) % 4) % 4)
        return base64.urlsafe_b64decode(data + padding)
