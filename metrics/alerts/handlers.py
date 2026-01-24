"""Alert handlers for different notification channels."""

from __future__ import annotations

import json
import logging
import smtplib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


class Severity(str, Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Alert data structure."""

    rule_name: str
    message: str
    severity: Severity
    metric_name: str
    metric_value: Any
    threshold: Any
    dataset: str | None = None
    layer: str | None = None
    timestamp: datetime | None = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "rule_name": self.rule_name,
            "message": self.message,
            "severity": self.severity.value,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "dataset": self.dataset,
            "layer": self.layer,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }


class AlertHandler(ABC):
    """Base class for alert handlers."""

    def __init__(self, severity_threshold: Severity = Severity.INFO):
        self.severity_threshold = severity_threshold

    @abstractmethod
    def send(self, alert: Alert) -> bool:
        """
        Send an alert.

        Args:
            alert: Alert to send

        Returns:
            True if sent successfully
        """
        pass

    def should_send(self, alert: Alert) -> bool:
        """Check if alert meets severity threshold."""
        severity_order = [Severity.INFO, Severity.WARNING, Severity.ERROR, Severity.CRITICAL]
        return severity_order.index(alert.severity) >= severity_order.index(self.severity_threshold)


class ConsoleAlertHandler(AlertHandler):
    """Console/logging alert handler."""

    def send(self, alert: Alert) -> bool:
        """Log alert to console."""
        if not self.should_send(alert):
            return False

        log_level = {
            Severity.INFO: logging.INFO,
            Severity.WARNING: logging.WARNING,
            Severity.ERROR: logging.ERROR,
            Severity.CRITICAL: logging.CRITICAL,
        }.get(alert.severity, logging.INFO)

        logger.log(
            log_level,
            f"[ALERT] [{alert.severity.value.upper()}] {alert.message} "
            f"(metric={alert.metric_name}, value={alert.metric_value})",
        )
        return True


class SlackAlertHandler(AlertHandler):
    """Slack webhook alert handler."""

    def __init__(
        self,
        webhook_url: str,
        channel: str | None = None,
        severity_threshold: Severity = Severity.WARNING,
    ):
        super().__init__(severity_threshold)
        self.webhook_url = webhook_url
        self.channel = channel

    def send(self, alert: Alert) -> bool:
        """Send alert to Slack."""
        if not self.should_send(alert):
            return False

        if not self.webhook_url:
            logger.warning("Slack webhook URL not configured")
            return False

        color = {
            Severity.INFO: "#36a64f",
            Severity.WARNING: "#ff9800",
            Severity.ERROR: "#f44336",
            Severity.CRITICAL: "#9c27b0",
        }.get(alert.severity, "#808080")

        payload = {
            "attachments": [
                {
                    "color": color,
                    "title": f"[{alert.severity.value.upper()}] {alert.rule_name}",
                    "text": alert.message,
                    "fields": [
                        {"title": "Metric", "value": alert.metric_name, "short": True},
                        {"title": "Value", "value": str(alert.metric_value), "short": True},
                        {"title": "Threshold", "value": str(alert.threshold), "short": True},
                        {"title": "Dataset", "value": alert.dataset or "N/A", "short": True},
                    ],
                    "footer": "DC Data Platform",
                    "ts": int(alert.timestamp.timestamp()) if alert.timestamp else None,
                }
            ]
        }

        if self.channel:
            payload["channel"] = self.channel

        try:
            req = Request(
                self.webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=10) as response:
                return response.status == 200
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False


class EmailAlertHandler(AlertHandler):
    """Email alert handler."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        from_address: str,
        to_addresses: list[str],
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = True,
        severity_threshold: Severity = Severity.ERROR,
    ):
        super().__init__(severity_threshold)
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.from_address = from_address
        self.to_addresses = to_addresses
        self.username = username
        self.password = password
        self.use_tls = use_tls

    def send(self, alert: Alert) -> bool:
        """Send alert via email."""
        if not self.should_send(alert):
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[{alert.severity.value.upper()}] DC Data Platform Alert: {alert.rule_name}"
        msg["From"] = self.from_address
        msg["To"] = ", ".join(self.to_addresses)

        text = f"""
DC Data Platform Alert

Severity: {alert.severity.value.upper()}
Rule: {alert.rule_name}
Message: {alert.message}

Details:
- Metric: {alert.metric_name}
- Value: {alert.metric_value}
- Threshold: {alert.threshold}
- Dataset: {alert.dataset or 'N/A'}
- Layer: {alert.layer or 'N/A'}
- Time: {alert.timestamp.isoformat() if alert.timestamp else 'N/A'}
"""

        html = f"""
<html>
<body>
<h2>DC Data Platform Alert</h2>
<p><strong>Severity:</strong> {alert.severity.value.upper()}</p>
<p><strong>Rule:</strong> {alert.rule_name}</p>
<p><strong>Message:</strong> {alert.message}</p>
<h3>Details</h3>
<table>
<tr><td>Metric</td><td>{alert.metric_name}</td></tr>
<tr><td>Value</td><td>{alert.metric_value}</td></tr>
<tr><td>Threshold</td><td>{alert.threshold}</td></tr>
<tr><td>Dataset</td><td>{alert.dataset or 'N/A'}</td></tr>
<tr><td>Layer</td><td>{alert.layer or 'N/A'}</td></tr>
<tr><td>Time</td><td>{alert.timestamp.isoformat() if alert.timestamp else 'N/A'}</td></tr>
</table>
</body>
</html>
"""

        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                if self.username and self.password:
                    server.login(self.username, self.password)
                server.sendmail(self.from_address, self.to_addresses, msg.as_string())
            return True
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False


# Default handlers
_handlers: list[AlertHandler] = [ConsoleAlertHandler()]


def register_handler(handler: AlertHandler) -> None:
    """Register an alert handler."""
    _handlers.append(handler)


def send_alert(alert: Alert) -> int:
    """
    Send alert to all registered handlers.

    Returns:
        Number of handlers that successfully sent the alert
    """
    success_count = 0
    for handler in _handlers:
        if handler.send(alert):
            success_count += 1
    return success_count
