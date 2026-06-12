"""HITL Gateway schemas."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class CreateApprovalRequest(BaseModel):
    supervisor_trace_id: str
    arbitration_id: str
    self_healing_plan: Dict[str, Any]
    risk_level: str  # medium | high
    requested_by: str = "arbitration_agent"
    summary: str = ""
    expires_in_seconds: int = 300


class ApproverAction(BaseModel):
    user_id: str
    action: str  # approved | rejected
    comment: str = ""
    timestamp: str = ""


class Approval(BaseModel):
    approval_id: str
    supervisor_trace_id: str
    arbitration_id: str
    status: str = "pending"  # pending | approved | rejected | expired
    risk_level: str
    required_approvers: int
    current_approvals: int = 0
    approvers: List[ApproverAction] = Field(default_factory=list)
    summary: str = ""
    self_healing_plan: Dict[str, Any] = Field(default_factory=dict)
    expires_at: str = ""
    created_at: str = ""


class ApproveRequest(BaseModel):
    user_id: str
    comment: str = ""


class RejectRequest(BaseModel):
    user_id: str
    reason: str
