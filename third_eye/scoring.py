"""Portable health-scoring primitives used by The Third Eye.

The Databricks jobs in the repository remain the source of truth for production
execution. This module exposes the deterministic scoring kernel so it can be
reused and tested independently of Databricks.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HealthWeights:
    """Weights for the composite health score."""

    drift: float = 0.35
    quality: float = 0.35
    cost: float = 0.15
    guardrail: float = 0.15
    criticality: float = 1.0


def health_score(
    *,
    drift: float,
    quality: float,
    cost: float,
    guardrail: float,
    weights: HealthWeights = HealthWeights(),
) -> float:
    """Return a 0-100 composite model-health score.

    Inputs are expected to be normalized to [0, 1]. Quality is interpreted as
    a positive quality signal, while the other components represent risk.
    """
    components = (
        weights.drift * drift
        + weights.quality * (1.0 - quality)
        + weights.cost * cost
        + weights.guardrail * guardrail
    )
    return max(0.0, min(100.0, 100.0 - components * weights.criticality))


def health_tier(score: float) -> str:
    """Map a 0-100 score to the standard Third Eye health tier."""
    if score >= 80:
        return "healthy"
    if score >= 60:
        return "watch"
    if score >= 40:
        return "at_risk"
    return "critical"
