from third_eye.scoring import HealthWeights, health_score, health_tier


def test_perfect_health():
    assert health_score(drift=0, quality=1, cost=0, guardrail=0) == 100.0
    assert health_tier(100) == "healthy"


def test_health_score_degrades_with_risk():
    score = health_score(
        drift=0.8,
        quality=0.7,
        cost=0.5,
        guardrail=0.2,
        weights=HealthWeights(criticality=1.5),
    )
    assert 0 <= score < 100
    assert health_tier(score) in {"healthy", "watch", "at_risk", "critical"}
