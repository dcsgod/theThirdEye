"""Command-line entry point for quick local validation."""

from __future__ import annotations

import argparse

from .scoring import health_score, health_tier


def main() -> None:
    parser = argparse.ArgumentParser(description="Third Eye health-score utility")
    parser.add_argument("--drift", type=float, default=0.0)
    parser.add_argument("--quality", type=float, default=1.0)
    parser.add_argument("--cost", type=float, default=0.0)
    parser.add_argument("--guardrail", type=float, default=0.0)
    args = parser.parse_args()

    score = health_score(
        drift=args.drift,
        quality=args.quality,
        cost=args.cost,
        guardrail=args.guardrail,
    )
    print(f"health_score={score:.2f} health_tier={health_tier(score)}")


if __name__ == "__main__":
    main()
