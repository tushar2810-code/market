"""
MODULE 7: AI Sentiment Layer

Uses Claude claude-haiku-4-5 to analyze earnings call transcripts, management commentary,
and regulatory filings. Extracts signals Simons never had access to at scale.

This layer is CONFIRMATORY — it boosts or penalizes signals from other modules.
It never generates standalone trades.

Scoring (per transcript/filing):
  1. Management confidence (hedging language vs definitive statements)
  2. Forward guidance quality (specific numbers vs vague optimism)
  3. Capex signals (investing for growth vs conserving cash)
  4. Red flags (unusual accounting, auditor qualifications, related-party emphasis)
  5. Competitive positioning (offensive vs defensive tone)

Score 8+ on 3+ factors → +10/100 to composite
Red flag score 7+      → -20/100 to composite

Caching: Results cached in .tmp/ai_sentiment/ to avoid repeated API calls.

Usage:
    # Analyze a transcript from file
    python3 execution/ai_sentiment_analyzer.py --symbol RELIANCE --file transcript.txt

    # Analyze from stdin
    cat earnings_call.txt | python3 execution/ai_sentiment_analyzer.py --symbol INFY

    # As library
    from ai_sentiment_analyzer import analyze_transcript, get_cached_sentiment
"""

import os
import sys
import json
import hashlib
import argparse
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CACHE_DIR = Path(".tmp/ai_sentiment")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Score thresholds (per the master brief)
POSITIVE_THRESHOLD = 8      # Score >= 8 on 3+ factors → boost
POSITIVE_FACTOR_COUNT = 3
RED_FLAG_THRESHOLD = 7      # Red flag score >= 7 → penalty
POSITIVE_SCORE_CONTRIBUTION = 10   # Points added to composite
NEGATIVE_SCORE_CONTRIBUTION = -20  # Points added to composite (red flag)


ANALYSIS_PROMPT = """Analyse this earnings call transcript or management commentary.
Score each dimension on a scale of 1-10:

1. MANAGEMENT_CONFIDENCE: How definitive are the statements?
   - 1-3: Heavy hedging ("might", "could", "subject to conditions"), vague commitments
   - 4-6: Mixed — some clarity, some hedging
   - 7-10: Definitive statements, specific commitments, clear accountability

2. FORWARD_GUIDANCE_QUALITY: How specific is the forward outlook?
   - 1-3: "Cautiously optimistic", "expect growth", no numbers given
   - 4-6: Directional guidance without specifics
   - 7-10: Specific revenue/margin targets, clear timelines, quantified goals

3. CAPEX_SIGNALS: Is the company investing for growth?
   - 1-3: Cost-cutting mode, asset-light pivot, no expansion plans
   - 4-6: Maintenance capex only
   - 7-10: Significant growth capex announced, capacity expansion, new markets

4. RED_FLAGS: Are there concerning elements?
   - 1-3: None detected — clean, transparent, no unusual items
   - 4-6: Some ambiguity in accounting or related party transactions
   - 7-10: SERIOUS: auditor qualifications, aggressive revenue recognition, excessive related-party,
             management changes during results, sudden accounting policy changes

5. COMPETITIVE_POSITIONING: Is the company playing offense or defense?
   - 1-3: Losing market share, defensive about competitors, citing macro headwinds
   - 4-6: Holding position, neutral competitive commentary
   - 7-10: Taking market share, mentioning specific wins, confident differentiation

Return ONLY a valid JSON object with exactly this structure (no additional text):
{
  "management_confidence": <score 1-10>,
  "forward_guidance_quality": <score 1-10>,
  "capex_signals": <score 1-10>,
  "red_flags": <score 1-10>,
  "competitive_positioning": <score 1-10>,
  "summary": "<one sentence summarizing the key signal>",
  "bull_case": "<one sentence on strongest positive signal>",
  "bear_case": "<one sentence on biggest risk>",
  "overall_sentiment": "<BULLISH|NEUTRAL|BEARISH>"
}"""


def get_cache_key(symbol: str, text: str) -> str:
    """Generate cache key from symbol + content hash."""
    content_hash = hashlib.md5(text[:2000].encode()).hexdigest()[:8]
    return f"{symbol}_{content_hash}"


def analyze_transcript(symbol: str, text: str, model: str = "claude-haiku-4-5-20251001") -> dict:
    """
    Analyze an earnings call transcript using Claude claude-haiku-4-5.

    Args:
        symbol: Stock symbol (e.g., 'RELIANCE')
        text:   Transcript or filing text to analyze
        model:  Claude model ID (default: claude-haiku-4-5 for cost efficiency)

    Returns:
        Dict with scores, summary, and composite sentiment score contribution
    """
    # Check cache first
    cache_key = get_cache_key(symbol, text)
    cache_path = CACHE_DIR / f"{cache_key}.json"
    if cache_path.exists():
        logger.info(f"Sentiment cache hit for {symbol}")
        with open(cache_path) as f:
            return json.load(f)

    # Require Anthropic API key
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set. Cannot run AI sentiment analysis.")
        return _empty_result(symbol, reason="ANTHROPIC_API_KEY not configured")

    try:
        import anthropic
    except ImportError:
        logger.warning("anthropic package not installed. Run: pip install anthropic")
        return _empty_result(symbol, reason="anthropic package not installed")

    # Truncate text to avoid token limits (claude-haiku-4-5 200k context, but keep cost low)
    max_chars = 15000  # ~3750 tokens — enough for a full earnings call Q&A section
    if len(text) > max_chars:
        logger.info(f"Truncating transcript from {len(text)} to {max_chars} chars")
        text = text[:max_chars] + "\n\n[TRANSCRIPT TRUNCATED]"

    try:
        client = anthropic.Anthropic(api_key=api_key)

        message = client.messages.create(
            model=model,
            max_tokens=500,
            messages=[
                {
                    "role": "user",
                    "content": f"COMPANY: {symbol}\n\n{ANALYSIS_PROMPT}\n\nTRANSCRIPT:\n{text}"
                }
            ]
        )

        response_text = message.content[0].text.strip()

        # Parse JSON response
        # Claude haiku sometimes wraps in ```json blocks
        if '```json' in response_text:
            response_text = response_text.split('```json')[1].split('```')[0].strip()
        elif '```' in response_text:
            response_text = response_text.split('```')[1].split('```')[0].strip()

        scores = json.loads(response_text)

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error for {symbol}: {e}\nResponse: {response_text[:200]}")
        return _empty_result(symbol, reason=f"JSON parse error: {e}")
    except Exception as e:
        logger.error(f"Claude API error for {symbol}: {e}")
        return _empty_result(symbol, reason=str(e))

    # Calculate composite score contribution
    composite_contribution = calculate_sentiment_score(scores)

    result = {
        'symbol': symbol,
        'analyzed_at': datetime.now().isoformat(),
        'model': model,
        'scores': {
            'management_confidence': scores.get('management_confidence', 5),
            'forward_guidance_quality': scores.get('forward_guidance_quality', 5),
            'capex_signals': scores.get('capex_signals', 5),
            'red_flags': scores.get('red_flags', 1),  # 1 = no red flags (GOOD)
            'competitive_positioning': scores.get('competitive_positioning', 5),
        },
        'summary': scores.get('summary', ''),
        'bull_case': scores.get('bull_case', ''),
        'bear_case': scores.get('bear_case', ''),
        'overall_sentiment': scores.get('overall_sentiment', 'NEUTRAL'),
        'composite_contribution': composite_contribution,
        'signal_type': (
            'SENTIMENT_POSITIVE' if composite_contribution > 0 else
            'SENTIMENT_RED_FLAG' if composite_contribution < 0 else
            'SENTIMENT_NEUTRAL'
        ),
        'source': 'CLAUDE_API',
    }

    # Cache result
    with open(cache_path, 'w') as f:
        json.dump(result, f, indent=2)

    logger.info(f"Sentiment analysis for {symbol}: {result['overall_sentiment']} | "
                f"Score contribution: {composite_contribution:+d}")
    return result


def calculate_sentiment_score(scores: dict) -> int:
    """
    Map Claude's scores to composite score contribution.

    Logic:
    - Red flag score >= 7 → -20 (immediate penalty, hard override)
    - 3+ positive factors (scores >= 8) → +10
    - Otherwise → 0 (no contribution, this layer is confirmatory only)

    Note: We invert red_flags — a HIGH red_flags score means MANY red flags (bad).
    """
    red_flag = scores.get('red_flags', 1)
    if red_flag >= RED_FLAG_THRESHOLD:
        return NEGATIVE_SCORE_CONTRIBUTION

    # Count positive factors
    positive_factors = [
        scores.get('management_confidence', 5),
        scores.get('forward_guidance_quality', 5),
        scores.get('capex_signals', 5),
        scores.get('competitive_positioning', 5),
    ]
    high_scores = sum(1 for s in positive_factors if s >= POSITIVE_THRESHOLD)

    if high_scores >= POSITIVE_FACTOR_COUNT:
        return POSITIVE_SCORE_CONTRIBUTION

    return 0  # Neutral — no contribution


def get_cached_sentiment(symbol: str):
    """
    Return most recent cached sentiment result for a symbol.
    Returns None if no cached result exists.
    """
    pattern = f"{symbol}_*.json"
    matches = list(CACHE_DIR.glob(pattern))
    if not matches:
        return None
    # Most recently modified
    latest = max(matches, key=lambda p: p.stat().st_mtime)
    with open(latest) as f:
        return json.load(f)


def _empty_result(symbol: str, reason: str = '') -> dict:
    return {
        'symbol': symbol,
        'analyzed_at': datetime.now().isoformat(),
        'model': None,
        'scores': {},
        'summary': reason,
        'composite_contribution': 0,
        'signal_type': 'SENTIMENT_UNAVAILABLE',
        'source': 'FALLBACK',
        'reason': reason,
    }


def print_sentiment_report(result: dict):
    """Pretty-print sentiment analysis result."""
    symbol = result.get('symbol', 'UNKNOWN')
    contrib = result.get('composite_contribution', 0)
    sentiment = result.get('overall_sentiment', 'N/A')

    if result.get('signal_type') == 'SENTIMENT_UNAVAILABLE':
        print(f"  {symbol}: Sentiment analysis unavailable — {result.get('reason', '')}")
        return

    icon = '▲' if contrib > 0 else ('▼' if contrib < 0 else '─')
    print(f"\n  {symbol}: {icon} {sentiment}  |  Score contribution: {contrib:+d}")

    scores = result.get('scores', {})
    if scores:
        print(f"\n  Scores (1-10):")
        for factor, score in scores.items():
            bar = '█' * score + '░' * (10 - score)
            flag = ' [RED FLAG!]' if factor == 'red_flags' and score >= RED_FLAG_THRESHOLD else ''
            print(f"    {factor:<30} {bar} {score}/10{flag}")

    print(f"\n  Summary:    {result.get('summary', 'N/A')}")
    print(f"  Bull case:  {result.get('bull_case', 'N/A')}")
    print(f"  Bear case:  {result.get('bear_case', 'N/A')}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description='Module 7: AI Sentiment Analyzer')
    parser.add_argument('--symbol', type=str, required=True, help='Stock symbol (e.g., RELIANCE)')
    parser.add_argument('--file', type=str, help='Transcript/filing text file path')
    parser.add_argument('--cached', action='store_true', help='Return cached result if available')
    args = parser.parse_args()

    print("╔" + "═" * 78 + "╗")
    print("║  MODULE 7: AI SENTIMENT LAYER (Claude claude-haiku-4-5)".ljust(79) + "║")
    print("║  Earnings call transcript and filing analysis".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    # Check for cached result first
    if args.cached:
        cached = get_cached_sentiment(args.symbol)
        if cached:
            print(f"\n  Using cached analysis from {cached.get('analyzed_at', 'unknown')[:10]}")
            print_sentiment_report(cached)
            sys.exit(0)

    # Read text
    if args.file:
        with open(args.file) as f:
            text = f.read()
    elif not sys.stdin.isatty():
        text = sys.stdin.read()
    else:
        print("  ERROR: Provide --file or pipe transcript text via stdin")
        print("  Example: cat earnings_call.txt | python3 execution/ai_sentiment_analyzer.py --symbol RELIANCE")
        sys.exit(1)

    print(f"\n  Analyzing {args.symbol} ({len(text)} chars)...")
    result = analyze_transcript(args.symbol, text)
    print_sentiment_report(result)
