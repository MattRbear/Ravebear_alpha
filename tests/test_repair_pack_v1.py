import unittest
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# Imports from ALPHA modules (assuming PYTHONPATH is set or running from ALPHA root)
from detectors.wick_detector import detect_wick_events
from utils.aggregation import Candle
from features.vwap import compute_vwap_features, STATE as VWAP_STATE, VWAPState
from features.wick_geometry import compute_wick_geometry
from analysis.scorer import WickScorer
from features import WickFeatures, WickEvent
from feeds.okx_trades import Trade

class TestRepairPackV1(unittest.TestCase):

    def setUp(self):
        # Reset VWAP state
        VWAP_STATE.clear()

    def test_wick_detection_ratio(self):
        """A) Test that wick detector respects the Ratio threshold (not just PCT range)."""
        # Create a candle with 10 range, 4 body, 6 wick.
        # Ratio = 6/4 = 1.5. Should pass min_ratio=1.5
        # Range PCT = 6/10 = 0.6. Should pass old 0.3 check.
        start = datetime.now(timezone.utc)
        c = Candle(
            start_ts=start, end_ts=start+timedelta(minutes=1),
            symbol="TEST",
            open=100, high=110, low=100, close=104, # Body 100-104 (4), Wick 104-110 (6)
            volume=1, buy_volume=1, sell_volume=0, trades=[]
        )
        
        # Test with ratio 1.5 (Should pass)
        events = detect_wick_events(c, wick_min_ratio=1.5)
        self.assertEqual(len(events), 1, "Should detect wick with ratio 1.5")
        self.assertEqual(events[0]['side'], 'upper')

        # Test with ratio 2.0 (Should fail: 1.5 < 2.0)
        events_strict = detect_wick_events(c, wick_min_ratio=2.0)
        self.assertEqual(len(events_strict), 0, "Should NOT detect wick with ratio 2.0")

    def test_vwap_scale_normalization(self):
        """B) Test that VWAP feature outputs 0-100 scale."""
        # Create a state where sigma is 1.0, mean is 100.
        symbol = "VWAP_TEST"
        state = VWAP_STATE[symbol]
        # Hack state: 2 trades at 99 and 101. Mean=100. Var ~ 1.
        # Trade 1: 99
        t1 = Trade(datetime.now(), symbol, 99.0, 1.0, 'buy')
        # Trade 2: 101
        t2 = Trade(datetime.now(), symbol, 101.0, 1.0, 'buy')
        
        compute_vwap_features([t1, t2], datetime.now(), symbol, "sess1", 101.0)
        
        # Now price at 103 (3 sigma away approx)
        feats = compute_vwap_features([], datetime.now(), symbol, "sess1", 103.0)
        score = feats['vwap_mean_reversion_score']
        
        # 103 is 3 units away from mean 100. Sigma approx 1. Z=3.
        # Normalized score should be close to 100.
        # (It depends on exact sigma calc, but should be high positive)
        self.assertTrue(score > 80, f"VWAP score {score} should be high (>80) for 3-sigma deviation")
        self.assertTrue(score <= 100, "VWAP score should be max 100")

    def test_vwap_state_isolation(self):
        """C) Test that BTC trades do not affect ETH VWAP."""
        # Feed BTC
        t_btc = Trade(datetime.now(), "BTC", 50000.0, 1.0, 'buy')
        compute_vwap_features([t_btc], datetime.now(), "BTC", "sess1", 50000.0)
        
        # Check ETH state (should be empty/default)
        feats_eth = compute_vwap_features([], datetime.now(), "ETH", "sess1", 3000.0)
        # If isolated, ETH global vwap should be 3000 (last price fallback) or 0 volume
        # If corrupted by BTC, it might be 50000 or similar
        
        state_eth = VWAP_STATE["ETH"]
        self.assertEqual(state_eth.global_acc.sum_v, 0, "ETH volume should be 0")
        
    def test_oi_units_scorer(self):
        """D) Test that Scorer awards points for fractional OI changes."""
        scorer = WickScorer()
        # Mock features with 1% OI change (0.01)
        feats = WickFeatures()
        feats.oi_change_pct = 0.012 # 1.2%
        
        # Create dummy wick event
        wick = WickEvent(
            ts=datetime.now(), symbol="TEST", timeframe="1m", wick_side="upper",
            wick_high=100, wick_low=90, features=feats
        )
        
        scores = scorer.score_wick(wick)
        breakdown = scores['score_breakdown']
        
        self.assertEqual(breakdown['oi_conviction'], 5, "Should award 5 pts for >1% OI change")

    def test_liquidity_missing_data(self):
        """E) Test that missing liquidity (0 depth) yields 0 points."""
        scorer = WickScorer()
        feats = WickFeatures()
        feats.l5_depth_bid = 0.0
        feats.l5_depth_ask = 0.0 # Missing
        
        wick = WickEvent(
            ts=datetime.now(), symbol="TEST", timeframe="1m", wick_side="upper",
            wick_high=100, wick_low=90, features=feats
        )
        
        scores = scorer.score_wick(wick)
        breakdown = scores['score_breakdown']
        
        self.assertEqual(breakdown['liquidity_density'], 0, "Should award 0 pts for missing liquidity")

    def test_velocity_calculation(self):
        """F) Test velocity calculation."""
        start = datetime.now(timezone.utc)
        end = start + timedelta(seconds=10) # 10s duration
        c = Candle(
            start_ts=start, end_ts=end, symbol="TEST",
            open=100, high=110, low=100, close=100,
            volume=1, buy_volume=0, sell_volume=0, trades=[]
        )
        
        # Wick size = 10 (110-100)
        # Velocity = 10 / 10s = 1.0
        
        geo = compute_wick_geometry(c, "upper")
        self.assertAlmostEqual(geo['rejection_velocity'], 1.0, places=2)

if __name__ == '__main__':
    unittest.main()
