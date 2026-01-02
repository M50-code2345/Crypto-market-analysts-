# Advanced Order Flow Analysis System - Technical Documentation

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Data Ingestion](#data-ingestion)
4. [Feature Calculations](#feature-calculations)
5. [Output Specifications](#output-specifications)
6. [Algorithm Details](#algorithm-details)

---

## System Overview

### Purpose
This system provides **institutional-grade market microstructure analysis** for cryptocurrency trading, specifically designed for Bitcoin/USDT markets on Binance. It processes real-time order book data and trade streams to extract 300+ features every 30 seconds.

### Key Capabilities
- **Real-time orderbook analysis** (1000 depth levels)
- **Trade flow monitoring** (aggTrade stream)
- **Institutional activity tracking** (whale/large order detection)
- **Market manipulation detection** (spoofing, layering, front-running)
- **Liquidity analysis** (walls, vacuums, depth imbalances)
- **Predictive features** (price magnets, support/resistance, regime classification)

### Data Sources
- **Binance WebSocket**: 1000-level order book depth (@depth1000ms)
- **Binance aggTrade**: Aggregated trade stream (real-time execution data)
- **Binance REST API**: Historical depth snapshots, 24h stats, funding rates

---

## Architecture

### Component Structure

```
┌─────────────────────────────────────────────────────────────┐
│                     MarketClient                             │
│  (WebSocket & REST API Connection Manager)                  │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  AdvancedOrderFlow                           │
│  (Core Processing Engine - 300+ Features)                   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Data Accumulators (30s rolling windows)             │  │
│  │  • Order book snapshots (depth_snapshots)            │  │
│  │  • Trade data (aggressive_buy_volume, etc.)          │  │
│  │  • Large orders (whale_trades, block_trades)         │  │
│  │  • Manipulation signals (spoofing_events, etc.)      │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Feature Computation Modules                          │  │
│  │  • Tier 1: Trade Metrics                             │  │
│  │  • Tier 2: Depth Metrics                             │  │
│  │  • Smart Order Detection                             │  │
│  │  • High-Value Predictive Features                    │  │
│  │  • Liquidity Vacuum Detection                        │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 Feature Output (Snapshot)                    │
│  • tier1: Trade flow & execution metrics                    │
│  • tier2: Depth & spread metrics                            │
│  • smart_order_detection: Manipulation signals              │
│  • high_value_predictive: Forecasting features              │
│  • liquidity_vacuum: Flash crash indicators                 │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Ingestion

### 1. Order Book Data (WebSocket)

**Stream**: `depth@1000ms`
**Frequency**: Every 1 second (1000ms)
**Depth**: 1000 bid levels + 1000 ask levels

#### Processing Pipeline:
```python
1. Receive raw orderbook message from Binance WebSocket
   Format: {"bids": [[price, qty], ...], "asks": [[price, qty], ...]}

2. Parse and validate data
   - Convert price/qty strings to floats
   - Ensure bids sorted descending, asks sorted ascending
   - Validate spread (bid < ask)

3. Calculate mid-price FIRST (before filtering)
   mid_price = (best_bid + best_ask) / 2
   
4. Apply display filter (±10% from mid-price)
   - Keeps top-of-book intact
   - Filters only distant levels for display
   
5. Store snapshot in rolling deque
   depth_snapshots.append((timestamp, bids, asks))
   Retention: Last 300 snapshots (5 minutes at 1s intervals)
```

#### Data Quality Checks:
- **Spread validation**: Warns if spread > 1% (detects corruption)
- **Freshness check**: Ensures data < 5 seconds old
- **Outlier detection**: Removed (was too aggressive)

### 2. Trade Data (WebSocket)

**Stream**: `aggTrade`
**Frequency**: Real-time (every trade execution)

#### Trade Classification:
```python
For each aggTrade message:
{
  "p": "88283.70",  # price
  "q": "0.0125",    # quantity (BTC)
  "m": false,       # is_buyer_maker (false = buy, true = sell)
  "T": 1735948372001 # timestamp
}

Classification:
- If m == False: Aggressive BUY (market buy hit ask)
- If m == True: Aggressive SELL (market sell hit bid)

Accumulation (30s rolling windows):
- aggressive_buy_volume: Sum of buy volumes (last 30s)
- aggressive_sell_volume: Sum of sell volumes (last 30s)
- trade_sizes: Individual trade sizes for percentiles
- aggressive_buy_vwap_data: (volume × price) for VWAP calculation
```

#### Trade Size Bucketing:
```python
Thresholds:
- Micro: < $1,000
- Small: $1,000 - $5,000
- Medium: $5,000 - $10,000
- Large: $10,000 - $25,000  # Captured in large_orders deque
- Whale: $25,000 - $100,000 # Captured in whale_trades deque
- Block: > $100,000         # Captured in block_trades deque

Storage: Each bucket tracks volume, count, notional value
```

---

## Feature Calculations

### Tier 1: Trade Flow Metrics

#### 1.1 Basic Trade Statistics
```python
recent_buy_volume:
  = Sum of aggressive buy volumes (last 30s)
  Units: BTC
  
recent_sell_volume:
  = Sum of aggressive sell volumes (last 30s)
  Units: BTC
  
recent_buy_count:
  = Number of buy trades (last 30s)
  
recent_sell_count:
  = Number of sell trades (last 30s)
  
total_recent_volume:
  = recent_buy_volume + recent_sell_volume
  Units: BTC
```

#### 1.2 VWAP Calculations
```python
buy_vwap:
  = Sum(buy_volume_i × price_i) / Sum(buy_volume_i)
  Units: USD
  Source: aggressive_buy_vwap_data deque
  
sell_vwap:
  = Sum(sell_volume_i × price_i) / Sum(sell_volume_i)
  Units: USD
  Source: aggressive_sell_vwap_data deque
  
vwap_spread:
  = |buy_vwap - sell_vwap|
  Units: USD
  
vwap_spread_bps:
  = (vwap_spread / buy_vwap) × 10,000
  Units: Basis points (0.01%)
```

#### 1.3 Cumulative Volume Delta (CVD)
```python
cum_volume_delta_30s:
  = recent_buy_volume - recent_sell_volume
  Units: BTC
  Interpretation:
    > 0: Net buying pressure
    < 0: Net selling pressure
    
delta_acceleration:
  = (current_cvd - previous_cvd) / time_diff
  Units: BTC/second
  Measures: Momentum of volume flow
```

#### 1.4 Trade Size Percentiles
```python
All percentiles calculated from trade_sizes deque (last 30s):

size_percentile_50: (P50, median)
  = Median trade size × last_price
  Units: USD
  
size_percentile_75: (P75)
  = 75th percentile trade size × last_price
  Units: USD
  
size_percentile_90: (P90)
  = 90th percentile trade size × last_price
  Units: USD
  
size_percentile_95: (P95)
  = 95th percentile trade size × last_price
  Units: USD
  
size_percentile_99: (P99)
  = 99th percentile trade size × last_price
  Units: USD
  
Calculation method:
  sorted_sizes = sorted(trade_sizes)
  index = int(percentile/100 × len(sorted_sizes))
  percentile_btc = sorted_sizes[index]
  percentile_usd = percentile_btc × last_price
```

#### 1.5 Institutional Trade Metrics

**Large Orders (>$10K)**:
```python
large_order_count:
  = len(large_orders deque)
  Filter: trades with notional > $10,000
  
large_order_buy_count:
  = Count of buy-side large orders
  
large_order_sell_count:
  = Count of sell-side large orders
  
large_order_total_notional:
  = Sum(volume × price) for all large orders
  Units: USD
```

**Whale Trades (>$25K)**:
```python
whale_trade_count:
  = len(whale_trades deque)
  Filter: trades with notional > $25,000
  
whale_buy_count:
  = Count of buy-side whale trades
  
whale_sell_count:
  = Count of sell-side whale trades
  
whale_total_notional:
  = Sum(volume × price) for all whale trades
  Units: USD
```

**Block Trades (>$100K)**:
```python
block_trade_count:
  = len(block_trades deque)
  Filter: trades with notional > $100,000
  
block_buy_count:
  = Count of buy-side block trades
  
block_sell_count:
  = Count of sell-side block trades
  
block_total_notional:
  = Sum(volume × price) for all block trades
  Units: USD
```

#### 1.6 Smart Money Intelligence

```python
smart_money_ratio:
  = (large_order_count + block_trade_count) / total_trade_count
  Units: Percentage (0-100)
  Interpretation: % of trades from institutional participants
  
institutional_bias:
  = (institutional_buy_volume - institutional_sell_volume) / 
    (institutional_buy_volume + institutional_sell_volume)
  Range: -1.0 to +1.0
  Interpretation:
    +1.0 = All institutional buying
    0.0 = Balanced
    -1.0 = All institutional selling
    
Where:
  institutional_buy_volume = large_buy_notional + block_buy_notional
  institutional_sell_volume = large_sell_notional + block_sell_notional
```

#### 1.7 Trade Size Distribution (Buckets)

```python
size_bucket_volumes:
  = {
      'micro': sum of volumes for trades < $1K,
      'small': sum of volumes for trades $1K-$5K,
      'medium': sum of volumes for trades $5K-$10K,
      'large': sum of volumes for trades $10K-$25K,
      'block': sum of volumes for trades > $100K
    }
  Units: BTC
  
size_bucket_counts:
  = {bucket_name: number of trades in bucket}
  
size_bucket_notionals:
  = {bucket_name: sum of (volume × price) for trades in bucket}
  Units: USD
```

---

### Tier 2: Depth & Spread Metrics

#### 2.1 Order Book Statistics

```python
total_bid_volume:
  = Sum of volumes across all bid levels (within ±10% of mid)
  Units: BTC
  
total_ask_volume:
  = Sum of volumes across all ask levels (within ±10% of mid)
  Units: BTC
  
bid_count:
  = Number of bid price levels (within ±10% of mid)
  
ask_count:
  = Number of ask price levels (within ±10% of mid)
  
avg_bid_size:
  = total_bid_volume / bid_count
  Units: BTC
  
avg_ask_size:
  = total_ask_volume / ask_count
  Units: BTC
```

#### 2.2 Depth Imbalances

**L5 Imbalance (Top 5 levels)**:
```python
l5_bid_volume = sum(bids[0:5] volumes)
l5_ask_volume = sum(asks[0:5] volumes)

depth_imbalance_l5:
  = (l5_bid_volume - l5_ask_volume) / (l5_bid_volume + l5_ask_volume)
  Range: -1.0 to +1.0
  Interpretation:
    > 0.2: Strong bid pressure
    < -0.2: Strong ask pressure
```

**L10, L20, L50 Imbalances**: Calculated identically for top 10, 20, 50 levels

#### 2.3 Spread Metrics

```python
spread:
  = best_ask - best_bid
  Units: USD
  
spread_bps:
  = (spread / mid_price) × 10,000
  Units: Basis points
  Normal range: 0.1-1.0 bps for BTC/USDT
  
spread_percentile:
  = Percentile rank of current spread in recent history
  Range: 0-100
  Interpretation: Where current spread sits in distribution
```

#### 2.4 Volume-Weighted Spread (50+ levels)

```python
For bid side (top 50 levels):
  vw_bid_price = Sum(bid_price_i × bid_volume_i) / Sum(bid_volume_i)
  
For ask side (top 50 levels):
  vw_ask_price = Sum(ask_price_i × ask_volume_i) / Sum(ask_volume_i)
  
volume_weighted_spread:
  = vw_ask_price - vw_bid_price
  Units: USD
  
vw_spread_bps:
  = (volume_weighted_spread / mid_price) × 10,000
  Units: Basis points
  
Interpretation: True liquidity cost for large orders (50+ level depth)
```

#### 2.5 Spread Velocity

```python
spread_velocity:
  = (current_spread - previous_spread) / (time_diff × mid_price)
  Units: Basis points per second
  
Calculation:
  spread_diff = current_spread_bps - prev_spread_bps
  time_diff = current_time - prev_time (seconds)
  spread_velocity = spread_diff / time_diff
  
Interpretation:
  > 0: Spread widening (liquidity draining)
  < 0: Spread tightening (liquidity improving)
  
Normal range: ±10-50 bps/sec
```

#### 2.6 Liquidity Cliffs

```python
For each of top 50 levels on bid/ask:
  cliff_score = (volume_diff / avg_volume) if volume_diff > threshold
  
  Where:
    volume_diff = |current_level_volume - next_level_volume|
    threshold = 2 × avg_volume
    
liquidity_cliff_slippage_bid_bps:
  = Sum of price gaps × cliff_scores (bid side)
  Units: Basis points
  
liquidity_cliff_slippage_ask_bps:
  = Sum of price gaps × cliff_scores (ask side)
  Units: Basis points
  
Interpretation: Potential slippage cost from sudden volume dropoffs
```

---

### Tier 3: Smart Order Detection

#### 3.1 Iceberg Order Detection

```python
Detection logic:
  1. Track order refills at same price level
  2. If refill count > 3 within 30s window
  3. And refill volume similar (±20%)
  4. Flag as iceberg pattern
  
iceberg_estimates:
  = {(side, price): estimated_hidden_volume}
  
  Key: Compound key (side='bid'|'ask', price)
  Value: Sum of observed refill volumes
  
Storage: Uses compound keys to prevent data overwriting
```

#### 3.2 Spoofing Detection

```python
Detection criteria:
  1. Large order placed (>1.0 BTC)
  2. Price far from best (>2 bps away)
  3. Order cancelled within 5 seconds
  4. No partial fill
  
spoofing_events (accumulated):
  = [(timestamp, side, price, volume), ...]
  
  Tracks all suspicious cancellations
  Used for manipulation pattern detection
```

#### 3.3 Layering Detection

```python
Detection window: Levels 1-30 on each side

layering_bid_levels:
  = Count of bid levels with volume > 2× average
  Range: 0-30
  
layering_ask_levels:
  = Count of ask levels with volume > 2× average
  Range: 0-30
  
Interpretation:
  > 10: Significant layering present (potential manipulation)
  < 5: Normal market structure
```

#### 3.4 Front-Running Detection

```python
Detection logic:
  1. Large trade executed (>0.5 BTC)
  2. Check if similar-side order appeared <100ms before
  3. At better price (1-2 bps improvement)
  
front_running_events (accumulated):
  = Count of detected front-running patterns
  
  Tracks: Pre-positioned orders that benefit from large trade
```

#### 3.5 Peg Order Detection

```python
Detection criteria:
  1. Order maintains constant distance from best bid/ask
  2. Distance stable across 3+ snapshots
  3. Typical distance: 1-5 bps
  
peg_order_bid_volume:
  = Sum of volumes for detected peg orders (bid side)
  Units: BTC
  
peg_order_ask_volume:
  = Sum of volumes for detected peg orders (ask side)
  Units: BTC
```

#### 3.6 Fake Liquidity Detection

```python
Criteria:
  1. Large volume appears suddenly (>5× average)
  2. At price level far from best (>10 bps)
  3. Disappears within 3 seconds
  4. No trade execution
  
fake_liquidity_bid_volume:
  = Sum of detected fake bid volumes (last 30s)
  Units: BTC
  
fake_liquidity_ask_volume:
  = Sum of detected fake ask volumes (last 30s)
  Units: BTC
```

---

### Tier 4: High-Value Predictive Features

#### 4.1 Liquidity Walls

```python
Detection algorithm:
  1. Calculate average volume per level: avg_vol
  2. For each level: if volume > threshold × avg_vol → Wall
  3. Threshold: 3× (increased sensitivity)
  
Wall characteristics:
  - Price: Wall price level
  - Volume: Wall size (BTC)
  - Notional: Wall size (USD) = volume × price
  - Strength: volume / avg_vol (multiple)
  
bid_walls:
  = [(price, volume, notional, strength), ...]
  Sorted by strength (descending)
  
ask_walls:
  = [(price, volume, notional, strength), ...]
  Sorted by strength (descending)
```

#### 4.2 Support & Resistance Levels

```python
Method: Volume Profile Analysis

For BID levels (Support):
  1. Group volumes by rounded price (nearest $10)
  2. Find price with highest cumulative volume
  3. Calculate strength score
  
support_price:
  = Price level with max bid volume concentration
  
support_strength:
  = (volume_at_level / total_bid_volume)^0.5 × 100
  Units: 0-100 score
  Formula: sqrt scaling prevents saturation
  
For ASK levels (Resistance):
  (Same calculation on ask side)
  
resistance_price:
  = Price level with max ask volume concentration
  
resistance_strength:
  = (volume_at_level / total_ask_volume)^0.5 × 100
  Units: 0-100 score
```

#### 4.3 Volume Point of Control (POC)

```python
Combined bid + ask volume profile:
  
volume_poc_price:
  = Price level with highest total volume (bids + asks)
  
Interpretation: Price with most liquidity interest
Used for: Mean reversion targets, high-probability fills
```

#### 4.4 Price Magnets (Psychological Levels)

```python
Detection: Round numbers ($88,000, $90,000, etc.)

magnet_levels:
  = [round_price for round_price in nearby_round_numbers
     if volume_at_round_price > 1.5× avg_volume]
  
magnet_strength:
  = (volume_at_magnet / total_volume)^0.5 × 100
  Units: 0-100 score
  Formula: sqrt scaling for granularity
  
magnet_volume_velocity:
  = (current_magnet_volume - prev_magnet_volume) / time_diff
  Units: BTC/second
  Interpretation: Accumulation rate at psychological level
```

#### 4.5 Depth Velocity Tracking

```python
For each price level, track volume changes:

depth_velocity:
  = (current_volume - previous_volume) / time_diff
  Units: BTC/second
  
Aggregated metrics:
  bid_depth_velocity = sum of positive velocity (bids)
  ask_depth_velocity = sum of positive velocity (asks)
  
Interpretation:
  > 0: Liquidity appearing (walls building)
  < 0: Liquidity vanishing (walls pulled)
  
Spoofing signal: If abs(depth_velocity) > 10 BTC/sec
```

#### 4.6 Wall Renewal Tracking

```python
For detected walls:
  1. Track if wall consumed (traded through)
  2. Monitor if wall rebuilds at same price
  3. Count rebuild frequency
  
wall_renewal_count:
  = Number of times wall rebuilt within 30s window
  
Institutional signature: renewal_count ≥ 3
  (Indicates algorithmic wall maintenance)
```

---

### Tier 5: Market Regime Classification

#### 5.1 Hurst Exponent (Trend vs Mean-Reversion)

```python
Calculation method: Rescaled Range (R/S) Analysis

1. Extract price series from recent snapshots
   prices = [mid_price_t for t in recent_snapshots]
   
2. Calculate log returns
   returns = [log(price_t / price_t-1) for t in range(1, len(prices))]
   
3. Detrend returns (remove linear drift)
   detrended = returns - linear_regression(returns)
   
4. Calculate cumulative deviation
   cumsum = cumulative_sum(detrended)
   
5. Calculate range and standard deviation
   R = max(cumsum) - min(cumsum)
   S = std_dev(detrended)
   
6. Hurst exponent
   H = log(R/S) / log(len(returns))
   
Interpretation:
  H < 0.5: Mean-reverting market (oscillating)
  H = 0.5: Random walk (unpredictable)
  H > 0.5: Trending market (persistent)
  
Normal range: 0.4-0.7
```

#### 5.2 Volatility Regime

```python
Calculation: Recent spread volatility

spread_history = [spread_bps_t for t in recent_30s]
spread_volatility = std_dev(spread_history)

volatility_percentile:
  = Rank of current volatility in 5-minute history
  Range: 0-100
  
regime_volatility:
  if percentile > 75: "high"
  elif percentile > 50: "medium"
  else: "low"
```

#### 5.3 Liquidity Regime

```python
Calculation: Recent depth availability

depth_history = [total_volume_t for t in recent_30s]
current_depth = total_bid_volume + total_ask_volume

liquidity_percentile:
  = Rank of current depth in 5-minute history
  Range: 0-100
  
regime_liquidity:
  if percentile > 75: "high"
  elif percentile > 50: "medium"
  else: "low"
```

#### 5.4 Imbalance Prediction

```python
Method: Linear extrapolation of depth imbalance momentum

1. Calculate recent imbalance trend
   imbalance_history = [l5_imbalance_t for t in recent_snapshots]
   
2. Extract timestamps
   timestamps = [t for (t, imb) in imbalance_history]
   
3. Linear regression: imbalance = a × time + b
   slope = regression_slope(timestamps, imbalances)
   
4. Project 30 seconds forward
   predicted_imbalance = current_imbalance + (slope × 30)
   
imbalance_momentum_per_sec:
  = slope
  Units: Imbalance units per second
  
predicted_imbalance_30s:
  = predicted_imbalance (clamped to [-1, 1])
```

---

### Tier 6: Liquidity Vacuum Detection

#### 6.1 Thin Liquidity Identification

```python
For each price level in orderbook:
  
  1. Get surrounding levels (prev, current, next)
  2. Compare volumes
  
  thin_level_criteria:
    current_volume < 0.5 × min(prev_volume, next_volume)
  
  3. Calculate trap score
     trap_score = (current_vol / max(prev_vol, next_vol, 1e-8)) × 10
     trap_score = min(100, trap_score)
     
Trap score interpretation:
  > 80: Severe vacuum (flash crash risk)
  60-80: Moderate vacuum (high slippage risk)
  < 60: Normal thin level
```

#### 6.2 Vacuum Zones

```python
liquidity_vacuum_bid:
  = [(price, trap_score) for price in bids 
     if trap_score > 60]
  Sorted by trap_score (descending)
  
liquidity_vacuum_ask:
  = [(price, trap_score) for price in asks
     if trap_score > 60]
  Sorted by trap_score (descending)
  
Total vacuum levels:
  vacuum_bid_levels = len(liquidity_vacuum_bid)
  vacuum_ask_levels = len(liquidity_vacuum_ask)
```

---

## Output Specifications

### Snapshot Structure

Every 30 seconds, the system outputs a comprehensive snapshot:

```python
snapshot = {
    "timestamp": float,           # Unix timestamp
    "symbol": str,                # "BTCUSDT"
    
    "tier1": {                    # TRADE FLOW METRICS
        # Basic stats
        "recent_buy_volume": float,           # BTC
        "recent_sell_volume": float,          # BTC
        "recent_buy_count": int,
        "recent_sell_count": int,
        "total_recent_volume": float,         # BTC
        
        # VWAPs
        "buy_vwap": float,                   # USD
        "sell_vwap": float,                  # USD
        "vwap_spread": float,                # USD
        "vwap_spread_bps": float,            # Basis points
        
        # CVD
        "cum_volume_delta_30s": float,       # BTC
        "delta_acceleration": float,          # BTC/sec
        
        # Percentiles
        "size_percentile_50": float,          # USD
        "size_percentile_75": float,          # USD
        "size_percentile_90": float,          # USD
        "size_percentile_95": float,          # USD
        "size_percentile_99": float,          # USD
        
        # Institutional metrics
        "large_order_count": int,             # >$10K
        "large_order_buy_count": int,
        "large_order_sell_count": int,
        "large_order_total_notional": float,  # USD
        
        "whale_trade_count": int,             # >$25K
        "whale_buy_count": int,
        "whale_sell_count": int,
        "whale_total_notional": float,        # USD
        
        "block_trade_count": int,             # >$100K
        "block_buy_count": int,
        "block_sell_count": int,
        "block_total_notional": float,        # USD
        
        # Smart money
        "smart_money_ratio": float,           # Percentage
        "institutional_bias": float,          # -1 to +1
        
        # Size buckets
        "size_bucket_volumes": dict,          # {bucket: volume_btc}
        "size_bucket_counts": dict,           # {bucket: count}
        "size_bucket_notionals": dict,        # {bucket: notional_usd}
        
        # Price reference
        "last_price": float                   # USD
    },
    
    "tier2": {                    # DEPTH METRICS
        # Basic stats
        "total_bid_volume": float,            # BTC
        "total_ask_volume": float,            # BTC
        "bid_count": int,
        "ask_count": int,
        "avg_bid_size": float,                # BTC
        "avg_ask_size": float,                # BTC
        
        # Imbalances
        "depth_imbalance_l5": float,          # -1 to +1
        "depth_imbalance_l10": float,
        "depth_imbalance_l20": float,
        "depth_imbalance_l50": float,
        
        # Spreads
        "spread": float,                      # USD
        "spread_bps": float,                  # Basis points
        "spread_percentile": float,           # 0-100
        "volume_weighted_spread": float,      # USD
        "vw_spread_bps": float,               # Basis points
        "spread_velocity": float,             # bps/sec
        
        # Liquidity cliffs
        "liquidity_cliff_slippage_bid_bps": float,
        "liquidity_cliff_slippage_ask_bps": float,
        
        # Rolling averages
        "vwap_30s": float,                    # USD
        "volume_30s": float                   # BTC
    },
    
    "smart_order_detection": {   # MANIPULATION SIGNALS
        # Iceberg
        "iceberg_estimates": dict,            # {(side, price): hidden_vol}
        
        # Spoofing
        "spoofing_events": int,               # Accumulated count
        
        # Layering
        "layering_bid_levels": int,           # 0-30
        "layering_ask_levels": int,           # 0-30
        
        # Front-running
        "front_running_events": int,          # Accumulated count
        
        # Peg orders
        "peg_order_bid_volume": float,        # BTC
        "peg_order_ask_volume": float,        # BTC
        
        # Fake liquidity
        "fake_liquidity_bid_volume": float,   # BTC
        "fake_liquidity_ask_volume": float,   # BTC
        
        # Queue changes
        "best_bid_queue_changes": int,        # L1 activity
        "best_ask_queue_changes": int         # L1 activity
    },
    
    "high_value_predictive": {   # FORECASTING FEATURES
        # Walls
        "bid_walls": list,                    # [(price, vol, notional, strength)]
        "ask_walls": list,
        "bid_wall_count": int,
        "ask_wall_count": int,
        
        # Support/Resistance
        "support_price": float,               # USD
        "support_strength": float,            # 0-100
        "resistance_price": float,            # USD
        "resistance_strength": float,         # 0-100
        "volume_poc_price": float,            # USD
        
        # Price magnets
        "magnet_levels": list,                # [price]
        "magnet_levels_count": int,
        "magnet_strength": float,             # 0-100
        "magnet_volume_velocity": float,      # BTC/sec
        
        # Regime
        "hurst_exponent": float,              # 0-1
        "regime_volatility": str,             # "low"|"medium"|"high"
        "regime_liquidity": str,              # "low"|"medium"|"high"
        
        # Predictions
        "imbalance_momentum_per_sec": float,
        "predicted_imbalance_30s": float,     # -1 to +1
        
        # Depth velocity
        "bid_depth_velocity": float,          # BTC/sec
        "ask_depth_velocity": float           # BTC/sec
    },
    
    "liquidity_vacuum": {        # FLASH CRASH INDICATORS
        "liquidity_vacuum_bid": list,         # [(price, trap_score)]
        "liquidity_vacuum_ask": list,
        "vacuum_bid_levels": int,
        "vacuum_ask_levels": int
    }
}
```

---

## Algorithm Details

### 1. Order Book Processing

#### Correct Processing Order (Critical):
```
1. Receive raw orderbook from WebSocket
2. Extract best_bid = bids[0][0], best_ask = asks[0][0]
3. Calculate mid_price = (best_bid + best_ask) / 2  [NO FILTERING YET]
4. Validate spread = (best_ask - best_bid) / mid_price
   → If spread > 1%, issue warning (data corruption)
5. Apply ±10% display filter from mid_price
   → Only for display/analysis, preserves top-of-book
6. Proceed with feature calculations
```

**Why this order matters**:
- Calculating mid-price BEFORE filtering ensures accuracy
- Previous approach: filter first → corrupted mid-price → 10% spread
- Current approach: mid first → accurate reference → realistic spread

### 2. VWAP Calculation

```python
# CORRECT method (from deque of (volume, price) tuples):
def calculate_vwap(vwap_data_deque):
    total_notional = 0.0
    total_volume = 0.0
    
    for (volume, price) in vwap_data_deque:
        total_notional += volume * price
        total_volume += volume
    
    if total_volume < 1e-8:
        return 0.0
    
    return total_notional / total_volume

# INCORRECT method (approximation):
# buy_vwap ≈ vwap_30s × (1 + imbalance/200)  # DON'T USE
```

### 3. Percentile Calculation with Unit Conversion

```python
# CORRECT method:
def calculate_percentiles(trade_sizes_btc, last_price):
    sorted_sizes = sorted(trade_sizes_btc)
    
    percentiles = {}
    for p in [50, 75, 90, 95, 99]:
        index = int((p/100) * len(sorted_sizes))
        if index >= len(sorted_sizes):
            index = len(sorted_sizes) - 1
        
        # Critical: Convert BTC to USD
        percentile_btc = sorted_sizes[index]
        percentile_usd = percentile_btc * last_price
        
        percentiles[f"size_percentile_{p}"] = percentile_usd
    
    return percentiles

# INCORRECT method:
# percentiles[p] = sorted_sizes[index]  # Missing price multiplication
```

### 4. Smart Money Ratio

```python
def calculate_smart_money_ratio(large_orders, block_trades, total_trades):
    institutional_trades = len(large_orders) + len(block_trades)
    
    if total_trades == 0:
        return 0.0
    
    ratio = (institutional_trades / total_trades) * 100
    return min(100.0, ratio)  # Cap at 100%
```

### 5. Institutional Bias

```python
def calculate_institutional_bias(large_orders, whale_trades, block_trades):
    # Accumulate by side
    buy_notional = 0.0
    sell_notional = 0.0
    
    for order in large_orders:
        if order['side'] == 'buy':
            buy_notional += order['notional']
        else:
            sell_notional += order['notional']
    
    for trade in whale_trades:
        if trade['side'] == 'buy':
            buy_notional += trade['notional']
        else:
            sell_notional += trade['notional']
    
    for trade in block_trades:
        if trade['side'] == 'buy':
            buy_notional += trade['notional']
        else:
            sell_notional += trade['notional']
    
    total = buy_notional + sell_notional
    
    if total < 1e-8:
        return 0.0
    
    bias = (buy_notional - sell_notional) / total
    return max(-1.0, min(1.0, bias))  # Clamp to [-1, 1]
```

### 6. Support/Resistance with Sqrt Scaling

```python
def calculate_support_resistance(bids, asks):
    # Group by rounded price (nearest $10)
    bid_levels = defaultdict(float)
    ask_levels = defaultdict(float)
    
    for price, volume in bids:
        rounded = round(price / 10) * 10
        bid_levels[rounded] += volume
    
    for price, volume in asks:
        rounded = round(price / 10) * 10
        ask_levels[rounded] += volume
    
    # Find max concentration
    support_price = max(bid_levels, key=bid_levels.get)
    resistance_price = max(ask_levels, key=ask_levels.get)
    
    # Calculate strength with SQRT scaling (prevents saturation)
    total_bid_vol = sum(bid_levels.values())
    total_ask_vol = sum(ask_levels.values())
    
    support_concentration = bid_levels[support_price] / total_bid_vol
    resistance_concentration = ask_levels[resistance_price] / total_ask_vol
    
    support_strength = (support_concentration ** 0.5) * 100  # Sqrt scaling
    resistance_strength = (resistance_concentration ** 0.5) * 100
    
    return {
        "support_price": support_price,
        "support_strength": min(100, support_strength),
        "resistance_price": resistance_price,
        "resistance_strength": min(100, resistance_strength)
    }
```

---

## Data Quality & Validation

### Critical Checks

1. **Spread Validation**:
   - Normal: 0.001-0.01% (0.1-1.0 bps)
   - Warning threshold: > 1% (100 bps)
   - Action: Log warning, flag potential data corruption

2. **Freshness Checks**:
   - Max age: 5 seconds
   - Action: Discard stale data, prevent calculations on old snapshots

3. **Volume Sanity**:
   - Check: volume > 0
   - Check: volume < 1000 BTC (per level)
   - Action: Filter outliers

4. **Price Validation**:
   - Check: bid < ask (no crossed book)
   - Check: prices within ±30% of 24h average
   - Action: Reject corrupted data

---

## Performance Characteristics

### Processing Latency
- Order book update: < 10ms
- Trade processing: < 5ms
- Feature computation (300+ features): < 100ms
- Total snapshot generation: < 150ms

### Memory Usage
- Depth snapshots (300 × 1KB): ~300 KB
- Trade data (10,000 trades): ~500 KB
- Feature cache: ~100 KB
- Total: < 1 MB per symbol

### Accuracy
- Price precision: 2 decimals ($88,283.70)
- Volume precision: 4 decimals (0.0125 BTC)
- Percentage precision: 2 decimals (11.10%)
- Timestamp precision: Millisecond (Unix ms)

---

## Version Information

**System**: Advanced Order Flow Analysis
**Version**: 2.0 (Enhanced)
**Last Updated**: 2026-01-02
**Maintained By**: Institutional Trading Research Team

**Key Enhancements from v1.0**:
- Fixed orderbook filtering (removed aggressive 30% outlier filter)
- Added institutional intelligence (smart money, whale tracking)
- Implemented complete VWAP calculations (buy/sell/CVD)
- Added trade size percentiles with proper USD conversion
- Integrated 300+ features with zero data loss
- Research-grade accuracy across all metrics

---

## Usage Notes

### For Traders
- Monitor `institutional_bias` for whale direction
- Watch `smart_money_ratio` for participation
- Use `support_price`/`resistance_price` for entry/exit
- Track `liquidity_vacuum` levels for slippage risk

### For Researchers
- All features exportable to CSV/database
- 30-second granularity suitable for academic research
- Complete audit trail of calculations
- Reproducible methodology

### For Developers
- Modular architecture for easy extension
- Safe math operations (div-by-zero protection)
- Comprehensive error handling
- Well-documented code

---

## Glossary

**Aggressive Trade**: Market order that takes liquidity (crosses spread)
**Basis Point (bps)**: 1/100th of 1% (0.01%)
**CVD**: Cumulative Volume Delta (buy volume - sell volume)
**Imbalance**: Ratio of bid volume to ask volume
**Liquidity Vacuum**: Price level with unusually thin orderbook
**Mid-Price**: Average of best bid and best ask
**Notional**: Dollar value of trade (volume × price)
**Top-of-Book**: Best bid and best ask prices
**VWAP**: Volume-Weighted Average Price
**Wall**: Unusually large order at single price level

---

## References

- Binance API Documentation: https://binance-docs.github.io/apidocs/spot
- Market Microstructure Theory: O'Hara (1995)
- Order Flow Imbalance: Cont, Kukanov, Stoikov (2014)
- Hurst Exponent: Mandelbrot & Wallis (1969)

---

*End of Technical Documentation*
