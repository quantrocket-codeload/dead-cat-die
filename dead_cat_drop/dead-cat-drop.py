# Copyright 2020 QuantRocket LLC - All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas as pd
from moonshot import Moonshot
from moonshot.commission import PercentageCommission
from quantrocket.fundamental import get_ibkr_shortable_shares_reindexed_like
from quantrocket.master import get_securities_reindexed_like

class DeadCatDrop(Moonshot):

    CODE = "dead-cat-drop"
    DB = None
    DB_FIELDS = ["Open","Close","Volume"]
    MIN_DOLLAR_VOLUME = 1000000
    MAX_WEIGHT_PER_POSITION = 0.1
    LIMIT_TO_CURRENCY = None
    COMMISSION_CLASS = None
    POSITIONS_CLOSED_DAILY = True # see https://www.quantrocket.com/docs/#moonshot-commissions-and-slippage-for-intraday-positions
    CONSTRAIN_SHORTABLE = False # whether to limit position sizes by shortable shares

    def prices_to_signals(self, prices: pd.DataFrame):
        closes = prices.loc["Close"]

        # Compute dollar volume mask
        dollar_volumes = prices.loc["Volume"] * closes
        avg_dollar_volumes = dollar_volumes.rolling(window=22).mean()
        are_eligible = avg_dollar_volumes >= self.MIN_DOLLAR_VOLUME

        sectypes = get_securities_reindexed_like(
            closes, "edi_SecTypeCode").loc["edi_SecTypeCode"]
        are_eligible &= sectypes == "EQS"

        if self.LIMIT_TO_CURRENCY:
            currencies = get_securities_reindexed_like(
                closes, "Currency").loc["Currency"]
            are_eligible &= currencies == self.LIMIT_TO_CURRENCY

        # Compute big losers mask
        prior_returns = (closes - closes.shift()) / closes.shift()
        big_losers = prior_returns <= -0.10

        short_signals = big_losers & are_eligible

        return -short_signals.astype(int)

    def signals_to_target_weights(self, signals: pd.DataFrame, prices: pd.DataFrame):
        weights = self.allocate_fixed_weights_capped(signals, weight=self.MAX_WEIGHT_PER_POSITION)
        return weights

    def limit_position_sizes(self, prices: pd.DataFrame):

        max_shares_for_shorts = None

        if self.CONSTRAIN_SHORTABLE:
            t = f"09:00:00 {self.TIMEZONE}"
            shortable_shares = get_ibkr_shortable_shares_reindexed_like(prices.loc["Close"], t)
            # constrain today's target weights by tomorrow's shortable shares, when the position will be entered
            max_shares_for_shorts = shortable_shares.shift(-1)

        return None, max_shares_for_shorts

    def target_weights_to_positions(self, weights: pd.DataFrame, prices: pd.DataFrame):
        # enter next day
        positions = weights.shift()
        return positions

    def positions_to_gross_returns(self, positions: pd.DataFrame, prices: pd.DataFrame):
        closes = prices.loc["Close"]
        opens = prices.loc["Open"]
        pct_changes = (closes - opens) / opens.where(opens > 0)
        gross_returns = pct_changes * positions
        return gross_returns

# Canada
class DeadCatDropCanada(DeadCatDrop):

    CODE = "dead-cat-drop-canada"
    DB = "edi-canada-1d"
    TIMEZONE = "America/Toronto"
    MIN_DOLLAR_VOLUME = 1000000.0
    LIMIT_TO_CURRENCY = "CAD"

# Eurozone
class DeadCatDropEurozone(DeadCatDrop):

    CODE = "dead-cat-drop-eurozone"
    DB = ['edi-belgium-1d', 'edi-france-1d',
          'edi-germany-1d', 'edi-netherlands-1d']
    TIMEZONE = "Europe/Paris"
    MIN_DOLLAR_VOLUME = 1000000.0
    LIMIT_TO_CURRENCY = "EUR"

class DeadCatDropHongkong(DeadCatDrop):

    CODE = "dead-cat-drop-hongkong"
    DB = "edi-hongkong-1d"
    TIMEZONE = "Asia/Hong_Kong"
    MIN_DOLLAR_VOLUME = 8000000.0
    LIMIT_TO_CURRENCY = "HKD"

# Japan
class JapanStockTieredCommission(PercentageCommission):
    BROKER_COMMISSION_RATE = 0.0005 # 0.05% of trade value
    EXCHANGE_FEE_RATE = 0.00002 + 0.000004 # 0.002% Tokyo Stock Exchange fee + 0.0004% clearing fee
    MIN_COMMISSION = 80.00 # JPY

class DeadCatDropJapan(DeadCatDrop):

    CODE = "dead-cat-drop-japan"
    DB = "edi-japan-1d"
    TIMEZONE = "Japan"
    MIN_DOLLAR_VOLUME = 100000000.0
    LIMIT_TO_CURRENCY = "JPY"

# Sweden
class DeadCatDropSweden(DeadCatDrop):

    CODE = "dead-cat-drop-sweden"
    DB = "edi-sweden-1d"
    TIMEZONE = "Europe/Stockholm"
    MIN_DOLLAR_VOLUME = 8000000.0
    LIMIT_TO_CURRENCY = "SEK"

# Switzerland
class DeadCatDropSwitzerland(DeadCatDrop):

    CODE = "dead-cat-drop-switzerland"
    DB = "edi-switzerland-1d"
    TIMEZONE = "Europe/Zurich"
    MIN_DOLLAR_VOLUME = 1000000.0
    LIMIT_TO_CURRENCY = "CHF"

# UK
class DeadCatDropUK(DeadCatDrop):

    CODE = "dead-cat-drop-uk"
    DB = "edi-uk-1d"
    TIMEZONE = "Europe/London"
    MIN_DOLLAR_VOLUME = 100000000.0
    LIMIT_TO_CURRENCY = "GBX"
