"""
market_scanner.py - Оптимизированный сканер рынка для 200+ пар
Решает проблемы: производительность, память, rate limiting, batch processing
"""

import asyncio
import time
import logging
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
import threading
import json
from collections import defaultdict, deque

import market_data
from ai_manager import get_ai_manager
from database_manager import get_db

class SignalQuality(Enum):
    MEGA_TOP = "mega_top"
    TOP = "top"
    GOOD = "good"
    WEAK = "weak"
    NONE = "none"

@dataclass
class ScanResult:
    symbol: str
    quality: SignalQuality
    score: float
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    timeframe: str
    confluence: int
    regime: str
    grade: str
    analysis_time: float
    source: str
    details: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ScanStats:
    total_symbols: int = 0
    successful_scans: int = 0
    failed_scans: int = 0
    mega_top: int = 0
    top_signals: int = 0
    good_signals: int = 0
    weak_signals: int = 0
    scan_time: float = 0.0
    avg_analysis_time: float = 0.0
    cache_hits: int = 0
    api_calls: int = 0
    errors: List[str] = field(default_factory=list)

class BatchProcessor:
    """
    Оптимизированная обработка пачек символов
    """
    def __init__(self, batch_size: int = 20, max_workers: int = 5):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.stats = defaultdict(int)
    
    def process_symbols_batch(self, symbols: List[str], 
                           timeframes: List[str] = None) -> List[ScanResult]:
        """
        Обработка пачки символов с batch processing
        """
        if timeframes is None:
            timeframes = ["15m", "1h", "4h"]
        
        results = []
        
        # Разбиваем на пачки
        batches = [
            symbols[i:i + self.batch_size]
            for i in range(0, len(symbols), self.batch_size)
        ]
        
        logging.info(f"Processing {len(symbols)} symbols in {len(batches)} batches")
        
        start_time = time.time()
        
        # Обрабатываем пачки параллельно
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(batches))) as executor:
            future_to_batch = {
                executor.submit(self._process_single_batch, batch, timeframes): batch
                for batch in batches
            }
            
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    batch_results = future.result(timeout=60)
                    results.extend(batch_results)
                    self.stats["successful_batches"] += 1
                except Exception as e:
                    logging.error(f"Batch {batch[:3]}... failed: {e}")
                    self.stats["failed_batches"] += 1
                    self.stats["errors"].append(str(e))
        
        scan_time = time.time() - start_time
        logging.info(f"Batch processing completed in {scan_time:.2f}s")
        
        return results
    
    def _process_single_batch(self, symbols: List[str], 
                            timeframes: List[str]) -> List[ScanResult]:
        """
        Обработка одной пачки символов
        """
        batch_results = []
        batch_start = time.time()
        
        # Получаем свечи для всех символов параллельно
        all_candles = {}
        
        for timeframe in timeframes:
            candles_data = market_data.get_multiple_candles(
                symbols, timeframe, limit=100, max_workers=5
            )
            
            for symbol, data in candles_data.items():
                if symbol not in all_candles:
                    all_candles[symbol] = {}
                all_candles[symbol][timeframe] = data
        
        # Анализируем каждый символ
        for symbol in symbols:
            try:
                result = self._analyze_symbol(symbol, all_candles.get(symbol, {}), timeframes)
                if result:
                    batch_results.append(result)
            except Exception as e:
                logging.debug(f"Failed to analyze {symbol}: {e}")
                self.stats["analysis_errors"] += 1
        
        batch_time = time.time() - batch_start
        self.stats["total_batch_time"] += batch_time
        
        return batch_results
    
    def _analyze_symbol(self, symbol: str, candles_data: Dict[str, Dict], 
                       timeframes: List[str]) -> Optional[ScanResult]:
        """
        Анализ одного символа
        """
        analysis_start = time.time()
        
        try:
            # Проверяем наличие данных
            if not candles_data:
                return None
            
            # Базовая валидация
            valid_timeframes = []
            for tf in timeframes:
                tf_data = candles_data.get(tf, {})
                if tf_data.get("candles") and len(tf_data["candles"]) >= 20:
                    valid_timeframes.append(tf)
            
            if not valid_timeframes:
                return None
            
            # Расчёт базовых метрик
            main_tf = valid_timeframes[0]
            main_candles = candles_data[main_tf]["candles"]
            
            if not main_candles:
                return None
            
            current_price = main_candles[-1]["close"]
            
            # Анализ тренда и структуры
            direction, confidence = self._analyze_trend(main_candles)
            
            if confidence < 30:  # Слишком слабый сигнал
                return None
            
            # Расчёт уровней
            sl, tp1, tp2, tp3 = self._calculate_levels(
                main_candles, direction, current_price
            )
            
            # Расчёт confluence
            confluence = self._calculate_confluence(
                candles_data, direction, valid_timeframes
            )
            
            # Определение качества сигнала
            quality = self._determine_quality(confluence, confidence)
            
            if quality == SignalQuality.NONE:
                return None
            
            # Определение режима рынка
            regime = self._determine_regime(main_candles)
            
            # Определение грейда
            grade = self._determine_grade(quality, confluence)
            
            analysis_time = time.time() - analysis_start
            
            return ScanResult(
                symbol=symbol,
                quality=quality,
                score=confluence + confidence,
                direction=direction,
                entry=current_price,
                sl=sl,
                tp1=tp1,
                tp2=tp2,
                tp3=tp3,
                timeframe=main_tf,
                confluence=confluence,
                regime=regime,
                grade=grade,
                analysis_time=analysis_time,
                source="optimized_scanner",
                details={
                    "valid_timeframes": valid_timeframes,
                    "trend_confidence": confidence,
                    "data_sources": [candles_data[tf].get("source", "unknown") 
                                   for tf in valid_timeframes]
                }
            )
            
        except Exception as e:
            logging.debug(f"Symbol analysis error for {symbol}: {e}")
            return None
    
    def _analyze_trend(self, candles: List[Dict]) -> Tuple[str, float]:
        """
        Анализ тренда
        """
        if len(candles) < 20:
            return "UNKNOWN", 0
        
        closes = [c["close"] for c in candles]
        
        # Простая скользящая средняя
        sma_short = sum(closes[-10:]) / 10
        sma_long = sum(closes[-20:]) / 20
        
        current_price = closes[-1]
        
        # Определение направления
        if current_price > sma_short > sma_long:
            direction = "BULLISH"
            confidence = min(80, ((current_price - sma_long) / sma_long) * 1000)
        elif current_price < sma_short < sma_long:
            direction = "BEARISH"
            confidence = min(80, ((sma_long - current_price) / sma_long) * 1000)
        else:
            direction = "SIDEWAYS"
            confidence = 20
        
        return direction, max(0, min(100, confidence))
    
    def _calculate_levels(self, candles: List[Dict], direction: str, 
                         current_price: float) -> Tuple[float, float, float, float]:
        """
        Расчёт уровней SL и TP
        """
        if len(candles) < 20:
            # Default уровни если недостаточно данных
            if direction == "BULLISH":
                sl = current_price * 0.98
                tp1 = current_price * 1.02
                tp2 = current_price * 1.04
                tp3 = current_price * 1.06
            else:
                sl = current_price * 1.02
                tp1 = current_price * 0.98
                tp2 = current_price * 0.96
                tp3 = current_price * 0.94
        else:
            # Расчёт на основе ATR
            highs = [c["high"] for c in candles[-20:]]
            lows = [c["low"] for c in candles[-20:]]
            
            atr = (max(highs) - min(lows)) * 0.3  # 30% от диапазона
            
            if direction == "BULLISH":
                sl = current_price - atr
                tp1 = current_price + atr * 1.5
                tp2 = current_price + atr * 2.5
                tp3 = current_price + atr * 4
            else:
                sl = current_price + atr
                tp1 = current_price - atr * 1.5
                tp2 = current_price - atr * 2.5
                tp3 = current_price - atr * 4
        
        return sl, tp1, tp2, tp3
    
    def _calculate_confluence(self, candles_data: Dict[str, Dict], 
                            direction: str, timeframes: List[str]) -> int:
        """
        Расчёт confluence score
        """
        confluence = 0
        
        for tf in timeframes:
            tf_data = candles_data.get(tf, {})
            candles = tf_data.get("candles", [])
            
            if not candles or len(candles) < 10:
                continue
            
            # Анализ объёма
            volumes = [c["volume"] for c in candles[-10:]]
            avg_volume = sum(volumes) / len(volumes)
            current_volume = candles[-1]["volume"]
            
            if current_volume > avg_volume * 1.5:
                confluence += 10
            
            # Анализ ценового действия
            closes = [c["close"] for c in candles[-5:]]
            
            if direction == "BULLISH":
                if all(closes[i] >= closes[i-1] for i in range(1, len(closes))):
                    confluence += 15
            else:
                if all(closes[i] <= closes[i-1] for i in range(1, len(closes))):
                    confluence += 15
        
        return min(100, confluence)
    
    def _determine_quality(self, confluence: int, confidence: float) -> SignalQuality:
        """
        Определение качества сигнала
        """
        total_score = confluence + confidence
        
        if total_score >= 150:
            return SignalQuality.MEGA_TOP
        elif total_score >= 120:
            return SignalQuality.TOP
        elif total_score >= 80:
            return SignalQuality.GOOD
        elif total_score >= 50:
            return SignalQuality.WEAK
        else:
            return SignalQuality.NONE
    
    def _determine_regime(self, candles: List[Dict]) -> str:
        """
        Определение режима рынка
        """
        if len(candles) < 20:
            return "UNKNOWN"
        
        closes = [c["close"] for c in candles]
        
        # Волатильность
        returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
        volatility = sum(abs(r) for r in returns) / len(returns) * 100
        
        # Тренд
        sma_short = sum(closes[-10:]) / 10
        sma_long = sum(closes[-20:]) / 20
        
        if abs(sma_short - sma_long) / sma_long < 0.01:
            if volatility < 2:
                return "RANGE_LOW_VOL"
            else:
                return "RANGE_HIGH_VOL"
        elif sma_short > sma_long:
            return "TREND_UP"
        else:
            return "TREND_DOWN"
    
    def _determine_grade(self, quality: SignalQuality, confluence: int) -> str:
        """
        Определение грейда сигнала
        """
        if quality == SignalQuality.MEGA_TOP:
            return "🔥🔥🔥 МЕГА ТОП"
        elif quality == SignalQuality.TOP:
            return "🔥🔥 ТОП СДЕЛКА"
        elif quality == SignalQuality.GOOD:
            return "✅ ХОРОШАЯ"
        else:
            return "⚠️ СЛАБАЯ"
    
    def get_stats(self) -> Dict[str, Any]:
        """Получить статистику обработки"""
        return dict(self.stats)

class MarketScanner:
    """
    Основной класс сканера рынка
    """
    def __init__(self, max_symbols: int = 200, batch_size: int = 20):
        self.max_symbols = max_symbols
        self.batch_size = batch_size
        self.processor = BatchProcessor(batch_size=batch_size, max_workers=5)
        self.ai_manager = get_ai_manager()
        self.db = get_db()
        
        # Кэш результатов
        self.result_cache = deque(maxlen=1000)
        self.cache_lock = threading.Lock()
        
        # Статистика
        self.scan_stats = ScanStats()
        
        # Список символов для сканирования
        self.symbols = []
        self.last_symbols_update = None
    
    def update_symbols(self) -> bool:
        """
        Обновление списка символов для сканирования
        """
        try:
            self.symbols = market_data.get_top_pairs(self.max_symbols)
            self.last_symbols_update = datetime.now()
            logging.info(f"Updated symbols list: {len(self.symbols)} pairs")
            return True
        except Exception as e:
            logging.error(f"Failed to update symbols: {e}")
            return False
    
    def scan_market(self, timeframes: List[str] = None, 
                   min_quality: SignalQuality = SignalQuality.GOOD) -> List[ScanResult]:
        """
        Полное сканирование рынка
        """
        scan_start = time.time()
        
        # Обновляем список символов если нужно
        if (not self.symbols or 
            not self.last_symbols_update or 
            (datetime.now() - self.last_symbols_update).seconds > 3600):
            self.update_symbols()
        
        if not self.symbols:
            logging.error("No symbols to scan")
            return []
        
        logging.info(f"Starting market scan: {len(self.symbols)} symbols")
        
        # Сброс статистики
        self.scan_stats = ScanStats()
        self.scan_stats.total_symbols = len(self.symbols)
        
        try:
            # Обработка пачками
            all_results = self.processor.process_symbols_batch(self.symbols, timeframes)
            
            # Фильтрация по качеству
            quality_order = {
                SignalQuality.MEGA_TOP: 0,
                SignalQuality.TOP: 1,
                SignalQuality.GOOD: 2,
                SignalQuality.WEAK: 3,
                SignalQuality.NONE: 4
            }
            
            min_order = quality_order.get(min_quality, 2)
            filtered_results = [
                r for r in all_results 
                if quality_order.get(r.quality, 4) <= min_order
            ]
            
            # Сортировка по качеству и score
            filtered_results.sort(key=lambda x: (
                quality_order.get(x.quality, 4),
                -x.score
            ))
            
            # Обновление статистики
            scan_time = time.time() - scan_start
            self.scan_stats.scan_time = scan_time
            self.scan_stats.successful_scans = len(all_results)
            self.scan_stats.mega_top = sum(1 for r in filtered_results if r.quality == SignalQuality.MEGA_TOP)
            self.scan_stats.top_signals = sum(1 for r in filtered_results if r.quality == SignalQuality.TOP)
            self.scan_stats.good_signals = sum(1 for r in filtered_results if r.quality == SignalQuality.GOOD)
            self.scan_stats.weak_signals = sum(1 for r in filtered_results if r.quality == SignalQuality.WEAK)
            
            if all_results:
                self.scan_stats.avg_analysis_time = sum(r.analysis_time for r in all_results) / len(all_results)
            
            # Сохранение в кэш
            with self.cache_lock:
                self.result_cache.extend(filtered_results)
            
            logging.info(f"Scan completed: {len(filtered_results)} signals in {scan_time:.2f}s")
            
            return filtered_results
            
        except Exception as e:
            logging.error(f"Market scan failed: {e}")
            self.scan_stats.errors.append(str(e))
            return []
    
    def scan_single_symbol(self, symbol: str, timeframes: List[str] = None) -> Optional[ScanResult]:
        """
        Сканирование одного символа
        """
        try:
            candles_data = {}
            if timeframes is None:
                timeframes = ["15m", "1h", "4h"]
            
            for tf in timeframes:
                data = market_data.get_candles_cached(symbol, tf, limit=100)
                if data["candles"]:
                    candles_data[tf] = data
            
            return self.processor._analyze_symbol(symbol, candles_data, timeframes)
            
        except Exception as e:
            logging.error(f"Single symbol scan failed for {symbol}: {e}")
            return None
    
    def get_top_signals(self, count: int = 10, 
                       min_quality: SignalQuality = SignalQuality.GOOD) -> List[ScanResult]:
        """
        Получение топ сигналов из последнего сканирования
        """
        with self.cache_lock:
            if not self.result_cache:
                return []
            
            quality_order = {
                SignalQuality.MEGA_TOP: 0,
                SignalQuality.TOP: 1,
                SignalQuality.GOOD: 2,
                SignalQuality.WEAK: 3
            }
            
            min_order = quality_order.get(min_quality, 2)
            filtered = [
                r for r in self.result_cache 
                if quality_order.get(r.quality, 4) <= min_order
            ]
            
            return sorted(filtered, key=lambda x: (
                quality_order.get(x.quality, 4),
                -x.score
            ))[:count]
    
    def get_scan_stats(self) -> ScanStats:
        """Получить статистику сканирования"""
        # Объединяем статистику процессора и сканера
        processor_stats = self.processor.get_stats()
        
        combined_stats = ScanStats(
            total_symbols=self.scan_stats.total_symbols,
            successful_scans=self.scan_stats.successful_scans,
            failed_scans=self.scan_stats.failed_scans,
            mega_top=self.scan_stats.mega_top,
            top_signals=self.scan_stats.top_signals,
            good_signals=self.scan_stats.good_signals,
            weak_signals=self.scan_stats.weak_signals,
            scan_time=self.scan_stats.scan_time,
            avg_analysis_time=self.scan_stats.avg_analysis_time,
            cache_hits=processor_stats.get("cache_hits", 0),
            api_calls=processor_stats.get("api_calls", 0),
            errors=self.scan_stats.errors + processor_stats.get("errors", [])
        )
        
        return combined_stats
    
    def format_signal(self, result: ScanResult) -> str:
        """
        Форматирование сигнала для вывода
        """
        direction_emoji = "🟢" if result.direction == "BULLISH" else "🔴"
        
        return (
            f"{direction_emoji} <b>{result.symbol}</b> {result.grade}\n"
            f"Entry: {result.entry:.6f}\n"
            f"SL: {result.sl:.6f} | TP1: {result.tp1:.6f}\n"
            f"Confluence: {result.confluence} | Regime: {result.regime}\n"
            f"Timeframe: {result.timeframe} | Source: {result.source}"
        )

# Глобальный экземпляр сканера
_scanner = MarketScanner()

def get_scanner() -> MarketScanner:
    """Получить глобальный экземпляр сканера"""
    return _scanner

# Удобные функции
def scan_market_sync(timeframes: List[str] = None, 
                   min_quality: SignalQuality = SignalQuality.GOOD) -> List[ScanResult]:
    """Синхронное сканирование рынка"""
    return _scanner.scan_market(timeframes, min_quality)

async def scan_market_async(timeframes: List[str] = None, 
                          min_quality: SignalQuality = SignalQuality.GOOD) -> List[ScanResult]:
    """Асинхронное сканирование рынка"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, scan_market_sync, timeframes, min_quality
    )

def get_top_signals(count: int = 10) -> List[ScanResult]:
    """Получить топ сигналы"""
    return _scanner.get_top_signals(count)

def get_scanner_stats() -> ScanStats:
    """Получить статистику сканера"""
    return _scanner.get_scan_stats()

def scan_symbol(symbol: str) -> Optional[ScanResult]:
    """Отсканировать один символ"""
    return _scanner.scan_single_symbol(symbol)
