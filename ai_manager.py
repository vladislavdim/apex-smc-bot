"""
ai_manager.py - Управление AI API с rate limiting, retry и fallback
Решает проблемы: перегрузка Groq API, отсутствие retry, fallback модели
"""

import os
import time
import json
import logging
import threading
import asyncio
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import requests
from groq import Groq

class ModelStatus(Enum):
    AVAILABLE = "available"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"
    DISABLED = "disabled"

@dataclass
class AIModel:
    name: str
    max_tokens: int
    cost_per_1k_tokens: float
    status: ModelStatus = ModelStatus.AVAILABLE
    last_error: Optional[str] = None
    last_success: Optional[datetime] = None
    error_count: int = 0
    success_count: int = 0

class RateLimiter:
    def __init__(self, calls_per_minute: int, tokens_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.tokens_per_minute = tokens_per_minute
        self.call_times = []
        self.token_usage = []
        self.lock = threading.Lock()
    
    def can_make_call(self, tokens: int) -> bool:
        with self.lock:
            now = time.time()
            minute_ago = now - 60
            
            # Очищаем старые записи
            self.call_times = [t for t in self.call_times if t > minute_ago]
            self.token_usage = [(t, tokens) for t, tokens in self.token_usage if t > minute_ago]
            
            # Проверяем лимиты
            if len(self.call_times) >= self.calls_per_minute:
                return False
            
            total_tokens = sum(tokens for _, tokens in self.token_usage)
            if total_tokens + tokens > self.tokens_per_minute:
                return False
            
            return True
    
    def record_call(self, tokens: int):
        with self.lock:
            now = time.time()
            self.call_times.append(now)
            self.token_usage.append((now, tokens))
    
    def wait_time(self, tokens: int) -> float:
        with self.lock:
            now = time.time()
            minute_ago = now - 60
            
            # Очищаем старые записи
            self.call_times = [t for t in self.call_times if t > minute_ago]
            self.token_usage = [(t, tokens) for t, tokens in self.token_usage if t > minute_ago]
            
            # Время до следующего вызова
            if len(self.call_times) >= self.calls_per_minute:
                return max(0, 60 - (now - self.call_times[0]))
            
            total_tokens = sum(tokens for _, tokens in self.token_usage)
            if total_tokens + tokens > self.tokens_per_minute:
                # Приблизительное время до освобождения токенов
                if self.token_usage:
                    return max(0, 60 - (now - self.token_usage[0][0]))
            
            return 0

class AIManager:
    def __init__(self):
        self.api_key = os.environ.get("GROQ_API_KEY", "")
        self.models = self._init_models()
        self.rate_limiters = {
            "groq": RateLimiter(calls_per_minute=30, tokens_per_minute=40000),
            "fallback": RateLimiter(calls_per_minute=10, tokens_per_minute=10000)
        }
        self.usage_stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "total_tokens": 0,
            "daily_tokens": 0,
            "last_reset": datetime.now().date()
        }
        self.lock = threading.Lock()
        
        # Дневной лимит токенов
        self.daily_token_limit = 480_000
        
        # Circuit breaker для каждой модели
        self.circuit_breakers = {}
        
    def _init_models(self) -> Dict[str, AIModel]:
        """Инициализация доступных моделей"""
        return {
            "llama-3.1-8b-instant": AIModel(
                name="llama-3.1-8b-instant",
                max_tokens=8192,
                cost_per_1k_tokens=0.05
            ),
            "llama-3.3-70b-versatile": AIModel(
                name="llama-3.3-70b-versatile", 
                max_tokens=8192,
                cost_per_1k_tokens=0.59
            ),
            "llama-3.1-70b-versatile": AIModel(
                name="llama-3.1-70b-versatile",
                max_tokens=8192,
                cost_per_1k_tokens=0.59
            ),
            "mixtral-8x7b-32768": AIModel(
                name="mixtral-8x7b-32768",
                max_tokens=32768,
                cost_per_1k_tokens=0.27
            )
        }
    
    def _check_daily_limit(self) -> bool:
        """Проверка дневного лимита токенов"""
        with self.lock:
            today = datetime.now().date()
            if today != self.usage_stats["last_reset"]:
                # Сброс дневных счётчиков
                self.usage_stats["daily_tokens"] = 0
                self.usage_stats["last_reset"] = today
            
            return self.usage_stats["daily_tokens"] < self.daily_token_limit
    
    def _update_stats(self, tokens: int, success: bool, model_name: str):
        """Обновление статистики использования"""
        with self.lock:
            self.usage_stats["total_calls"] += 1
            self.usage_stats["total_tokens"] += tokens
            self.usage_stats["daily_tokens"] += tokens
            
            if success:
                self.usage_stats["successful_calls"] += 1
                if model_name in self.models:
                    self.models[model_name].success_count += 1
                    self.models[model_name].last_success = datetime.now()
                    self.models[model_name].error_count = 0  # Сброс счётчика ошибок
                    self.models[model_name].status = ModelStatus.AVAILABLE
            else:
                self.usage_stats["failed_calls"] += 1
                if model_name in self.models:
                    self.models[model_name].error_count += 1
                    # Отключаем модель после 5 ошибок подряд
                    if self.models[model_name].error_count >= 5:
                        self.models[model_name].status = ModelStatus.DISABLED
                        logging.warning(f"Model {model_name} disabled due to errors")
    
    def _get_best_model(self, tokens_needed: int) -> Optional[str]:
        """Выбор лучшей доступной модели"""
        available_models = [
            name for name, model in self.models.items()
            if model.status == ModelStatus.AVAILABLE
            and model.max_tokens >= tokens_needed
        ]
        
        if not available_models:
            return None
        
        # Приоритет: самая дешёвая модель с достаточным количеством токенов
        return min(available_models, key=lambda x: self.models[x].cost_per_1k_tokens)
    
    def call_groq(self, prompt: str, max_tokens: int = 800, 
                  temperature: float = 0.3, model: Optional[str] = None,
                  max_retries: int = 3) -> Optional[str]:
        """
        Вызов Groq API с rate limiting и retry
        """
        if not self.api_key:
            logging.error("GROQ_API_KEY not set")
            return None
        
        if not self._check_daily_limit():
            logging.error("Daily token limit exceeded")
            return None
        
        # Оценка количества токенов в промпте (приблизительно)
        prompt_tokens = len(prompt.split()) * 1.3  # ~1.3 токена на слово
        total_tokens_needed = prompt_tokens + max_tokens
        
        # Выбор модели
        if not model:
            model = self._get_best_model(total_tokens_needed)
            if not model:
                logging.error("No available models for token requirements")
                return None
        
        # Rate limiting
        limiter = self.rate_limiters["groq"]
        if not limiter.can_make_call(total_tokens_needed):
            wait_time = limiter.wait_time(total_tokens_needed)
            if wait_time > 0:
                logging.info(f"Rate limiting: waiting {wait_time:.1f}s")
                time.sleep(wait_time)
        
        # Retry loop
        for attempt in range(max_retries):
            try:
                # Создаём клиент для каждой попытки
                client = Groq(api_key=self.api_key)
                
                response = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    temperature=temperature
                )
                
                if not response.choices:
                    raise ValueError("No choices in response")
                
                result = response.choices[0].message.content.strip()
                
                # Записываем успешный вызов
                actual_tokens = response.usage.total_tokens if response.usage else total_tokens_needed
                limiter.record_call(actual_tokens)
                self._update_stats(actual_tokens, True, model)
                
                logging.info(f"Groq call successful: {model}, {actual_tokens} tokens")
                return result
                
            except Exception as e:
                error_msg = str(e)
                logging.warning(f"Groq API error (attempt {attempt + 1}): {error_msg}")
                
                # Анализ ошибки
                if "429" in error_msg or "rate limit" in error_msg.lower():
                    # Rate limit - увеличиваем время ожидания
                    wait_time = 30 * (2 ** attempt)  # 30, 60, 120 секунд
                    logging.info(f"Rate limit hit, waiting {wait_time}s")
                    time.sleep(wait_time)
                    
                elif "model" in error_msg.lower() and "not found" in error_msg.lower():
                    # Модель не найдена - отключаем её
                    if model in self.models:
                        self.models[model].status = ModelStatus.DISABLED
                        self.models[model].last_error = error_msg
                    # Пробуем другую модель
                    model = self._get_best_model(total_tokens_needed)
                    if not model:
                        break
                    continue
                    
                elif "insufficient_quota" in error_msg.lower():
                    # Квота исчерпана
                    logging.error("Groq quota exceeded")
                    self._update_stats(total_tokens_needed, False, model)
                    return None
                    
                else:
                    # Другая ошибка
                    if attempt == max_retries - 1:
                        self._update_stats(total_tokens_needed, False, model)
                        return None
                    
                    # Exponential backoff
                    wait_time = 5 * (2 ** attempt)
                    time.sleep(wait_time)
        
        return None
    
    def call_with_fallback(self, prompt: str, max_tokens: int = 800,
                          temperature: float = 0.3) -> Optional[str]:
        """
        Вызов с fallback на другие модели и методы
        """
        # Пробуем последовательно модели
        model_priority = [
            "llama-3.1-8b-instant",
            "llama-3.3-70b-versatile", 
            "llama-3.1-70b-versatile",
            "mixtral-8x7b-32768"
        ]
        
        for model in model_priority:
            if model not in self.models:
                continue
                
            model_obj = self.models[model]
            if model_obj.status != ModelStatus.AVAILABLE:
                continue
            
            result = self.call_groq(
                prompt=prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                model=model,
                max_retries=2
            )
            
            if result:
                return result
        
        # Fallback на простые ответы
        logging.warning("All AI models failed, using fallback response")
        return self._generate_fallback_response(prompt)
    
    def _generate_fallback_response(self, prompt: str) -> str:
        """Генерация простого fallback ответа"""
        prompt_lower = prompt.lower()
        
        if "analyze" in prompt_lower and "signal" in prompt_lower:
            return "Analysis temporarily unavailable. Please try again later."
        elif "summary" in prompt_lower:
            return "Summary generation temporarily unavailable."
        elif "recommendation" in prompt_lower or "advice" in prompt_lower:
            return "Recommendations temporarily unavailable. Use manual analysis."
        else:
            return "AI service temporarily unavailable. Please try again later."
    
    async def call_async(self, prompt: str, max_tokens: int = 800,
                        temperature: float = 0.3) -> Optional[str]:
        """Асинхронный вызов AI"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.call_with_fallback, prompt, max_tokens, temperature
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики использования"""
        with self.lock:
            return {
                "usage": self.usage_stats.copy(),
                "models": {
                    name: {
                        "status": model.status.value,
                        "error_count": model.error_count,
                        "success_count": model.success_count,
                        "last_error": model.last_error,
                        "last_success": model.last_success.isoformat() if model.last_success else None
                    }
                    for name, model in self.models.items()
                },
                "rate_limiters": {
                    name: {
                        "calls_per_minute": limiter.calls_per_minute,
                        "tokens_per_minute": limiter.tokens_per_minute,
                        "recent_calls": len(limiter.call_times),
                        "recent_tokens": sum(tokens for _, tokens in limiter.token_usage)
                    }
                    for name, limiter in self.rate_limiters.items()
                }
            }
    
    def reset_model_status(self, model_name: str):
        """Сброс статуса модели"""
        if model_name in self.models:
            self.models[model_name].status = ModelStatus.AVAILABLE
            self.models[model_name].error_count = 0
            self.models[model_name].last_error = None
            logging.info(f"Model {model_name} status reset")
    
    def disable_model(self, model_name: str, reason: str = ""):
        """Отключение модели"""
        if model_name in self.models:
            self.models[model_name].status = ModelStatus.DISABLED
            self.models[model_name].last_error = reason
            logging.warning(f"Model {model_name} disabled: {reason}")
    
    def get_daily_usage_percent(self) -> float:
        """Процент использования дневного лимита"""
        with self.lock:
            return (self.usage_stats["daily_tokens"] / self.daily_token_limit) * 100

# Глобальный экземпляр
_ai_manager = AIManager()

def get_ai_manager() -> AIManager:
    """Получить глобальный экземпляр AI менеджера"""
    return _ai_manager

# Удобные функции для обратной совместимости
def groq_call(prompt: str, max_tokens: int = 800, temperature: float = 0.3) -> Optional[str]:
    """Простой вызов Groq с управлением"""
    return _ai_manager.call_with_fallback(prompt, max_tokens, temperature)

async def groq_call_async(prompt: str, max_tokens: int = 800, temperature: float = 0.3) -> Optional[str]:
    """Асинхронный вызов Groq"""
    return await _ai_manager.call_async(prompt, max_tokens, temperature)

def get_ai_stats() -> Dict[str, Any]:
    """Получить статистику AI"""
    return _ai_manager.get_stats()

def check_ai_limits() -> Dict[str, Any]:
    """Проверить лимиты AI"""
    return {
        "daily_usage_percent": _ai_manager.get_daily_usage_percent(),
        "daily_tokens_used": _ai_manager.usage_stats["daily_tokens"],
        "daily_limit": _ai_manager.daily_token_limit,
        "can_make_calls": _ai_manager._check_daily_limit()
    }
