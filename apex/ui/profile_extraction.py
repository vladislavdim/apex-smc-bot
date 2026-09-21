"""Injected AI profile extraction for user-facing chat memory."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class ProfileExtractionService:
    client: object
    model_provider: Callable[[], list[str]]
    load_memory: Callable
    update_memory: Callable

    def extract(self, user_id, user_name, message, ai_response=None) -> None:
        """Preserve the legacy bounded JSON profile extraction contract."""
        del ai_response  # Retained for the existing Telegram dependency contract.
        try:
            memory = self.load_memory(user_id)
            prompt = f"""Извлеки факты о трейдере из сообщения. Верни только JSON:
Текущий профиль: {memory["profile"] or "пустой"}
Сообщение: {message}
{{"profile": "1-2 предложения о стиле торговли", "coins": "монеты через запятую", "preferences": "таймфрейм, стиль, риск"}}"""
            response = self.client.chat.completions.create(
                model=self.model_provider()[0],
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
            )
            text = response.choices[0].message.content.strip()
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(text[start:end])
                self.update_memory(
                    user_id,
                    name=user_name,
                    profile=data.get("profile"),
                    coins=data.get("coins"),
                    preferences=data.get("preferences"),
                )
        except Exception as error:
            logging.error("Profile extract error: %s", error)
            self.update_memory(user_id, name=user_name)


_service: ProfileExtractionService | None = None


def configure_profile_extraction(service: ProfileExtractionService | None) -> None:
    global _service
    _service = service


def extract_and_save_profile(user_id, user_name, message, ai_response=None) -> None:
    service = _service
    if service is not None:
        service.extract(user_id, user_name, message, ai_response)


__all__ = [
    "ProfileExtractionService",
    "configure_profile_extraction",
    "extract_and_save_profile",
]
