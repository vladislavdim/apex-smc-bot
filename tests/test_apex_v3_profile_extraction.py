import unittest
from types import SimpleNamespace

from apex.ui.profile_extraction import ProfileExtractionService


class _Completions:
    def __init__(self, content=None, error=None):
        self.content = content
        self.error = error

    def create(self, **kwargs):
        if self.error:
            raise self.error
        self.kwargs = kwargs
        return SimpleNamespace(choices=[SimpleNamespace(
            message=SimpleNamespace(content=self.content),
        )])


class ProfileExtractionTests(unittest.TestCase):
    def test_json_profile_is_saved_with_existing_contract(self):
        completions = _Completions(
            'result: {"profile":"swing trader","coins":"BTC",'
            '"preferences":"4h, low risk"}',
        )
        updates = []
        service = ProfileExtractionService(
            SimpleNamespace(chat=SimpleNamespace(completions=completions)),
            lambda: ["model-a"],
            lambda user_id: {"profile": "old"},
            lambda user_id, **kwargs: updates.append((user_id, kwargs)),
        )

        service.extract(7, "Vlad", "I trade BTC", "unused")

        self.assertEqual(completions.kwargs["model"], "model-a")
        self.assertEqual(completions.kwargs["max_tokens"], 200)
        self.assertEqual(updates, [(7, {
            "name": "Vlad",
            "profile": "swing trader",
            "coins": "BTC",
            "preferences": "4h, low risk",
        })])

    def test_provider_failure_still_saves_user_name(self):
        updates = []
        service = ProfileExtractionService(
            SimpleNamespace(chat=SimpleNamespace(
                completions=_Completions(error=RuntimeError("offline")),
            )),
            lambda: ["model-a"],
            lambda user_id: {"profile": None},
            lambda user_id, **kwargs: updates.append((user_id, kwargs)),
        )

        service.extract(9, "User", "hello")

        self.assertEqual(updates, [(9, {"name": "User"})])


if __name__ == "__main__":
    unittest.main()
