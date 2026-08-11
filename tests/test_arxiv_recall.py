import unittest
from unittest.mock import patch

from src.connectors.arxiv import (
    UR_KEYWORDS,
    _build_query,
    _openalex_work_to_arxiv_record,
    fetch_category,
    matches_ur_keywords,
)
from src.pipeline.ingest_arxiv import STRONG_KEYWORDS, passes_strong_filter


SYNTHETIC_USER_TERMS = {
    "simulated user",
    "simulated users",
    "synthetic user",
    "synthetic users",
    "user simulation",
    "user simulator",
    "persona agent",
    "persona agents",
    "virtual user",
    "virtual users",
}


class ArxivRecallTests(unittest.TestCase):
    def test_matraix_is_a_regression_fixture_for_strong_filter(self):
        title = "MatrAIx: Simulating the World with 8.3 Billion Persona Agents"
        abstract = (
            "We introduce MatrAIx, a population-scale simulated-user evaluation "
            "infrastructure for testing AI systems and digital products with "
            "heterogeneous users."
        )

        self.assertTrue(passes_strong_filter(title, abstract))

    def test_synthetic_user_terms_reach_both_recall_stages(self):
        self.assertTrue(SYNTHETIC_USER_TERMS.issubset(set(UR_KEYWORDS)))
        self.assertTrue(SYNTHETIC_USER_TERMS.issubset(set(STRONG_KEYWORDS)))

    def test_hyphenated_simulated_user_phrase_passes_strong_filter(self):
        self.assertTrue(
            passes_strong_filter(
                "Population-scale evaluation with simulated-user agents",
                "The agents interact with digital products across many domains.",
            )
        )

    def test_arxiv_query_searches_title_and_abstract(self):
        query = _build_query("cs.AI", ["simulated user"])

        self.assertIn('abs:"simulated user"', query)
        self.assertIn('ti:"simulated user"', query)

    def test_openalex_fallback_maps_matraix_to_arxiv_record(self):
        work = {
            "id": "https://openalex.org/W7196962344",
            "doi": "https://doi.org/10.48550/arxiv.2608.04205",
            "title": "MatrAIx: Simulating the World with 8.3 Billion Persona Agents",
            "publication_date": "2026-08-04",
            "publication_year": 2026,
            "abstract_inverted_index": {
                "population-scale": [0],
                "simulated-user": [1],
                "evaluation": [2],
            },
            "authorships": [
                {"author": {"display_name": "Xiaomin Li"}},
            ],
        }

        record = _openalex_work_to_arxiv_record(work)

        self.assertEqual(record["arxiv_id"], "2608.04205")
        self.assertEqual(record["doi"], "10.48550/arxiv.2608.04205")
        self.assertEqual(record["authors"], [{"name": "Xiaomin Li"}])
        self.assertTrue(matches_ur_keywords(record["title"], record["abstract"]))

    def test_openalex_fallback_rejects_non_arxiv_work(self):
        self.assertIsNone(
            _openalex_work_to_arxiv_record(
                {
                    "doi": "https://doi.org/10.1000/example",
                    "title": "Persona study",
                }
            )
        )

    def test_openalex_local_filter_does_not_treat_personalized_as_persona(self):
        self.assertFalse(
            matches_ur_keywords(
                "A personalized optimizer for neural networks",
                "A purely technical benchmark without human participants.",
            )
        )

    @patch("src.connectors.arxiv.time.sleep", return_value=None)
    @patch("src.connectors.arxiv.get_with_retry", side_effect=TimeoutError("boom"))
    @patch("src.connectors.arxiv.make_client")
    def test_arxiv_api_failure_is_not_silently_treated_as_success(
        self, make_client, _get_with_retry, _sleep
    ):
        make_client.return_value.__enter__.return_value = object()

        with self.assertRaisesRegex(RuntimeError, "arXiv API failed.*cs.AI"):
            list(fetch_category("cs.AI", from_date="2026-07-11", max_results=1))


if __name__ == "__main__":
    unittest.main()
