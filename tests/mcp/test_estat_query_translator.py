"""e-stat Query Translator のテスト"""

import pytest
from pathlib import Path
import tempfile
import shutil

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))
from opendatajounalism.mcp import EstatQueryTranslator, QueryResult


@pytest.fixture
def temp_data_dir():
    """テスト用の一時データディレクトリ"""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture  
def translator(temp_data_dir):
    """テスト用のTranslatorインスタンス"""
    return EstatQueryTranslator(data_dir=temp_data_dir)


def test_parse_query_regions(translator):
    """地域名の抽出テスト"""
    entities = translator.parse_query("東京都の人口データが知りたい")
    assert "東京都" in entities.regions


def test_parse_query_statistical_items(translator):
    """統計項目の抽出テスト"""
    entities = translator.parse_query("完全失業率の推移を見たい")
    assert "失業率" in entities.statistical_items


def test_translate_query_basic(translator):
    """基本的なクエリ変換テスト"""
    results = translator.translate_query("東京都の人口が知りたい")
    assert len(results) > 0
    assert isinstance(results[0], QueryResult)
    assert results[0].stats_data_id
    assert results[0].confidence_score > 0


def test_translate_query_unemployment(translator):
    """失業率クエリのテスト"""
    results = translator.translate_query("最新の完全失業率")
    assert len(results) > 0
    result = results[0]
    assert "失業" in result.description or "労働" in result.description


def test_area_mappings(translator):
    """地域コードマッピングのテスト"""
    assert translator.area_mappings["東京都"] == "13000"
    assert translator.area_mappings["大阪府"] == "27000"


def test_query_suggestions(translator):
    """クエリ補完のテスト"""
    suggestions = translator.get_query_suggestions("東京")
    assert len(suggestions) > 0
    assert any("東京" in suggestion for suggestion in suggestions)