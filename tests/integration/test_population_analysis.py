"""
人口減少と世帯数変化の統合テスト
日本の人口動態分析のための統合テストスイート
"""

import pytest
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import japanize_matplotlib

# プロジェクトパスを追加
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from opendatajounalism.mcp import EstatQueryTranslator, QueryResult


class TestPopulationAnalysisIntegration:
    """人口減少・世帯数変化の統合分析テスト"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """テストセットアップ"""
        self.translator = EstatQueryTranslator()
        self.results = {}
        
    def test_population_decline_query(self):
        """人口減少クエリのテスト"""
        query = "日本の人口推移データが知りたい"
        results = self.translator.translate_query(query)
        
        assert len(results) > 0, "人口推移クエリで結果が見つかりませんでした"
        
        result = results[0]
        assert result.stats_data_id, "統計表IDが設定されていません"
        assert result.confidence_score > 0, "信頼度スコアが0以下です"
        
        self.results['population'] = result
        print(f"✅ 人口推移: {result.table_name} (ID: {result.stats_data_id})")
        
    def test_household_change_query(self):
        """世帯数変化クエリのテスト"""
        query = "日本の世帯数の推移を見たい"
        results = self.translator.translate_query(query)
        
        assert len(results) > 0, "世帯数推移クエリで結果が見つかりませんでした"
        
        result = results[0]
        self.results['household'] = result
        print(f"✅ 世帯数推移: {result.table_name} (ID: {result.stats_data_id})")
        
    def test_age_group_analysis_query(self):
        """年齢別人口分析クエリのテスト"""
        query = "年齢別人口の構成比を知りたい"
        results = self.translator.translate_query(query)
        
        assert len(results) > 0, "年齢別人口クエリで結果が見つかりませんでした"
        
        result = results[0]
        self.results['age_groups'] = result
        print(f"✅ 年齢別人口: {result.table_name} (ID: {result.stats_data_id})")
        
    def test_prefecture_comparison_query(self):
        """都道府県別比較クエリのテスト"""
        query = "都道府県別の人口減少率を比較したい"
        results = self.translator.translate_query(query)
        
        assert len(results) > 0, "都道府県別比較クエリで結果が見つかりませんでした"
        
        result = results[0]
        self.results['prefecture_comparison'] = result
        print(f"✅ 都道府県別比較: {result.table_name} (ID: {result.stats_data_id})")
        
    def test_regional_analysis_queries(self):
        """地域別分析クエリのテスト"""
        regions = ["東京都", "大阪府", "愛知県", "北海道", "沖縄県"]
        
        for region in regions:
            query = f"{region}の人口と世帯数の変化"
            results = self.translator.translate_query(query)
            
            assert len(results) > 0, f"{region}のクエリで結果が見つかりませんでした"
            
            result = results[0]
            assert region.replace("県", "").replace("府", "").replace("都", "") in result.parameters.get("cdArea", ""), \
                f"{region}の地域コードが正しく設定されていません"
            
            print(f"✅ {region}: パラメータ {result.parameters}")
    
    def test_time_series_analysis_query(self):
        """時系列分析クエリのテスト"""
        queries = [
            "2000年から2020年の人口推移",
            "最新の人口データ",
            "2010年の世帯数データ"
        ]
        
        for query in queries:
            results = self.translator.translate_query(query)
            assert len(results) > 0, f"時系列クエリ '{query}' で結果が見つかりませんでした"
            
            result = results[0]
            print(f"✅ 時系列: '{query}' → パラメータ {result.parameters}")
    
    def test_demographic_analysis_queries(self):
        """人口統計分析クエリのテスト"""
        demographic_queries = [
            "男女別人口の推移",
            "高齢者人口の割合",
            "生産年齢人口の変化",
            "年少人口の推移"
        ]
        
        for query in demographic_queries:
            results = self.translator.translate_query(query)
            assert len(results) > 0, f"人口統計クエリ '{query}' で結果が見つかりませんでした"
            
            result = results[0]
            print(f"✅ 人口統計: '{query}' → {result.table_name}")
    
    def test_query_suggestions(self):
        """クエリ補完機能のテスト"""
        partial_queries = ["人口", "東京", "世帯", "高齢"]
        
        for partial in partial_queries:
            suggestions = self.translator.get_query_suggestions(partial)
            assert len(suggestions) > 0, f"'{partial}' の補完候補が見つかりませんでした"
            
            print(f"✅ 補完 '{partial}': {len(suggestions)}件の候補")
            for suggestion in suggestions[:3]:
                print(f"    - {suggestion}")
    
    def test_parameter_generation_accuracy(self):
        """パラメータ生成精度のテスト"""
        test_cases = [
            {
                "query": "東京都の2020年男女別人口",
                "expected_params": ["cdArea", "cdTime"],
                "expected_area": "13000"
            },
            {
                "query": "全国の最新人口データ", 
                "expected_params": ["cdTime"],
                "expected_area": None
            },
            {
                "query": "大阪府の年齢別人口推移",
                "expected_params": ["cdArea"],
                "expected_area": "27000"
            }
        ]
        
        for case in test_cases:
            results = self.translator.translate_query(case["query"])
            assert len(results) > 0, f"クエリ '{case['query']}' で結果が見つかりませんでした"
            
            result = results[0]
            params = result.parameters
            
            # 期待するパラメータが含まれているかチェック
            for expected_param in case["expected_params"]:
                if expected_param == "cdArea" and case["expected_area"]:
                    assert params.get("cdArea") == case["expected_area"], \
                        f"地域コードが期待値 {case['expected_area']} と異なります: {params.get('cdArea')}"
                elif expected_param == "cdTime":
                    assert "cdTime" in params, "時間パラメータが設定されていません"
            
            print(f"✅ パラメータ精度: '{case['query']}' → {params}")
    
    def test_confidence_scoring(self):
        """信頼度スコアリングのテスト"""
        queries_with_expected_confidence = [
            ("東京都の人口データ", 0.5),  # 地域+統計項目で高信頼度
            ("日本の統計データ", 0.2),   # 曖昧なクエリで低信頼度
            ("国勢調査の人口推移", 0.4)   # 具体的な統計名で中程度信頼度
        ]
        
        for query, min_confidence in queries_with_expected_confidence:
            results = self.translator.translate_query(query)
            assert len(results) > 0, f"クエリ '{query}' で結果が見つかりませんでした"
            
            result = results[0]
            assert result.confidence_score >= min_confidence, \
                f"信頼度スコア {result.confidence_score} が期待最小値 {min_confidence} を下回っています"
            
            print(f"✅ 信頼度: '{query}' → {result.confidence_score:.2f}")


def run_integration_test():
    """統合テストの実行"""
    print("=== 人口減少・世帯数変化 統合テスト ===\n")
    
    # pytest を使ってテストを実行
    pytest.main([__file__, "-v", "--tb=short"])


if __name__ == "__main__":
    run_integration_test()