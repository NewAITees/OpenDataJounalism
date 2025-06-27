#!/usr/bin/env python3
"""
実際のe-statデータを使用したOllama統合テスト
"""

import sys
from pathlib import Path

# プロジェクトパスを追加
sys.path.append(str(Path(__file__).parent / "src"))

from opendatajounalism.mcp.estat_metadata_loader import EstatMetadataLoader
from opendatajounalism.mcp.ollama_integration import OllamaStatsMCP


def test_metadata_loading():
    """実際のe-statメタデータの読み込みテスト"""
    print("=== e-stat メタデータ読み込みテスト ===")
    
    try:
        # メタデータローダーの初期化
        loader = EstatMetadataLoader()
        
        # メタデータキャッシュの更新（小規模テスト用）
        print("📊 メタデータキャッシュの更新中...")
        loader.update_metadata_cache(max_tables=50)  # テスト用に50件に限定
        
        # Ollama用データの確認
        ollama_data = loader.load_all_stats_for_ollama()
        
        print(f"✅ データ準備完了:")
        print(f"   統計表総数: {ollama_data.get('统计表总数', 0)}")
        print(f"   カテゴリ数: {len(ollama_data.get('分类统计表', {}))}")
        
        # サンプル表示
        categories = ollama_data.get('分类统计表', {})
        for category, subcategories in list(categories.items())[:3]:
            total_tables = sum(len(tables) for tables in subcategories.values())
            print(f"   📊 {category}: {total_tables}件")
            
            # サブカテゴリの詳細
            for sub_category, tables in list(subcategories.items())[:2]:
                print(f"      ▶ {sub_category}: {len(tables)}件")
                
                # 具体的な統計表
                for table in tables[:2]:
                    table_id = table.get('统计表ID', '')
                    stat_name = table.get('统计名称', '')
                    print(f"        - {table_id}: {stat_name}")
        
        return True
        
    except Exception as e:
        print(f"❌ メタデータ読み込みエラー: {e}")
        return False


def test_ollama_integration():
    """Ollama統合機能のテスト"""
    print("\n=== Ollama統合機能テスト ===")
    
    try:
        # Ollama MCPの初期化
        ollama_mcp = OllamaStatsMCP()
        
        # 接続状況確認
        status = ollama_mcp.get_ollama_status()
        print(f"Ollama接続状況: {status}")
        
        # テストクエリ
        test_queries = [
            {
                "query": "東京都の人口推移が知りたい",
                "region": "東京都",
                "time_period": None
            },
            {
                "query": "最新の完全失業率データを取得したい", 
                "region": None,
                "time_period": "最新"
            },
            {
                "query": "都道府県別の世帯数を比較したい",
                "region": None,
                "time_period": None
            },
            {
                "query": "2020年の年齢別人口構成",
                "region": None,
                "time_period": "2020"
            }
        ]
        
        for i, test_case in enumerate(test_queries, 1):
            print(f"\n--- テスト{i}: {test_case['query']} ---")
            
            response = ollama_mcp.suggest_stats_table_and_axes(
                query=test_case['query'],
                region=test_case['region'],
                time_period=test_case['time_period']
            )
            
            print(f"🎯 提案統計表ID: {response.stats_table_id}")
            print(f"📊 統計表名: {response.table_name}")
            print(f"🔧 軸マッピング: {response.axis_mappings}")
            print(f"📈 信頼度: {response.confidence:.2f}")
            print(f"💭 選択理由: {response.reasoning}")
            
            # 軸詳細の取得
            if response.stats_table_id:
                print("🔍 軸詳細情報:")
                axis_details = ollama_mcp.explain_axis_codes(response.stats_table_id)
                for axis_code, details in axis_details.items():
                    print(f"   {axis_code}: {details.get('description', 'N/A')}")
                    examples = details.get('examples', {})
                    if examples:
                        example_str = ", ".join([f"{k}={v}" for k, v in list(examples.items())[:3]])
                        print(f"      例: {example_str}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ollama統合テストエラー: {e}")
        return False


def test_full_integration():
    """フル統合テスト（EstatQueryTranslator経由）"""
    print("\n=== フル統合テスト ===")
    
    try:
        from opendatajounalism.mcp import EstatQueryTranslator
        
        # Ollama統合有効でTranslatorを初期化
        translator = EstatQueryTranslator(use_ollama=True)
        
        # テストクエリ
        integration_queries = [
            "東京都の年齢別人口が知りたい",
            "最新の完全失業率を調べたい",
            "都道府県別の人口減少率を比較したい"
        ]
        
        for query in integration_queries:
            print(f"\n🔍 クエリ: 「{query}」")
            
            results = translator.translate_query(query)
            
            if results:
                result = results[0]
                print(f"✅ 結果:")
                print(f"   統計表ID: {result.stats_data_id}")
                print(f"   統計表名: {result.table_name}")
                print(f"   説明: {result.description}")
                print(f"   パラメータ: {result.parameters}")
                print(f"   信頼度: {result.confidence_score:.2f}")
                
                if result.alternative_suggestions:
                    print(f"   代替案: {len(result.alternative_suggestions)}件")
            else:
                print("❌ 結果が見つかりませんでした")
        
        return True
        
    except Exception as e:
        print(f"❌ フル統合テストエラー: {e}")
        return False


def main():
    """メインテスト実行"""
    print("🚀 実際のe-statデータを使用したOllama統合テスト開始")
    print("=" * 60)
    
    # テスト実行
    tests = [
        ("メタデータ読み込み", test_metadata_loading),
        ("Ollama統合機能", test_ollama_integration), 
        ("フル統合", test_full_integration)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ {test_name}でエラー: {e}")
            results.append((test_name, False))
    
    # 結果サマリー
    print("\n" + "="*60)
    print("📋 テスト結果サマリー")
    print("="*60)
    
    for test_name, success in results:
        status = "✅ 成功" if success else "❌ 失敗"
        print(f"{status}: {test_name}")
    
    total_tests = len(results)
    successful_tests = sum(1 for _, success in results if success)
    
    print(f"\n🎯 総合結果: {successful_tests}/{total_tests} テスト成功")
    
    if successful_tests == total_tests:
        print("🎉 全テストが成功しました！実際のe-statデータを使用したOllama統合が動作しています。")
    else:
        print("⚠️ 一部のテストが失敗しました。設定を確認してください。")


if __name__ == "__main__":
    main()