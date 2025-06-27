#!/usr/bin/env python3
"""
e-stat MCP デモンストレーション

自然言語でe-statデータを検索するデモ
"""

import os
from dotenv import load_dotenv
from src.opendatajounalism.mcp import EstatQueryTranslator
from src.opendatajounalism.mcp.catalog_integration import CatalogIntegrator


def main():
    print("=== e-stat AI Query Translator デモ ===\n")
    
    # 環境変数の読み込み
    load_dotenv()
    
    # 1. カタログの同期（初回のみまたは更新時）
    print("1. カタログデータの準備...")
    integrator = CatalogIntegrator()
    integrator.sync_catalog_to_mcp_db()
    
    # 2. トランスレータの初期化
    translator = EstatQueryTranslator()
    
    # 3. デモクエリの実行
    demo_queries = [
        "東京都の年齢別人口が知りたい",
        "最新の完全失業率を見たい",
        "都道府県別の人口を比較したい",
        "2020年の男女別人口データが欲しい",
        "賃金の統計データを探している"
    ]
    
    for i, query in enumerate(demo_queries, 1):
        print(f"\n{i}. クエリ: 「{query}」")
        print("-" * 50)
        
        try:
            results = translator.translate_query(query)
            
            if results:
                result = results[0]
                print(f"✅ 統計表ID: {result.stats_data_id}")
                print(f"📊 表名: {result.table_name}")
                print(f"📝 説明: {result.description}")
                print(f"🔧 APIパラメータ: {result.parameters}")
                print(f"🎯 信頼度: {result.confidence_score:.2f}")
                
                if result.alternative_suggestions:
                    print(f"\n💡 代替案 ({len(result.alternative_suggestions)}件):")
                    for alt in result.alternative_suggestions[:2]:  # 上位2件のみ表示
                        print(f"   - {alt.table_name} (信頼度: {alt.confidence_score:.2f})")
            else:
                print("❌ 該当する統計表が見つかりませんでした")
                
        except Exception as e:
            print(f"❌ エラー: {e}")
    
    # 4. インタラクティブモード
    print(f"\n{'='*60}")
    print("🔍 インタラクティブモード（'quit'で終了）")
    print("='*60")
    
    while True:
        try:
            user_query = input("\n💬 クエリを入力してください: ").strip()
            
            if user_query.lower() in ['quit', 'exit', '終了']:
                break
            
            if not user_query:
                continue
                
            results = translator.translate_query(user_query)
            
            if results:
                result = results[0]
                print(f"\n✅ 結果:")
                print(f"   統計表ID: {result.stats_data_id}")
                print(f"   表名: {result.table_name}")
                print(f"   APIパラメータ: {result.parameters}")
            else:
                print("\n❌ 該当する統計表が見つかりませんでした")
                
                # 候補の提案
                suggestions = translator.get_query_suggestions(user_query[:5])
                if suggestions:
                    print("\n💡 こんなクエリはいかがですか？")
                    for suggestion in suggestions[:3]:
                        print(f"   - {suggestion}")
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ エラー: {e}")
    
    print("\n👋 デモを終了します")


if __name__ == "__main__":
    main()