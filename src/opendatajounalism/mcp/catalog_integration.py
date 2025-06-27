"""既存のカタログダウンローダーとの統合機能"""

import json
import sqlite3
from pathlib import Path
from typing import Dict, List

import pandas as pd

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from catalog_downloader import EStatCatalogDownloader


class CatalogIntegrator:
    """カタログダウンローダーとMCPの統合クラス"""
    
    def __init__(self, catalog_dir: str = "estat_catalog", mcp_data_dir: str = "data/mcp"):
        self.catalog_dir = Path(catalog_dir)
        self.mcp_data_dir = Path(mcp_data_dir)
        self.mcp_data_dir.mkdir(parents=True, exist_ok=True)
        
    def sync_catalog_to_mcp_db(self):
        """カタログダウンローダーのデータをMCPデータベースに同期"""
        db_path = self.mcp_data_dir / "catalog_index.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # テーブルが存在しない場合は作成
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats_tables (
                stats_data_id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                description TEXT,
                organization TEXT,
                field_code TEXT,
                field_name TEXT,
                keywords TEXT,
                available_areas TEXT,
                categories TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 既存のカタログファイルを検索
        csv_files = list(self.catalog_dir.glob("*_combined_*.csv"))
        
        if csv_files:
            # 最新のカタログファイルを使用
            latest_catalog = max(csv_files, key=lambda x: x.stat().st_mtime)
            print(f"カタログファイルを読み込み中: {latest_catalog}")
            
            catalog_df = pd.read_csv(latest_catalog)
            
            # データをMCPデータベース形式に変換
            for _, row in catalog_df.iterrows():
                # キーワードの生成（統計名とタイトルから）
                keywords = self._extract_keywords(row.get('STAT_NAME', ''), row.get('TITLE', ''))
                
                cursor.execute('''
                    INSERT OR REPLACE INTO stats_tables 
                    (stats_data_id, table_name, description, organization, field_code, field_name, keywords)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row.get('TABLE_INF', ''),
                    row.get('STAT_NAME', ''),
                    row.get('TITLE', ''),
                    row.get('GOV_ORG', ''),
                    row.get('FIELD_CODE', ''),
                    row.get('FIELD_NAME', ''),
                    json.dumps(keywords)
                ))
            
            conn.commit()
            print(f"データベースに {len(catalog_df)} 件のデータを同期しました")
        
        conn.close()
    
    def _extract_keywords(self, stat_name: str, title: str) -> List[str]:
        """統計名とタイトルからキーワードを抽出"""
        keywords = []
        text = f"{stat_name} {title}".lower()
        
        # 主要キーワードの辞書
        keyword_patterns = {
            "人口": ["人口", "国勢", "住民"],
            "労働": ["労働", "雇用", "失業", "就業"],
            "賃金": ["賃金", "給与", "所得"],
            "物価": ["物価", "価格", "指数"],
            "家計": ["家計", "消費", "支出"],
            "企業": ["企業", "法人", "会社"],
            "建設": ["建設", "建築", "住宅"],
            "農業": ["農業", "農林", "作物"],
            "工業": ["工業", "製造", "生産"],
            "商業": ["商業", "小売", "卸売"],
        }
        
        for keyword, patterns in keyword_patterns.items():
            if any(pattern in text for pattern in patterns):
                keywords.append(keyword)
        
        return keywords

    def update_catalog_and_sync(self):
        """カタログを更新してMCPデータベースに同期"""
        print("=== カタログの更新開始 ===")
        
        # カタログダウンローダーを実行
        downloader = EStatCatalogDownloader()
        all_catalog = downloader.download_all_stats_catalog(limit=1000)
        
        if not all_catalog.empty:
            catalogs = downloader.classify_by_field(all_catalog)
            downloader.save_catalogs(catalogs)
            
            # MCPデータベースに同期
            print("=== MCPデータベースへの同期開始 ===")
            self.sync_catalog_to_mcp_db()
            
            print("=== 統合完了 ===")
        else:
            print("カタログデータを取得できませんでした")


def main():
    """統合処理のテスト"""
    integrator = CatalogIntegrator()
    
    # 既存カタログがあれば同期、なければ新規作成
    integrator.update_catalog_and_sync()


if __name__ == "__main__":
    main()