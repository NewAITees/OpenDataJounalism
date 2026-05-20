"""
e-stat メタデータローダー
実際のe-stat統計表情報とメタデータを取得・管理するモジュール
"""

import json
import os
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv


class EstatMetadataLoader:
    """e-stat統計表のメタデータを取得・管理するクラス"""

    def __init__(self, data_dir: str = "data/mcp"):
        load_dotenv()
        self.appid = os.getenv("ESTAT_APPID")
        if not self.appid:
            raise ValueError("ESTAT_APPIDが設定されていません。.envファイルを確認してください。")

        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # メタデータキャッシュ用のデータベース
        self.metadata_db = self.data_dir / "estat_metadata.db"
        self._init_metadata_db()

        # e-stat API のベースURL
        self.api_base = "https://api.e-stat.go.jp/rest/3.0/app"

    def _init_metadata_db(self):
        """メタデータ用データベースの初期化"""
        conn = sqlite3.connect(self.metadata_db)
        cursor = conn.cursor()

        # 統計表リストテーブル
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stats_tables (
                table_id TEXT PRIMARY KEY,
                stat_id TEXT,
                gov_org TEXT,
                stat_name TEXT,
                title TEXT,
                cycle TEXT,
                survey_date TEXT,
                open_date TEXT,
                small_area INTEGER,
                main_category_code TEXT,
                main_category TEXT,
                sub_category_code TEXT,
                sub_category TEXT,
                overall_total_number INTEGER,
                updated_date TEXT
            )
        """
        )

        # メタデータテーブル（各統計表の詳細軸情報）
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS table_metadata (
                table_id TEXT,
                class_obj_id TEXT,
                class_obj_name TEXT,
                class_name TEXT,
                level TEXT,
                unit TEXT,
                PRIMARY KEY (table_id, class_obj_id)
            )
        """
        )

        # クラス値テーブル（各軸の具体的な値）
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS class_values (
                table_id TEXT,
                class_obj_id TEXT,
                class_code TEXT,
                class_name TEXT,
                level TEXT,
                parent_code TEXT,
                PRIMARY KEY (table_id, class_obj_id, class_code)
            )
        """
        )

        conn.commit()
        conn.close()

    def fetch_all_stats_tables(self, limit: int = 10000) -> List[Dict]:
        """全統計表の基本情報を取得"""
        print(f"📊 e-statから統計表リストを取得中（最大{limit}件）...")

        try:
            url = f"{self.api_base}/getStatsList"
            params = {
                "appId": self.appid,
                "limit": limit,
                "searchWord": "",  # 検索語なしで全統計表を取得
                "collect": "Y",  # 収集済みデータのみ
            }

            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()

            # XMLレスポンスを解析
            root = ET.fromstring(response.text)

            # DATALIST_INF要素を探す
            datalist_inf = root.find(".//DATALIST_INF")
            if datalist_inf is None:
                print("統計データが見つかりませんでした")
                return []

            # TABLE_INF要素からデータを抽出
            table_infos = datalist_inf.findall("TABLE_INF")
            stats_tables = []

            for table_inf in table_infos:
                table_data = {}
                for child in table_inf:
                    table_data[child.tag] = child.text if child.text else ""

                # データベース保存用に整理
                processed_table = {
                    "table_id": table_data.get("TABLE_INF", ""),
                    "stat_id": table_data.get("STAT_NAME_CODE", ""),
                    "gov_org": table_data.get("GOV_ORG", ""),
                    "stat_name": table_data.get("STAT_NAME", ""),
                    "title": table_data.get("TITLE", ""),
                    "cycle": table_data.get("CYCLE", ""),
                    "survey_date": table_data.get("SURVEY_DATE", ""),
                    "open_date": table_data.get("OPEN_DATE", ""),
                    "small_area": 1 if table_data.get("SMALL_AREA") == "1" else 0,
                    "main_category_code": table_data.get("MAIN_CATEGORY_CODE", ""),
                    "main_category": table_data.get("MAIN_CATEGORY", ""),
                    "sub_category_code": table_data.get("SUB_CATEGORY_CODE", ""),
                    "sub_category": table_data.get("SUB_CATEGORY", ""),
                    "overall_total_number": (
                        int(table_data.get("OVERALL_TOTAL_NUMBER", 0))
                        if table_data.get("OVERALL_TOTAL_NUMBER")
                        else 0
                    ),
                    "updated_date": datetime.now().isoformat(),
                }

                stats_tables.append(processed_table)

            print(f"✅ {len(stats_tables)}件の統計表情報を取得しました")
            return stats_tables

        except Exception as e:
            print(f"統計表リスト取得エラー: {e}")
            return []

    def fetch_table_metadata(self, table_id: str) -> Dict:
        """特定統計表のメタデータ（軸情報）を取得"""
        print(f"🔍 統計表 {table_id} のメタデータを取得中...")

        try:
            url = f"{self.api_base}/getMetaInfo"
            params = {"appId": self.appid, "statsDataId": table_id}

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            # XMLレスポンスを解析
            root = ET.fromstring(response.text)

            metadata = {"table_id": table_id, "class_objects": [], "class_values": {}}

            # CLASS_INF要素を探す
            class_infs = root.findall(".//CLASS_INF")

            for class_inf in class_infs:
                class_obj = {}

                # CLASS_OBJ要素の情報
                class_obj_elem = class_inf.find("CLASS_OBJ")
                if class_obj_elem is not None:
                    class_obj["id"] = class_obj_elem.get("id", "")
                    class_obj["name"] = class_obj_elem.get("name", "")

                    # CLASS要素の情報
                    class_elem = class_obj_elem.find("CLASS")
                    if class_elem is not None:
                        class_obj["class_name"] = class_elem.get("name", "")
                        class_obj["level"] = class_elem.get("level", "")
                        class_obj["unit"] = class_elem.get("unit", "")

                # CLASS_VALUE要素の値一覧
                class_values = []
                class_value_elems = class_inf.findall(".//CLASS_VALUE")

                for class_value in class_value_elems:
                    value_info = {
                        "code": class_value.get("code", ""),
                        "name": class_value.get("name", ""),
                        "level": class_value.get("level", ""),
                        "parent_code": class_value.get("parentCode", ""),
                    }
                    class_values.append(value_info)

                metadata["class_objects"].append(class_obj)
                metadata["class_values"][class_obj.get("id", "")] = class_values

            return metadata

        except Exception as e:
            print(f"メタデータ取得エラー（{table_id}）: {e}")
            return {}

    def save_stats_tables_to_db(self, stats_tables: List[Dict]):
        """統計表リストをデータベースに保存"""
        conn = sqlite3.connect(self.metadata_db)
        cursor = conn.cursor()

        for table in stats_tables:
            cursor.execute(
                """
                INSERT OR REPLACE INTO stats_tables 
                (table_id, stat_id, gov_org, stat_name, title, cycle, survey_date, 
                 open_date, small_area, main_category_code, main_category, 
                 sub_category_code, sub_category, overall_total_number, updated_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    table["table_id"],
                    table["stat_id"],
                    table["gov_org"],
                    table["stat_name"],
                    table["title"],
                    table["cycle"],
                    table["survey_date"],
                    table["open_date"],
                    table["small_area"],
                    table["main_category_code"],
                    table["main_category"],
                    table["sub_category_code"],
                    table["sub_category"],
                    table["overall_total_number"],
                    table["updated_date"],
                ),
            )

        conn.commit()
        conn.close()
        print(f"💾 {len(stats_tables)}件をデータベースに保存しました")

    def save_table_metadata_to_db(self, metadata: Dict):
        """統計表メタデータをデータベースに保存"""
        table_id = metadata["table_id"]
        conn = sqlite3.connect(self.metadata_db)
        cursor = conn.cursor()

        # メタデータテーブルに保存
        for class_obj in metadata.get("class_objects", []):
            cursor.execute(
                """
                INSERT OR REPLACE INTO table_metadata 
                (table_id, class_obj_id, class_obj_name, class_name, level, unit)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    table_id,
                    class_obj.get("id", ""),
                    class_obj.get("name", ""),
                    class_obj.get("class_name", ""),
                    class_obj.get("level", ""),
                    class_obj.get("unit", ""),
                ),
            )

        # クラス値テーブルに保存
        for class_obj_id, class_values in metadata.get("class_values", {}).items():
            for value in class_values:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO class_values 
                    (table_id, class_obj_id, class_code, class_name, level, parent_code)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        table_id,
                        class_obj_id,
                        value.get("code", ""),
                        value.get("name", ""),
                        value.get("level", ""),
                        value.get("parent_code", ""),
                    ),
                )

        conn.commit()
        conn.close()
        print(f"💾 統計表 {table_id} のメタデータを保存しました")

    def load_all_stats_for_ollama(self) -> Dict:
        """Ollama用に全統計表情報を整理して返す"""
        conn = sqlite3.connect(self.metadata_db)
        cursor = conn.cursor()

        # 統計表基本情報を取得
        cursor.execute(
            """
            SELECT table_id, stat_name, title, main_category, sub_category, 
                   gov_org, survey_date, overall_total_number
            FROM stats_tables
            ORDER BY main_category_code, sub_category_code
        """
        )

        stats_tables = cursor.fetchall()

        # カテゴリ別に整理
        ollama_data = {
            "统计表总数": len(stats_tables),
            "最新更新": datetime.now().strftime("%Y-%m-%d"),
            "分类统计表": {},
        }

        for table in stats_tables:
            (
                table_id,
                stat_name,
                title,
                main_category,
                sub_category,
                gov_org,
                survey_date,
                total_num,
            ) = table

            if main_category not in ollama_data["分类统计表"]:
                ollama_data["分类统计表"][main_category] = {}

            if sub_category not in ollama_data["分类统计表"][main_category]:
                ollama_data["分类统计表"][main_category][sub_category] = []

            # 軸情報も取得
            cursor.execute(
                """
                SELECT class_obj_id, class_obj_name, class_name, unit
                FROM table_metadata
                WHERE table_id = ?
            """,
                (table_id,),
            )

            axes_info = cursor.fetchall()
            axes = {}
            for axis in axes_info:
                axis_id, axis_name, class_name, unit = axis
                axes[axis_id] = {
                    "name": axis_name,
                    "class": class_name,
                    "unit": unit if unit else "",
                }

            table_info = {
                "统计表ID": table_id,
                "统计名称": stat_name,
                "表标题": title,
                "实施机关": gov_org,
                "调查日期": survey_date,
                "数据总数": total_num,
                "可用轴": axes,
            }

            ollama_data["分类统计表"][main_category][sub_category].append(table_info)

        conn.close()
        return ollama_data

    def get_table_axis_details(self, table_id: str) -> Dict:
        """特定統計表の軸詳細情報を取得"""
        conn = sqlite3.connect(self.metadata_db)
        cursor = conn.cursor()

        # 軸情報を取得
        cursor.execute(
            """
            SELECT class_obj_id, class_obj_name, class_name, unit
            FROM table_metadata
            WHERE table_id = ?
        """,
            (table_id,),
        )

        axes = cursor.fetchall()
        axis_details = {}

        for axis in axes:
            axis_id, axis_name, class_name, unit = axis

            # 該当軸の値一覧を取得
            cursor.execute(
                """
                SELECT class_code, class_name, level, parent_code
                FROM class_values
                WHERE table_id = ? AND class_obj_id = ?
                ORDER BY class_code
            """,
                (table_id, axis_id),
            )

            values = cursor.fetchall()
            value_list = []
            for value in values:
                code, name, level, parent = value
                value_list.append({"code": code, "name": name, "level": level, "parent": parent})

            axis_details[axis_id] = {
                "axis_name": axis_name,
                "class_name": class_name,
                "unit": unit,
                "values": value_list,
            }

        conn.close()
        return axis_details

    def update_metadata_cache(self, max_tables: int = 100):
        """メタデータキャッシュの更新"""
        print("🔄 e-stat メタデータキャッシュを更新中...")

        # 1. 統計表リストを取得・保存
        stats_tables = self.fetch_all_stats_tables(limit=max_tables)
        if stats_tables:
            self.save_stats_tables_to_db(stats_tables)

        # 2. 主要統計表のメタデータを取得（人口・労働関連優先）
        priority_keywords = ["人口", "労働", "世帯", "家計", "国勢"]
        priority_tables = []

        for table in stats_tables:
            if any(
                keyword in table.get("stat_name", "") or keyword in table.get("title", "")
                for keyword in priority_keywords
            ):
                priority_tables.append(table["table_id"])

        print(f"🎯 優先統計表 {len(priority_tables[:20])}件のメタデータを取得中...")

        for i, table_id in enumerate(priority_tables[:20]):  # 上位20件のみ
            print(f"  {i + 1}/20: {table_id}")
            metadata = self.fetch_table_metadata(table_id)
            if metadata:
                self.save_table_metadata_to_db(metadata)

        print("✅ メタデータキャッシュの更新が完了しました")


def main():
    """メタデータローダーのテスト"""
    print("=== e-stat メタデータローダー ===")

    loader = EstatMetadataLoader()

    # メタデータキャッシュの更新
    loader.update_metadata_cache(max_tables=200)

    # Ollama用データの確認
    ollama_data = loader.load_all_stats_for_ollama()
    print("\nOllama用データ準備完了:")
    print(f"  統計表総数: {ollama_data['统计表总数']}")
    print(f"  カテゴリ数: {len(ollama_data['分类统计表'])}")

    # サンプル表示
    for category, subcategories in list(ollama_data["分类统计表"].items())[:3]:
        print(f"  📊 {category}: {sum(len(tables) for tables in subcategories.values())}件")


if __name__ == "__main__":
    main()
