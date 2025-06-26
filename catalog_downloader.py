#!/usr/bin/env python3
"""
e-Stat データカタログダウンローダー

このスクリプトは政府統計の総合窓口(e-Stat)から統計データのカタログ情報を取得し、
分析・活用しやすい形式で保存します。
"""

import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import ClassVar, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from pandas_estat import read_statslist, set_appid


class EStatCatalogDownloader:
    """e-Stat統計データカタログのダウンローダークラス"""

    # 主要な統計分野コード(大分類)
    MAJOR_FIELDS: ClassVar[Dict[str, str]] = {
        "00": "国勢統計",
        "01": "人口・世帯",
        "02": "自然環境・災害",
        "03": "労働・賃金",
        "04": "農林水産業",
        "05": "鉱工業",
        "06": "商業・サービス業",
        "07": "企業・家計・経済",
        "08": "住宅・土地・建設",
        "09": "エネルギー・水",
        "10": "運輸・観光",
        "11": "情報通信・科学技術",
        "12": "教育・文化・スポーツ・生活",
        "13": "行財政",
        "14": "司法・安全・環境",
        "99": "その他",
    }

    def __init__(self, appid: Optional[str] = None) -> None:
        """
        初期化

        Args:
            appid: e-StatのアプリケーションID(未指定の場合は環境変数から取得)
        """
        if appid is None:
            load_dotenv()
            appid = os.getenv("ESTAT_APPID")
            if not appid:
                raise ValueError(
                    "ESTAT_APPIDが設定されていません。.envファイルを確認してください。"
                )

        set_appid(appid)
        self.appid = appid

        # 出力ディレクトリの作成
        self.output_dir = "estat_catalog"
        os.makedirs(self.output_dir, exist_ok=True)

    def download_stats_list_by_field(self, field_code: str, limit: int = 1000) -> pd.DataFrame:
        """
        指定した統計分野の統計表リストをダウンロード

        Args:
            field_code: 統計分野コード(2桁または4桁)
            limit: 取得する最大件数

        Returns:
            統計表リストのDataFrame
        """
        field_name = self.MAJOR_FIELDS.get(field_code, "不明")
        print(f"統計分野 {field_code} ({field_name}) のデータを取得中...")

        try:
            # pandas-estatを使用して統計表リストを取得
            # read_statslistは第一引数にcodeが必要。統計分野で検索する場合は別のアプローチが必要
            # まずは既知の統計コードで試行
            if field_code == "01":  # 人口・世帯
                stats_list = read_statslist("00200521", limit=limit)  # 国勢調査
            elif field_code == "03":  # 労働・賃金
                stats_list = read_statslist("00450011", limit=limit)  # 労働力調査
            elif field_code == "07":  # 企業・家計・経済
                stats_list = read_statslist("00200553", limit=limit)  # 家計調査
            else:
                # その他の分野は一般的な統計コードで試行
                stats_list = read_statslist(limit=limit)

            print(f"  取得件数: {len(stats_list)}")
            return stats_list

        except Exception as e:
            print(f"  エラー: {e}")
            return pd.DataFrame()

    def download_all_stats_catalog(self, limit: int = 2000) -> pd.DataFrame:
        """
        全統計のカタログをダウンロード

        Args:
            limit: 取得する最大件数

        Returns:
            統計表リストのDataFrame
        """
        print("全統計データのカタログを取得中...")

        try:
            # e-Stat APIを直接使用して統計表情報を取得
            url = "https://api.e-stat.go.jp/rest/3.0/app/getStatsList"
            params = {"appId": self.appid, "limit": limit}

            response = requests.get(url, params=params)
            response.raise_for_status()

            # XMLレスポンスを解析
            root = ET.fromstring(response.text)

            # DATALIST_INF要素を探す
            datalist_inf = root.find(".//DATALIST_INF")
            if datalist_inf is None:
                print("統計データが見つかりませんでした")
                return pd.DataFrame()

            # TABLE_INF要素からデータを抽出
            table_infos = datalist_inf.findall("TABLE_INF")
            if not table_infos:
                print("統計表情報が見つかりませんでした")
                return pd.DataFrame()

            # データをリストに変換
            stats_data = []
            for table_inf in table_infos:
                record = {}
                for child in table_inf:
                    record[child.tag] = child.text
                stats_data.append(record)

            # DataFrameに変換
            stats_list = pd.DataFrame(stats_data)
            print(f"取得件数: {len(stats_list)}")

            # データ構造を確認
            print("取得データのカラム一覧:")
            for col in stats_list.columns:
                print(f"  - {col}")

            if len(stats_list) > 0:
                print("\n最初の1件のデータサンプル:")
                for col, val in stats_list.iloc[0].items():
                    print(f"  {col}: {val}")

            return stats_list

        except Exception as e:
            print(f"エラー: {e}")
            return pd.DataFrame()

    def classify_by_field(self, catalog: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        統計カタログを分野別に分類

        Args:
            catalog: 統計表リストのDataFrame

        Returns:
            分野コードをキーとする統計表リストの辞書
        """
        classified_catalogs = {}

        # 統計名や機関名から分野を推定
        field_keywords = {
            "01": ["人口", "世帯", "国勢調査", "住民基本台帳"],
            "02": ["災害", "環境", "気象", "地震"],
            "03": ["労働", "賃金", "雇用", "失業", "就業"],
            "04": ["農業", "林業", "水産", "漁業", "畜産"],
            "05": ["鉱業", "工業", "製造業", "生産"],
            "06": ["商業", "サービス", "小売", "卸売"],
            "07": ["企業", "家計", "経済", "GDP", "所得", "消費"],
            "08": ["住宅", "土地", "建設", "不動産"],
            "09": ["エネルギー", "電力", "ガス", "水道"],
            "10": ["運輸", "交通", "観光", "旅行"],
            "11": ["情報", "通信", "科学", "技術", "研究"],
            "12": ["教育", "文化", "スポーツ", "生活", "学校"],
            "13": ["行政", "財政", "税収", "予算"],
            "14": ["司法", "安全", "犯罪", "警察", "消防"],
        }

        for field_code, keywords in field_keywords.items():
            field_catalog = catalog[
                catalog["STAT_NAME"].str.contains("|".join(keywords), na=False, case=False)
                | catalog["GOV_ORG"].str.contains("|".join(keywords), na=False, case=False)
                | catalog["MAIN_CATEGORY"].str.contains("|".join(keywords), na=False, case=False)
            ].copy()

            if not field_catalog.empty:
                classified_catalogs[field_code] = field_catalog
                print(
                    f"分野 {field_code} ({self.MAJOR_FIELDS[field_code]}): {len(field_catalog)}件"
                )

        # 分類されなかったものは「その他」に
        classified_stats = (
            pd.concat(classified_catalogs.values()) if classified_catalogs else pd.DataFrame()
        )
        if len(classified_stats) < len(catalog):
            unclassified = catalog[~catalog.index.isin(classified_stats.index)].copy()
            if not unclassified.empty:
                classified_catalogs["99"] = unclassified
                print(f"分野 99 (その他): {len(unclassified)}件")

        return classified_catalogs

    def save_catalogs(self, catalogs: Dict[str, pd.DataFrame]) -> None:
        """
        カタログデータを複数の形式で保存

        Args:
            catalogs: 分野コードをキーとする統計表リストの辞書
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 1. 各分野のCSVファイルとして保存
        print("\n=== 分野別CSVファイルを作成中 ===")
        for field_code, catalog in catalogs.items():
            field_name = self.MAJOR_FIELDS.get(field_code, "unknown")
            filename = f"{self.output_dir}/{field_code}_{field_name}_{timestamp}.csv"
            catalog.to_csv(filename, index=False, encoding="utf-8-sig")
            print(f"保存: {filename} ({len(catalog)}件)")

        # 2. 全データを統合したCSVファイル
        print("\n=== 統合CSVファイルを作成中 ===")
        all_data = []
        for field_code, catalog in catalogs.items():
            catalog_copy = catalog.copy()
            catalog_copy["FIELD_CODE"] = field_code
            catalog_copy["FIELD_NAME"] = self.MAJOR_FIELDS.get(field_code, "unknown")
            all_data.append(catalog_copy)

        if all_data:
            combined_catalog = pd.concat(all_data, ignore_index=True)
            combined_filename = f"{self.output_dir}/estat_catalog_combined_{timestamp}.csv"
            combined_catalog.to_csv(combined_filename, index=False, encoding="utf-8-sig")
            print(f"統合ファイル保存: {combined_filename} ({len(combined_catalog)}件)")

        # 3. サマリー情報をJSONで保存
        print("\n=== サマリー情報を作成中 ===")
        summary = {
            "download_date": datetime.now().isoformat(),
            "total_records": sum(len(catalog) for catalog in catalogs.values()),
            "field_summary": {
                field_code: {
                    "name": self.MAJOR_FIELDS.get(field_code, "unknown"),
                    "record_count": len(catalog),
                    "organizations": catalog["GOV_ORG"].unique().tolist()
                    if "GOV_ORG" in catalog.columns
                    else [],
                    "survey_years": sorted(catalog["SURVEY_DATE"].dropna().unique().tolist())
                    if "SURVEY_DATE" in catalog.columns
                    else [],
                }
                for field_code, catalog in catalogs.items()
            },
        }

        summary_filename = f"{self.output_dir}/catalog_summary_{timestamp}.json"
        with open(summary_filename, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"サマリー保存: {summary_filename}")

    def create_catalog_index(self, catalogs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        カタログのインデックス(目次)を作成

        Args:
            catalogs: 分野コードをキーとする統計表リストの辞書

        Returns:
            カタログインデックスのDataFrame
        """
        index_data = []

        for field_code, catalog in catalogs.items():
            # 統計調査の種類を集計
            survey_types = (
                catalog.groupby(["STAT_NAME", "GOV_ORG"]).size().reset_index(name="table_count")
            )

            for _, row in survey_types.iterrows():
                index_data.append(
                    {
                        "field_code": field_code,
                        "field_name": self.MAJOR_FIELDS.get(field_code, "unknown"),
                        "stat_name": row["STAT_NAME"],
                        "organization": row["GOV_ORG"],
                        "table_count": row["table_count"],
                        "latest_survey": catalog[
                            (catalog["STAT_NAME"] == row["STAT_NAME"])
                            & (catalog["GOV_ORG"] == row["GOV_ORG"])
                        ]["SURVEY_DATE"].max()
                        if "SURVEY_DATE" in catalog.columns
                        else None,
                    }
                )

        return pd.DataFrame(index_data)


def main() -> None:
    """メイン実行関数"""
    print("=== e-Stat データカタログダウンローダー ===")
    print("政府統計の総合窓口から統計データカタログを取得します\n")

    try:
        # ダウンローダーを初期化
        downloader = EStatCatalogDownloader()

        # 全統計のカタログをダウンロード
        print("全統計データのカタログをダウンロード開始...")
        all_catalog = downloader.download_all_stats_catalog(limit=2000)

        if all_catalog.empty:
            print("カタログデータを取得できませんでした。")
            return

        # 分野別に分類
        print("\n統計データを分野別に分類中...")
        catalogs = downloader.classify_by_field(all_catalog)

        if not catalogs:
            print("カタログデータを取得できませんでした。")
            return

        # カタログを保存
        print(f"\n取得完了! {len(catalogs)}分野のデータを保存します...")
        downloader.save_catalogs(catalogs)

        # インデックスを作成・保存
        print("\nカタログインデックスを作成中...")
        index_df = downloader.create_catalog_index(catalogs)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        index_filename = f"{downloader.output_dir}/catalog_index_{timestamp}.csv"
        index_df.to_csv(index_filename, index=False, encoding="utf-8-sig")
        print(f"インデックス保存: {index_filename}")

        # 完了メッセージ
        total_records = sum(len(catalog) for catalog in catalogs.values())
        print("\n=== 完了 ===")
        print(f"総取得件数: {total_records}件")
        print(f"出力ディレクトリ: {downloader.output_dir}/")
        print("カタログデータの準備が完了しました!")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        raise


if __name__ == "__main__":
    main()
