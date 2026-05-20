"""
Ollama統合モジュール
AIが統計表IDと軸情報を動的に提案する機能
"""

import json
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests


@dataclass
class OllamaResponse:
    """Ollamaからのレスポンス"""

    stats_table_id: str
    table_name: str
    axis_mappings: Dict[str, str]
    confidence: float
    reasoning: str


class OllamaStatsMCP:
    """Ollama統合によるe-stat統計表とパラメータ提案システム"""

    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3.2"):
        self.base_url = base_url
        self.model = model
        self.available = self._check_ollama_availability()

        # 実際のe-statメタデータを読み込み
        self._load_real_estat_data()

        # 軸コード情報
        self.axis_knowledge_base = {
            "地域軸": {
                "cdArea": "地域コード",
                "全国": "00000",
                "都道府県": "01000-47000",
                "市区町村": "詳細コード",
            },
            "時間軸": {
                "cdTime": "時間コード",
                "年次": "YYYY000000",
                "月次": "YYYYMM0000",
                "四半期": "YYYYQQ0000",
            },
            "分類軸": {
                "cdCat01": "第1分類（年齢、性別等）",
                "cdCat02": "第2分類（職業、産業等）",
                "cdCat03": "第3分類（詳細分類）",
            },
        }

    def _check_ollama_availability(self) -> bool:
        """Ollamaの利用可能性をチェック"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False

    def _call_ollama(self, prompt: str) -> str:
        """Ollamaに問い合わせを実行"""
        if not self.available:
            return self._fallback_response(prompt)

        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": False},
                timeout=30,
            )

            if response.status_code == 200:
                return response.json().get("response", "")
            else:
                return self._fallback_response(prompt)

        except Exception as e:
            print(f"Ollama接続エラー: {e}")
            return self._fallback_response(prompt)

    def _fallback_response(self, prompt: str) -> str:
        """Ollamaが利用できない場合のフォールバック"""
        # 簡単なルールベースの推定
        if "人口" in prompt:
            return json.dumps(
                {
                    "stats_table_id": "00200521001",
                    "table_name": "国勢調査（人口等基本集計）",
                    "axis_mappings": {
                        "cdCat01": "年齢階級",
                        "cdCat02": "男女別",
                        "cdArea": "地域コード",
                    },
                    "confidence": 0.7,
                    "reasoning": "人口関連クエリのため国勢調査を選択",
                }
            )
        elif "労働" in prompt or "失業" in prompt:
            return json.dumps(
                {
                    "stats_table_id": "00450011001",
                    "table_name": "労働力調査（基本集計）",
                    "axis_mappings": {"cdCat01": "労働力状態", "cdCat02": "男女別"},
                    "confidence": 0.6,
                    "reasoning": "労働関連クエリのため労働力調査を選択",
                }
            )
        else:
            return json.dumps(
                {
                    "stats_table_id": "00200521001",
                    "table_name": "国勢調査（人口等基本集計）",
                    "axis_mappings": {"cdArea": "地域コード"},
                    "confidence": 0.4,
                    "reasoning": "一般的なクエリのためデフォルト統計表を選択",
                }
            )

    def suggest_stats_table_and_axes(
        self,
        query: str,
        region: Optional[str] = None,
        time_period: Optional[str] = None,
    ) -> OllamaResponse:
        """クエリに基づいて統計表IDと軸情報を提案"""

        # Ollamaに送るプロンプトを構築
        prompt = self._build_suggestion_prompt(query, region, time_period)

        # Ollamaに問い合わせ
        ai_response = self._call_ollama(prompt)

        # レスポンスを解析
        return self._parse_ollama_response(ai_response)

    def _build_suggestion_prompt(
        self, query: str, region: Optional[str], time_period: Optional[str]
    ) -> str:
        """Ollama用のプロンプトを構築"""

        # 実際のe-stat統計表情報をコンテキストとして提供
        stats_context = self._get_comprehensive_stats_context()

        # 軸情報のコンテキスト
        axis_context = "\n軸コード情報:\n"
        for axis_type, info in self.axis_knowledge_base.items():
            axis_context += f"\n【{axis_type}】\n"
            for code, description in info.items():
                if isinstance(description, str):
                    axis_context += f"  {code}: {description}\n"

        prompt = f"""
あなたは日本の政府統計データ（e-stat）の専門家です。
以下のクエリに最適な統計表IDと軸パラメータを提案してください。

【クエリ】
{query}

【追加情報】
地域: {region if region else "指定なし"}
時期: {time_period if time_period else "指定なし"}

{stats_context}

{axis_context}

【回答形式】
以下のJSON形式で回答してください:
{{
    "stats_table_id": "統計表ID",
    "table_name": "統計表名",
    "axis_mappings": {{
        "cdArea": "地域軸の説明",
        "cdTime": "時間軸の説明", 
        "cdCat01": "第1分類軸の説明",
        "cdCat02": "第2分類軸の説明"
    }},
    "confidence": 0.0-1.0の信頼度,
    "reasoning": "選択理由の説明"
}}

注意事項:
- 統計表IDは必ず上記リストから選択
- 軸マッピングは実際に必要なもののみ含める
- 信頼度は選択の確実性を0-1で評価
- 理由は簡潔に日本語で説明

クエリに最も適した統計表とパラメータを提案してください。
        """

        return prompt

    def _parse_ollama_response(self, ai_response: str) -> OllamaResponse:
        """Ollamaのレスポンスを解析"""
        try:
            # JSON部分を抽出
            json_match = re.search(r"\{.*\}", ai_response, re.DOTALL)
            if json_match:
                response_data = json.loads(json_match.group())

                return OllamaResponse(
                    stats_table_id=response_data.get("stats_table_id", ""),
                    table_name=response_data.get("table_name", ""),
                    axis_mappings=response_data.get("axis_mappings", {}),
                    confidence=response_data.get("confidence", 0.0),
                    reasoning=response_data.get("reasoning", ""),
                )
            else:
                # JSON解析に失敗した場合のフォールバック
                return self._create_fallback_response()

        except Exception as e:
            print(f"Ollamaレスポンス解析エラー: {e}")
            return self._create_fallback_response()

    def _create_fallback_response(self) -> OllamaResponse:
        """フォールバック用のレスポンス"""
        return OllamaResponse(
            stats_table_id="00200521001",
            table_name="国勢調査（人口等基本集計）",
            axis_mappings={"cdArea": "地域コード"},
            confidence=0.3,
            reasoning="AI解析に失敗したためデフォルト統計表を選択",
        )

    def explain_axis_codes(self, stats_table_id: str) -> Dict[str, str]:
        """統計表IDに対応する軸コードの詳細説明を取得"""

        # 実際のメタデータから軸情報を取得
        if self.metadata_loader:
            try:
                real_axis_details = self.metadata_loader.get_table_axis_details(stats_table_id)
                if real_axis_details:
                    # 実際のデータを整理してフォーマット
                    formatted_axes = {}
                    for axis_id, axis_info in real_axis_details.items():
                        examples = {}
                        for value in axis_info.get("values", [])[:5]:  # 最初の5件
                            examples[value["code"]] = value["name"]

                        formatted_axes[axis_id] = {
                            "description": f"{axis_info.get('axis_name', '')} - {axis_info.get('class_name', '')}",
                            "unit": axis_info.get("unit", ""),
                            "examples": examples,
                        }

                    if formatted_axes:
                        return formatted_axes
            except Exception as e:
                print(f"実際の軸データ取得エラー: {e}")

        # Ollamaに問い合わせ（実データが利用できない場合）
        prompt = f"""
統計表ID「{stats_table_id}」で利用可能な軸コードとその具体的な値について説明してください。

{stats_context if hasattr(self, "stats_context") else ""}

例えば:
- cdCat01で「001」は何を意味するか
- cdAreaで「13000」は何を意味するか
- cdTimeで「2020000000」は何を意味するか

JSON形式で回答:
{{
    "cdArea": {{
        "description": "地域軸の説明",
        "examples": {{"00000": "全国", "13000": "東京都"}}
    }},
    "cdTime": {{
        "description": "時間軸の説明", 
        "examples": {{"2020000000": "2020年", "202001000000": "2020年1月"}}
    }},
    "cdCat01": {{
        "description": "第1分類軸の説明",
        "examples": {{"001": "総数", "002": "男性"}}
    }}
}}
        """

        ai_response = self._call_ollama(prompt)

        try:
            json_match = re.search(r"\{.*\}", ai_response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
        except:
            pass

        # フォールバック
        return {
            "cdArea": {
                "description": "地域コード（都道府県・市区町村）",
                "examples": {"00000": "全国", "13000": "東京都", "27000": "大阪府"},
            },
            "cdTime": {
                "description": "時間コード（年月日）",
                "examples": {"2020000000": "2020年", "2024000000": "2024年"},
            },
            "cdCat01": {
                "description": "第1分類（年齢・性別等）",
                "examples": {"001": "総数", "002": "男性", "003": "女性"},
            },
        }

    def _load_real_estat_data(self):
        """実際のe-statメタデータを読み込み"""
        try:
            from .estat_metadata_loader import EstatMetadataLoader

            self.metadata_loader = EstatMetadataLoader()

            # Ollama用の統計表データを読み込み
            self.real_stats_data = self.metadata_loader.load_all_stats_for_ollama()

            print(
                f"✅ 実際のe-statデータを読み込み: {self.real_stats_data.get('统计表总数', 0)}件の統計表"
            )

            # 統計表データが空の場合はキャッシュ更新を提案
            if self.real_stats_data.get("统计表总数", 0) == 0:
                print("⚠️ 統計表データが見つかりません。メタデータキャッシュの更新が必要です。")
                self._update_metadata_if_needed()

        except ImportError:
            print("⚠️ estat_metadata_loader が見つかりません。サンプルデータを使用します。")
            self.real_stats_data = None
            self.metadata_loader = None
        except Exception as e:
            print(f"⚠️ e-statデータ読み込みエラー: {e}")
            self.real_stats_data = None
            self.metadata_loader = None

    def _update_metadata_if_needed(self):
        """必要に応じてメタデータキャッシュを更新"""
        if self.metadata_loader:
            print("🔄 e-statメタデータキャッシュを更新中...")
            try:
                self.metadata_loader.update_metadata_cache(max_tables=500)
                self.real_stats_data = self.metadata_loader.load_all_stats_for_ollama()
                print(f"✅ 更新完了: {self.real_stats_data.get('统计表总数', 0)}件の統計表")
            except Exception as e:
                print(f"❌ メタデータ更新エラー: {e}")

    def _get_comprehensive_stats_context(self) -> str:
        """包括的な統計表コンテキストを生成"""
        if not self.real_stats_data:
            # フォールバック: 基本的な統計表情報
            return """
利用可能な主要統計表（サンプル）:
【人口・世帯】
  00200521001: 国勢調査（人口等基本集計）
  0000020101: 人口推計（月報）
【労働・雇用】  
  00450011001: 労働力調査（基本集計）
【家計・消費】
  00200553001: 家計調査（家計収支編）
            """

        # 実際のデータから詳細なコンテキストを生成
        context = f"""
e-stat政府統計データベース（統計表総数: {self.real_stats_data.get("统计表总数", 0)}件）
最終更新: {self.real_stats_data.get("最新更新", "unknown")}

=== 利用可能な統計表一覧 ===
"""

        # カテゴリ別統計表を整理
        categories = self.real_stats_data.get("分类统计表", {})

        for main_category, subcategories in categories.items():
            context += f"\n【{main_category}】\n"

            for sub_category, tables in subcategories.items():
                context += f"  ▶ {sub_category}\n"

                # 各サブカテゴリから代表的な統計表を選択（最大3件）
                for table in tables[:3]:
                    table_id = table.get("统计表ID", "")
                    stat_name = table.get("统计名称", "")
                    title = table.get("表标题", "")
                    org = table.get("实施机关", "")

                    context += f"    {table_id}: {stat_name} - {title} ({org})\n"

                    # 利用可能な軸情報
                    axes = table.get("可用轴", {})
                    if axes:
                        context += f"      軸: {', '.join(axes.keys())}\n"

                if len(tables) > 3:
                    context += f"    ... 他{len(tables) - 3}件\n"

        return context

    def get_ollama_status(self) -> Dict[str, any]:
        """Ollama接続状況を取得"""
        return {
            "available": self.available,
            "base_url": self.base_url,
            "model": self.model,
            "stats_tables_count": sum(len(tables) for tables in self.stats_knowledge_base.values()),
        }


# 使用例とテスト
def main():
    """Ollama統合のテスト"""
    print("=== Ollama統合 e-stat MCP テスト ===")

    ollama_mcp = OllamaStatsMCP()

    # 接続状況確認
    status = ollama_mcp.get_ollama_status()
    print(f"Ollama接続状況: {status}")

    # テストクエリ
    test_queries = [
        "東京都の年齢別人口が知りたい",
        "最新の完全失業率を見たい",
        "都道府県別の人口推移を比較したい",
    ]

    for query in test_queries:
        print(f"\n--- クエリ: {query} ---")

        response = ollama_mcp.suggest_stats_table_and_axes(
            query=query, region="東京都" if "東京" in query else None
        )

        print(f"統計表ID: {response.stats_table_id}")
        print(f"統計表名: {response.table_name}")
        print(f"軸マッピング: {response.axis_mappings}")
        print(f"信頼度: {response.confidence:.2f}")
        print(f"選択理由: {response.reasoning}")

        # 軸コードの詳細説明
        axis_details = ollama_mcp.explain_axis_codes(response.stats_table_id)
        print(f"軸コード詳細: {axis_details}")


if __name__ == "__main__":
    main()
