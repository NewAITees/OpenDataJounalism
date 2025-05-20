import os
import pandas as pd
import matplotlib.pyplot as plt
import japanize_matplotlib
from pandas_estat import set_appid, read_statslist, read_statsdata
from dotenv import load_dotenv

def main():
    # .envファイルから環境変数を読み込む
    load_dotenv()
    
    # e-Statのアプリケーションキーを環境変数から取得
    estat_appid = os.getenv('ESTAT_APPID')
    if not estat_appid:
        raise ValueError("ESTAT_APPIDが設定されていません。.envファイルを確認してください。")
    
    # e-Statのアプリケーションキーを設定
    set_appid(estat_appid)

    # 人口に関する統計情報を検索（00200521は国勢調査）
    print("人口統計データを検索中...")
    population_stats = read_statslist("00200521")

    # 統計データのカテゴリ情報を表示
    print("\n===== 統計データのカテゴリ一覧 =====")
    categories = pd.DataFrame({
        "カテゴリコード": population_stats["MAIN_CATEGORY_CODE"].unique(),
        "カテゴリ名": population_stats["MAIN_CATEGORY"].unique()
    })
    print(categories)

    # サブカテゴリも表示
    print("\n===== サブカテゴリ一覧 =====")
    subcategories = pd.DataFrame({
        "サブカテゴリコード": population_stats["SUB_CATEGORY_CODE"].unique(),
        "サブカテゴリ名": population_stats["SUB_CATEGORY"].unique()
    })
    print(subcategories)

    # 最初の10件の統計表情報を表示
    print("\n===== 人口に関する統計表の例（最初の10件） =====")
    print(population_stats[["TABLE_INF", "TITLE", "SURVEY_DATE"]].head(10))

    # 例として、人口推移データを取得
    print("\n特定の統計データを取得しています...")
    stat_id = population_stats["TABLE_INF"].iloc[0]
    print(f"統計表ID {stat_id} のデータを取得中...")

    try:
        # 統計データの取得
        pop_data = read_statsdata(stat_id)
        
        # データの最初の5行を表示
        print("\n===== 取得したデータのサンプル =====")
        print(pop_data.head())
        
        # データの列（カテゴリ）の一覧を表示
        print("\n===== データカラム（カテゴリ情報） =====")
        for col in pop_data.columns:
            print(f"- {col}")
        
        # データの簡単な可視化
        print("\n簡単なグラフを作成中...")
        plt.figure(figsize=(12, 6))
        
        # 時間軸のカラム名を特定
        time_col = next((col for col in pop_data.columns if '時間' in col), None)
        
        if time_col:
            # 適当なカテゴリで絞り込み
            category_col = pop_data.columns[1]  # 2番目のカラムを使用
            first_category = pop_data[category_col].iloc[0]
            filtered_data = pop_data[pop_data[category_col] == first_category]
            
            # 時間順にソートしてプロット
            filtered_data = filtered_data.sort_values(time_col)
            plt.plot(filtered_data[time_col], filtered_data['value'], marker='o')
            plt.title(f'{first_category}の推移')
            plt.ylabel('値')
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
        else:
            print("時間軸のカラムが見つからないため、グラフ化はスキップします")
            
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        print("別の統計表IDを試してみてください。")


if __name__ == "__main__":
    main()
