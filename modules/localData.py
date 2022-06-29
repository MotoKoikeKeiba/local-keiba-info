import time
import pandas as pd
import re
import requests
from tqdm.notebook import tqdm
from urllib.request import urlopen
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm

"""
地方競馬情報サイトより、現役の競走馬データをスクレイピングする関数
Parameters:なし
----------
Returns:
----------
df : pandas.DataFrame
    現役の競走馬データをまとめてDataFrame型にしたもの
"""
def scrape():
    #race_idをkeyにしてDataFrame型を格納
    race_results = {}
    
    df = pd.DataFrame()
    place_no_arr = ['1', '2', '6', '7', '8', '11', '15', '17', '21', '26', '27', '28', '29']
    #place_no_arr = ['1']

    for place_no in tqdm(place_no_arr):
        for i in range(40):
            # 相手サーバーに負担をかけないように1秒待機する
            time.sleep(1)
            try:
                # URL生成
                url = 'https://www.keiba.go.jp/KeibaWeb/DataRoom/RaceHorseList?k_flag=1&k_pageNum=' + str(i)\
                    + '&k_horseName=&k_horseNameCondition=start&k_horsebelong=' + place_no\
                    + '&k_birthYear=*&k_fatherHorse=&k_fatherHorseCondition=start&k_motherHorse=&k_motherHorseCondition=start&k_activeCode=1&k_dataKind=1'
                
                # メインとなるテーブルデータを取得
                df_tmp = pd.read_html(url)[0]

                # カラム名にNo.が含まれていない場合は、処理を抜ける
                if 'No.' not in df_tmp.columns :
                    break

                html = requests.get(url)
                html.encoding = "UTF-8"
                soup = BeautifulSoup(html.text, "html.parser")
                #print(soup)

                # horse_idを取得
                horse_id_list = []
                horse_td_list = soup.find("table", attrs={'class': 'RaceHorseList'}).find_all("a", attrs={"href": re.compile("lineageLoginCode")})
                #print(horse_td_list)
                for td in horse_td_list:
                    #print(td)
                    horse_id = re.findall(r"\d+", td["href"])[0]
                    horse_id_list.append(horse_id)
                    #print(horse_id)

                df_tmp["horse_id"] = horse_id_list

                df = pd.concat([df, df_tmp], ignore_index=True)

            except Exception as e:
                print(e)
                break
            except:
                break

    return df

"""
地方競馬情報サイトより、指定されたhorse_idの競走馬データをスクレイピングする関数
Parameters:
horse_id_list : list
----------
Returns:
----------
df : pandas.DataFrame
    馬主データをまとめてDataFrame型にしたもの
"""
def scrape_horse(horse_id_list: list):
    #horse_idをkeyにしてDataFrame型を格納    
    owner_results = []
    df = pd.DataFrame()
    horse_id_list_tmp = []
    for horse_id in tqdm(horse_id_list):
        #print(horse_id)
        # 相手サーバーに負担をかけないように1秒待機する
        time.sleep(1)
        try:
            # URL生成
            url = 'https://www.keiba.go.jp/KeibaWeb/DataRoom/RaceHorseInfo?k_lineageLoginCode=' + str(horse_id) + '&k_activeCode=1'
            # メインとなるテーブルデータを取得
            df_tmp = pd.read_html(url)[0]

            html = requests.get(url)
            html.encoding = "UTF-8"
            soup = BeautifulSoup(html.text, "html.parser")

            # 馬情報を取得
            horse_info_list = []
            horse_info_list = soup.find("table", attrs={'class': 'horse_info_table'}).find("tbody")

            # 調教師
            #print(str(umanushi_list.find_all('td')).split(',')[3])

            # 馬主
            #print(str(umanushi_list.find_all('td')).split(',')[9])
            #print(str(umanushi_list.find_all('td')).split(',')[10])

            owner_idx = 0
            trainer_idx = 0

            if '馬主' in str(horse_info_list.find_all('td')).split(',')[7]:
                owner_idx = 7

            elif '馬主' in str(horse_info_list.find_all('td')).split(',')[8]:
                owner_idx = 8

            elif '馬主' in str(horse_info_list.find_all('td')).split(',')[9]:
                owner_idx = 9

            elif '馬主' in str(horse_info_list.find_all('td')).split(',')[10]:
                owner_idx = 10

            elif '馬主' in str(horse_info_list.find_all('td')).split(',')[11]:
                owner_idx = 11
            
            else:
                print('err 馬主 ' + horse_id)

            if owner_idx > 0:

                owner = str(horse_info_list.find_all('td')).split(',')[owner_idx + 1]
                owner = owner.replace('<td>', '').replace('</td>', '').replace('　', ' ')
                #print(owner)

                owner_results.append(owner)
                horse_id_list_tmp.append(horse_id)
                #print(owner_results)
                          
        except Exception as e:
            print(e)
            break

        except:
            break

    df = pd.DataFrame(owner_results, columns = ['owner'], index=[horse_id_list_tmp])

    return df

"""
地方競馬情報サイトより、指定されたhorse_idの馬情報をスクレイピングする関数（未使用）
Parameters:
horse_id_list : list
----------
Returns:
----------
df : pandas.DataFrame
    馬情報をまとめてDataFrame型にしたもの
"""
def scrape_horse_info(horse_id_list: list):
    owner_results = []
    trainer_results = []
    #horse_idをkeyにしてDataFrame型を格納    
    df = pd.DataFrame()
    horse_id_list_tmp = []
    for horse_id in tqdm(horse_id_list):
        # 相手サーバーに負担をかけないように1秒待機する
        time.sleep(1)
        try:
            # URL生成
            url = 'https://www.keiba.go.jp/KeibaWeb/DataRoom/RaceHorseInfo?k_lineageLoginCode=' + str(horse_id) + '&k_activeCode=1'
            # メインとなるテーブルデータを取得
            df_tmp = pd.read_html(url)[0]

            html = requests.get(url)
            html.encoding = "UTF-8"
            soup = BeautifulSoup(html.text, "html.parser")

            # umanushiを取得
            umanushi_list = []
            umanushi_list = soup.find("table", attrs={'class': 'horse_info_table'}).find("tbody")

            # 調教師
            #print(str(umanushi_list.find_all('td')).split(',')[3])

            # 馬主
            #print(str(umanushi_list.find_all('td')).split(',')[9])
            #print(str(umanushi_list.find_all('td')).split(',')[10])

            owner_idx = 0
            trainer_idx = 0

            if '馬主' in str(umanushi_list.find_all('td')).split(',')[7]:
                owner_idx = 7

            elif '馬主' in str(umanushi_list.find_all('td')).split(',')[8]:
                owner_idx = 8

            elif '馬主' in str(umanushi_list.find_all('td')).split(',')[9]:
                owner_idx = 9

            elif '馬主' in str(umanushi_list.find_all('td')).split(',')[10]:
                owner_idx = 10

            elif '馬主' in str(umanushi_list.find_all('td')).split(',')[11]:
                owner_idx = 11
            
            else:
                print('err 馬主 ' + horse_id)
           
            if '調教師' in str(umanushi_list.find_all('td')).split(',')[2]:
                trainer_idx = 2
            
            elif '調教師' in str(umanushi_list.find_all('td')).split(',')[3]:
                trainer_idx = 3
            
            elif '調教師' in str(umanushi_list.find_all('td')).split(',')[4]:
                trainer_idx = 4
            
            else:
                print('err 調教師 ' + horse_id)

            if owner_idx > 0 and trainer_idx > 0:

                owner = str(umanushi_list.find_all('td')).split(',')[owner_idx + 1]
                owner = owner.replace('<td>', '').replace('</td>', '').replace('　', ' ')
                #print(owner)

                trainer = str(umanushi_list.find_all('td')).split(',')[trainer_idx + 1]
                trainer = trainer.replace('<td>', '').replace('</td>', '').replace('　', ' ')
                #print(trainer)

                owner_results.append(owner)
                trainer_results.append(trainer)
                horse_id_list_tmp.append(horse_id)
                #print(owner_results)
                #print(trainer_results)
                           
        except Exception as e:
            print(e)
            break

        except:
            break
    
    mylist = list(zip(owner_results, trainer_results))
    #df = pd.DataFrame(owner_results, columns = ['owner'], index=[horse_id_list_tmp])
    df = pd.DataFrame(mylist, columns = ['owner', 'trainer'], index=[horse_id_list_tmp])
    
    return df