import requests
import json
from bs4 import BeautifulSoup
import re
import os
from os import system
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import apiclient

ahrefs_token = None
ahrefs_url = 'https://apiv2.ahrefs.com/'
content_watch_key = None
content_watch_url = 'https://content-watch.ru/public/api/'
min_word_count = int()  # минимальное количество слов в теге article чтобы текст считался нормальным
min_uniqueness = int()  # минимальный процент уникальности текста для записи в таблицу
uniqueness_ch = True
open_extra_links = False

spreadsheetId = '1J852vBFt-dEt4gb7bHjfVawuTkdKQam0iA0H3POLjI0'
service = None


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36'
}


def get_info_from_gsheet():
    global ahrefs_token
    global content_watch_key
    global min_word_count
    global min_uniqueness

    credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json',
                                                                   ['https://www.googleapis.com/auth/spreadsheets',
                                                                    'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    global service
    service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)

    values = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId,
        majorDimension='COLUMNS',
        range='Info'
    ).execute()['values']

    info = {value[0]: value[1] for value in values}
    ahrefs_token = info.get('Ahrefs Token')
    content_watch_key = info.get('Content Watch Key')
    min_word_count = int(info.get('Min Words Count'))
    min_uniqueness = int(info.get('Min Uniqueness'))
    print("Data from GoogleSheets received")


def insert_into_gsheet(target, data):
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
    titles = {sheet['properties']['title']: sheet['properties']['sheetId'] for sheet in spreadsheet['sheets']}
    sheetTitle = target
    if titles.get(target):
        index = 2
        while titles.get(sheetTitle):
            sheetTitle = f'{target} - {index}'
            index += 1

    create = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body={
        "requests": {
            "addSheet": {
                "properties": {
                    "title": sheetTitle
                }
            }
        }
    }).execute()

    sheetId = create['replies'][0]['addSheet']['properties']['sheetId']

    values = [
        ['Title', 'Percent', 'Url', 'Length', 'Text']
    ]

    for item in data:
        tmp_list = [item.get('title'), item.get('percent'), item.get('url'), item.get('word_count'), item.get('article')]
        values.append(tmp_list)

    result = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheetId, body={
        'valueInputOption': 'USER_ENTERED',
        'data': [{
            'range': f'{sheetTitle}',
            'majorDimension': 'ROWS',
            'values': values
        }]
    }).execute()

    width = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body={
        'requests': [
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheetId,
                        'dimension': 'ROWS'
                    },
                    'properties': {
                        'pixelSize': 21
                    },
                    'fields': 'pixelSize'
                }
            },  # высота строк
            {
                'updateDimensionProperties': {  # title
                    'range': {
                        'sheetId': sheetId,
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': 1
                    },
                    'properties': {
                        'pixelSize': 355
                    },
                    'fields': 'pixelSize'
                }
            },
            {
                'updateDimensionProperties': {  # percent
                    'range': {
                        'sheetId': sheetId,
                        'dimension': 'COLUMNS',
                        'startIndex': 1,
                        'endIndex': 2
                    },
                    'properties': {
                        'pixelSize': 70
                    },
                    'fields': 'pixelSize'
                }
            },
            {
                'updateDimensionProperties': {  # url
                    'range': {
                        'sheetId': sheetId,
                        'dimension': 'COLUMNS',
                        'startIndex': 2,
                        'endIndex': 3
                    },
                    'properties': {
                        'pixelSize': 350
                    },
                    'fields': 'pixelSize'
                }
            },
            {
                'updateDimensionProperties': {  # length
                    'range': {
                        'sheetId': sheetId,
                        'dimension': 'COLUMNS',
                        'startIndex': 3,
                        'endIndex': 4
                    },
                    'properties': {
                        'pixelSize': 70
                    },
                    'fields': 'pixelSize'
                }
            },
            {
                'updateDimensionProperties': {  # text
                    'range': {
                        'sheetId': sheetId,
                        'dimension': 'COLUMNS',
                        'startIndex': 4,
                        'endIndex': 5
                    },
                    'properties': {
                        'pixelSize': 425
                    },
                    'fields': 'pixelSize'
                }
            },

            {
                'repeatCell': {
                    'cell': {
                        'userEnteredFormat': {
                            'horizontalAlignment': 'CENTER',
                            'backgroundColor': {
                                'red': 0.69,
                                'green': 1,
                                'blue': 0.64,
                                'alpha': 1
                            },
                            'textFormat': {
                                'bold': True,
                                'fontSize': 12
                            }
                        }
                    },
                    'range': {
                        'sheetId': sheetId,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': 5
                    },
                    'fields': 'userEnteredFormat'
                }
            }  # форматирвание шапки
        ]
    }).execute()

    url = f'https://docs.google.com/spreadsheets/d/{spreadsheetId}/edit#gid={sheetId}'
    return url


def get_target():
    domain = input('INPUT TARGET URL: ')
    try:
        domain = domain.split('/')[2].strip()
    except Exception:
        pass
    mode = 'domain' if domain.count('.') == 1 else 'subdomains'
    return domain, mode


def check_api():
    # AHREFS
    message = ''
    data = {
        'token': ahrefs_token,
        'from': 'subscription_info',
        'output': 'json',
        'select': 'rows_left'
    }
    try:
        response_ahrefs = json.loads(requests.post(ahrefs_url, data).text)['info']
        rows_left = response_ahrefs['rows_left'] # количество доступных API строк AHREFS
        if rows_left < 5000:
            message = f'The number of available ahrefs API rows is less than {5000}. AHREFS API TOKEN must be changed!'
            print(message)
            exit()
    except:
        message = 'AHREFS API TOKEN is invalid. Need to change'
        prepare_urls(message)
        exit()

    # CONTENT WATCH
    data = {
        'action': 'GET_BALANCE',
        'key': content_watch_key
    }
    balance, tariff = None, None
    try:
        response_content_watch = requests.post(content_watch_url, data).json()
        balance = float(response_content_watch['balance'])
        tariff = float(response_content_watch['tariff'])
    except:
        message = 'CONTENT WATCH API KEY is invalid. Need to change'
        print(message)
        exit()

    amount = int(balance / tariff)
    if amount < 500:
        answer = None
        try:
            answer = int(input(f'The number of texts available for verification for uniqueness is {amount}.\n'
                               'It may not be enough to check all the texts,'
                               ' so choose how to continue the program execution:\n'
                               '1. Exit the program\n'
                               '2. Write links from Web Archive to file and exit\n'
                               '3. Continue execution and check the available number of texts\n'
                               '>>> '))
        except ValueError:
            print('Bad input')
            exit()
        if answer in [1, 2, 3]:
            return str(answer)
        else:
            print('Bad input')
            exit()
    else:
        return True


def get_back_links_lost(target, mode):
    print('Getting lost backlinks...')
    data = {
        'token': ahrefs_token,
        'target': target,
        'from': 'backlinks_new_lost',
        'mode': mode,
        'where': 'http_code=404,type="lost"',
        'order_by': 'date:desc',
        'output': 'json',
        'select': 'url_from'
    }
    r = requests.post(ahrefs_url, data).text
    response = json.loads(r)['refpages']
    links = []
    for r in response:
        link = r['url_from'].strip()
        if link not in links:
            links.append(link)
    return links


def get_back_links_broken(target, mode):
    print('Getting broken backlinks...')
    data = {
        'token': ahrefs_token,
        'target': target,
        'from': 'broken_backlinks',
        'mode': mode,
        'order_by': 'last_visited:desc',
        'output': 'json',
        'select': 'url_to'
    }
    r = requests.post(ahrefs_url, data)
    response = json.loads(r.text)['refpages']
    links = []
    for r in response:
        link = r['url_to'].strip()
        if link not in links:
            links.append(link)
    return links


def check_http_status_code(urls):
    print('Checking http status code...')
    urls_404 = []
    for url in urls:
        try:
            status_code = requests.get(url, headers=headers).status_code
            if status_code == 404:
                urls_404.append(url)
        except Exception as e:
            pass
    return urls_404


def web_archive(urls):
    request_url = 'http://archive.org/wayback/available?url='
    web_archive_links = []
    for url in urls:
        current_url = request_url + url
        r = requests.get(current_url, headers=headers).text
        response = json.loads(r)['archived_snapshots']
        try:
            link = response['closest']['url']  # последний сниппет с вебархива
            web_archive_links.append(link)
        except KeyError:
            pass
    return web_archive_links


def word_counter(article):
    try:
        s = (re.findall(
            r"\b(\w*[A-Za-z0-9]+[!#$%&'\"*+-.^_`|~:\w]*)\b", article))
        return len(s)
    except:
        return None


def articles_word_count_checker(urls):
    normal_articles = []
    for url in urls:
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        try:
            article = soup.select_one('article')
            word_count = word_counter(article.text)
            if word_count >= min_word_count:
                article_text_striped = editing_article(article.text)
                normal_articles.append((url, article_text_striped))
        except Exception:
            pass
    return normal_articles


def editing_article(article):
    article = article.strip('\n').strip().replace('\n'*2, '\n').replace('\n'*3, '\n').replace('\n'*4, '\n').replace('\n'*2, '\n')
    return article


def prepare_urls(data):
    prepared_urls = []
    for url, article in data:
        new_line = url.strip()
        if new_line.find('/https://') != -1:
            new_line = new_line.replace('/https://', '/')
        elif new_line.find('/http://') != -1:
            new_line = new_line.replace('/http://', '/')
        prepared_urls.append((new_line, article))
    return prepared_urls


def uniqueness_check(articles_data):
    finished_list = []
    data = {
        'action': 'GET_BALANCE',
        'key': content_watch_key
    }
    response = requests.post(content_watch_url, data).json()
    balance = float(response['balance'])
    tariff = float(response['tariff'])
    amount = int(balance / tariff)

    if amount < len(articles_data):
        print(
            f'Texts to be checked: {len(articles_data)}. Available checks: {amount}. To be checked: {amount}')

        articles_to_write = articles_data[amount:]
        with open('extra_links.txt', 'w', encoding='utf-8') as f:
            for url, article in articles_to_write:
                f.write(url + '\n')
        global open_extra_links
        open_extra_links = True

        articles_data = articles_data[:amount]

    else:
        print(f'The balance is {balance} rubles. This is enough to check {amount} texts')

    data = {
        'action': 'CHECK_URL',
        'key': content_watch_key,
    }

    index = 1
    for url, article in articles_data:
        data['url'] = url
        response = requests.post(content_watch_url, data).json()
        if len(response['error']) > 0:
            continue
        percent = float(response['percent'])
        text = response['text'].strip()
        get_title = requests.get(url, headers=headers).text
        title = BeautifulSoup(get_title, "html.parser").title.text.strip()

        if percent >= min_uniqueness:
            d = {
                'title': str(title),
                'percent': str(percent).replace('.', ','),
                'url': str(url),
                'word_count': str(word_counter(article)),
                'article': article,
                'text': text
            }
            finished_list.append(d)
        print(f'[{index} / {len(articles_data)}] {title[:70]}... >>> {percent} >>> {word_counter(article)} >>> {url}')
        index += 1

    return finished_list


def main():
    directory_name = "results"
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)

    # AHREFS BLOCK
    target, mode = get_target()
    print("Target: ", target)
    back_links_lost = get_back_links_lost(target, mode)
    back_links_broken = get_back_links_broken(target, mode)
    back_links = back_links_lost + back_links_broken
    back_links = check_http_status_code(back_links)  # 404 урлы, с них будет тянутся ссылка с вебархива
    with open(f'{directory_name}\\ahrefs_links.txt', 'w', encoding='utf-8') as f:
        for back_link in back_links:
            f.write(back_link + '\n')
    print('Ahrefs backlinks count: ', len(back_links))

    # WEB ARCHIVE BLOCK
    web_archive_links = web_archive(back_links)
    normal_articles = prepare_urls(articles_word_count_checker(web_archive_links))  # статьи, которые будут проверятся на уникальность
    with open(f'{directory_name}\\webarchive_links.txt', 'w', encoding='utf-8') as f:
        for url, article in normal_articles:
            f.write(url + '\n')
    print('Web Archive links count: ', len(normal_articles))

    if not uniqueness_ch:
        with open('links.txt', 'w', encoding='utf-8') as f:
            for url, article in normal_articles:
                f.write(url + '\n')
        system('links.txt')
        exit()

    # CONTENT WATCH BLOCK
    print('Uniqueness verification process...')
    good_info = uniqueness_check(normal_articles)
    with open(f'{directory_name}\\good_articles.csv', 'w', encoding='utf-8') as f:
        for item in good_info:
            f.write(";".join([item.get('title'), str(item.get('percent')), item.get('url'), item.get('text')]) + '\n')

    url = insert_into_gsheet(target, good_info)
    print(f'Have done!\n{url}')
    system(f'start {url}')

    if open_extra_links:
        system('extra_links.txt')
        exit()


if __name__ == '__main__':
    get_info_from_gsheet()
    check = check_api()
    if isinstance(check, str):
        if check == '1':
            exit()
        elif check == '2':
            uniqueness_ch = False
    main()
