import requests
import bs4
import fake_useragent
from config import username, password, root_page, db_path
import sqlite3

if __name__ == '__main__':
    conn = sqlite3.connect(db_path)  # или :memory: чтобы сохранить в RAM
    cursor = conn.cursor()
    session = requests.Session()
    link = '{}/?act=auth'.format(root_page)
    user = fake_useragent.UserAgent().random
    data = {
        'filter_user': username,
        'filter_pass': password,
        'entrance_user': '%D0%92%D0%BE%D0%B9%D1%82%D0%B8'
    }
    headers = {
        'User-Agent': user
    }

    response_auth = session.post(link, data=data, headers=headers, allow_redirects=False)
    response_after_redirect = session.get('{}/?act=hpage'.format(root_page))
    serials = {
        'to_page_link': 'serials/',
        'max_page_link': '/serials',
        'sql': 'serial'

    }
    films = {
        'to_page_link': 'posts/category/1.html',
        'max_page_link': '/posts/category/1',
        'sql': 'film'

    }

    for content in (serials, films):
        to_page = session.get('{}/{}'.format(root_page, content['to_page_link']))
        max_page = to_page.text.split('</a><a href="{}/thisPage/2.html">»</a> </div>'
                                      .format(content['max_page_link']))[0].split('>')[-1]
        date = to_page.text.split('<span class="text-blue">')[1].split('<')[0]
        soup = bs4.BeautifulSoup(to_page.text, 'lxml')
        tr_list = soup.select('td')
        if content['sql'] == 'serial':
            date_name = (str(tr_list[0]).split('</a>')[0]).split('>')[-1]
        if content['sql'] == 'film':
            date_name = ((str(tr_list[0]).split('title="')[1]).split('>')[1]).split(' <')[0]

        cursor.execute('''select * from last_loaded_{}'''.format(content['sql']))
        try:
            last_result = cursor.fetchall()[-1]
        except IndexError:
            print('IndexError')
        err_pages = []
        done = 0
        for page in range(1, int(max_page) + 1):
            # for page in range(5, 7):
            # print(range(int(max_page)).index(page) / int(max_page))
            try:
                response_videos = session.get('{}{}/thisPage/{}.html'.format(root_page, content['max_page_link'], page))
            except ConnectionError:
                print('ConnectionError', page, sep=': ')
                err_pages.append(page)
                cursor.execute("INSERT INTO err_pages VALUES (?)", (page,))
                conn.commit()
            else:
                if response_videos.status_code != 200:
                    err_pages.append(page)
                    cursor.execute("INSERT INTO err_pages VALUES (?)", (page,))
                    conn.commit()
                    continue
                soup = bs4.BeautifulSoup(response_videos.text, 'lxml')
                tr_list = soup.select('td')
                names, links, kp_links, kp_rate, imdb_links, imdb_rate, dates = [], [], [], [], [], [], []
                for i in tr_list:
                    if 'span2' in str(i):
                        dates.append(str(i).split('<span class="text-blue">')[1].split('<')[0])
                    if '<td class="">' in str(i) or '<td class>' in str(i):
                        if content['sql'] == 'film':
                            names.append(((str(i).split('title="')[1]).split('>')[1]).split(' <')[0])
                        if content['sql'] == 'serial':
                            names.append(((str(i).split('</a>')[0]).split('>')[-1]))
                    if 'data-buffer' in str(i):
                        links.append((str(i).split('data-buffer="')[1]).split('"')[0])
                    if '"span3 rating"' in str(i):
                        if 'kinopoisk' in str(i):
                            kp_link = (str(i).split('"span3 rating"><a href="')[1]).split('"')[0]
                            if kp_link == 'N/A':
                                cursor.execute("select kp_links from {0}s_{0}s where kp_links like 'N*'".format(content['sql']))
                                n_a = cursor.fetchall()
                                try:
                                    kp_link = "N/A {}".format(len(n_a)+1)
                                except:
                                    kp_link = "N/A 1"
                            kp_links.append(kp_link)
                            try:
                                rate = (str(i).split('"Рейтинг Kinopoisk: ')[1]).split('"')[0]
                            except:
                                rate = 'N/A'
                            if rate == '':
                                rate = 'N/A'
                            kp_rate.append(rate)

                        else:
                            kp_links.append('N/A')
                            kp_rate.append('N/A')

                        if 'imdb.com' in str(i):
                            imdb_links.append('https://www.imdb.com/'
                                              + (str(i).split('<a href="https://www.imdb.com/')[1]).split('"')[0])
                            try:
                                rate = (str(i).split('"Рейтинг IMDB: ')[1]).split('"')[0]
                            except:
                                rate = 'N/A'
                            if rate == '':
                                rate = 'N/A'
                            imdb_rate.append(rate)
                        else:
                            imdb_links.append('N/A')
                            imdb_rate.append('N/A')
                print(names, links, kp_links, kp_rate, imdb_links, imdb_rate)
                strings_count = range(len(names))
                try:
                    if last_result[1] in dates:
                        print(last_result[1], dates)
                        last_index = dates.index(last_result[1])
                        print(last_index, len(dates), len(names))
                        print(names[last_index], last_result)
                        if names[last_index] == last_result[0]:
                            done = 1
                            strings_count = range(last_index)
                except IndexError:
                    print('IndexError')
                except NameError:
                    print('NameError')
                films = [
                    (names[i], links[i],  kp_rate[i], kp_links[i], imdb_rate[i], imdb_links[i]) for i in strings_count
                ]
                cursor.executemany("INSERT INTO {0}s_{0}s "
                                   "(names, links, kp_rate, kp_links, imdb_rate, imdb_links )"
                                   "VALUES (?,?,?,?,?,?)"
                                   .format(content['sql']), films)
                conn.commit()
                if done == 1:
                    break

        cursor.execute('''insert into last_loaded_{}
                                        (name, date)
                                        values
                                        (?, ?);'''.format(content['sql']), (date_name, date))
        conn.commit()

    conn.close()
