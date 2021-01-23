import requests
import bs4
import fake_useragent
from config import username, password
import sqlite3

if __name__ == '__main__':
    conn = sqlite3.connect("db.db")  # или :memory: чтобы сохранить в RAM
    cursor = conn.cursor()
    '''cursor.execute("""CREATE TABLE Serials
                      (names, links, kp_links, kp_rate, imdb_links, imdb_rate)
                   """)
    cursor.execute("""CREATE TABLE err_pages_ser
                          (page)
                       """)'''

    session = requests.Session()
    link = 'http://ustore.bz/?act=auth'
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
    response_after_redirect = session.get('http://ustore.bz/?act=hpage')
    to_page = session.get('http://ustore.bz/serials/')
    max_page = to_page.text.split('</a><a href="/serials/thisPage/2.html">»</a> </div>')[0].split('>')[-1]
    date = to_page.text.split('<span class="text-blue">')[1].split('<')[0]
    soup = bs4.BeautifulSoup(to_page.text, 'lxml')
    tr_list = soup.select('td')
    date_name = (str(tr_list[0]).split('</a>')[0]).split('>')[-1]
    cursor.execute("""CREATE TABLE last_loaded_ser
                                      (name, date)
                                   """)
    cursor.execute('''insert into last_loaded_ser
                                    (name, date)
                                    values
                                    (?, ?);''', (date_name, date))
    conn.commit()
    err_pages = []
    for page in range(int(max_page)):
        print(range(int(max_page)).index(page)/int(max_page))
        try:
            response_videos = session.get('http://ustore.bz/serials/thisPage/{}.html'.format(page))
        except ConnectionError:
            print('ConnectionError', page, sep=': ')
            err_pages.append(page)
            cursor.execute("INSERT INTO err_pages_ser VALUES (?)", (page,))
            conn.commit()
        else:
            if response_videos.status_code != 200:
                err_pages.append(page)
                cursor.execute("INSERT INTO err_pages_ser VALUES (?)", (page,))
                conn.commit()
                continue
            names, links, kp_links, kp_rate, imdb_links, imdb_rate = [], [], [], [], [], []
            soup = bs4.BeautifulSoup(response_videos.text, 'lxml')
            tr_list = soup.select('td')
            for i in tr_list:
                if '<td class="">' in str(i):
                    names.append(((str(i).split('</a>')[0]).split('>')[-1]))
                if 'data-buffer' in str(i):
                    links.append((str(i).split('data-buffer="')[1]).split('"')[0])
                if '"span3 rating"' in str(i):
                    if 'Рейтинг Kinopoisk:' in str(i):
                        kp_links.append((str(i).split('"span3 rating"><a href="')[1]).split('"')[0])
                        rate = (str(i).split('"Рейтинг Kinopoisk: ')[1]).split('"')[0]
                        if rate == '':
                            rate = 'N/A'
                        kp_rate.append(rate)

                    else:
                        kp_links.append('N/A')
                        kp_rate.append('N/A')

                    if 'Рейтинг IMDB:' in str(i):
                        imdb_links.append('https://www.imdb.com/'
                                          + (str(i).split('<a href="https://www.imdb.com/')[1]).split('"')[0])
                        rate = (str(i).split('"Рейтинг IMDB: ')[1]).split('"')[0]
                        if rate == '':
                            rate = 'N/A'
                        imdb_rate.append(rate)
                    else:
                        imdb_links.append('N/A')
                        imdb_rate.append('N/A')
            films = [
                (names[i], links[i], kp_links[i], kp_rate[i], imdb_links[i], imdb_rate[i]) for i in range(len(names))
            ]
            cursor.executemany("INSERT INTO serials_serials VALUES (?,?,?,?,?,?)", films)
            conn.commit()
            # break

    print(len(err_pages))
