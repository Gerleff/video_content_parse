import sqlite3

if __name__ == '__main__':
    conn = sqlite3.connect("db.db")  # или :memory: чтобы сохранить в RAM
    cursor = conn.cursor()
    for table_name in ('serials_serials', 'films_films'):
        """sql = "SELECT * FROM {}".format(table_name)
        cursor.execute(sql)
        table = cursor.fetchall()
        kplinks = [i[2] for i in table]
        for i in range(len(table)):
            table[i] = table[i][1:]
        table.reverse()
        for i in table:
            print(table.index(i) / len(table) * 100, '%')
            if kplinks.count(i[2]) > 1 and i[2] != 'N/A':
                table.remove(i)
                kplinks.remove(kplinks[kplinks.index(i[2])])
        table.reverse()
        cursor.execute('delete from {}'.format(table_name))
        cursor.execute('reindex {}'.format(table_name))
        # cursor.execute('vacuum')
        for i in table:
            cursor.execute('''insert into {} 
                            (names, links, kp_links, kp_rate, imdb_links, imdb_rate)
                            values
                            (?, ?, ?, ?, ?, ?);'''.format(table_name), i)
        conn.commit()
        print(len(table))"""
        sql = ('drop table if exists `t_temp`',

               """
            CREATE TEMPORARY TABLE `t_temp` 
            as  
            SELECT min(id) as id
            FROM {0}
            GROUP BY kp_links
            ;""".format(table_name),

               """
            DELETE from {0}
            WHERE {0}.id not in (
            SELECT id FROM t_temp
            );
        """.format(table_name))

        for command in sql:
            cursor.execute(command)
        conn.commit()

        cursor.execute('select * from {}'.format(table_name))
        table = cursor.fetchall()
        print(len(table))

    conn.close()
