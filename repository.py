# -*- coding: utf-8 -*-

import pymysql

# import sqlite3
# import datetime

'''
conn = pymysql.connect(
    host='localhost',
    user='root',  # Мои настройки для БД
    password='root',  # Мои настройки для БД
    db='ratepersons',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)

cursor = conn.cursor()
'''


class DbRepositoryConnect:
    def __init__(self):
        self.conn = pymysql.connect(
            host='localhost',
            user='root',  # Мои настройки для БД
            password='`1qazxsw2',  # Мои настройки для БД
            db='ratepersons',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.conn.cursor()


class RepositoryObject:
    """Класс для создания метода представления сущностей"""
    name = ''
    objects = {}

    def __init__(self, name):
        self.name = name
        RepositoryObject.objects[self.name] = self

    @property
    def value(self):
        return self.__dict__


class Person(RepositoryObject):
    """Класс описыввает сущность Person"""
    name = 'Person'

    def __init__(self, **kwargs):
        super(RepositoryObject, self).__init__()
        for name, value in kwargs.items():
            setattr(self, name, value)


class Keyword(RepositoryObject):
    """Класс описыввает сущность Keyword"""
    name = 'Keyword'

    def __init__(self, **kwargs):
        super(RepositoryObject, self).__init__()
        for name, value in kwargs.items():
            setattr(self, name, value)


class Site(RepositoryObject):
    """Класс описыввает сущность Site"""
    name = 'Site'

    def __init__(self, **kwargs):
        super(RepositoryObject, self).__init__()
        for name, value in kwargs.items():
            setattr(self, name, value)


class Page(RepositoryObject):
    """Класс описыввает сущность Page"""
    name = 'Page'

    def __init__(self, **kwargs):
        super(RepositoryObject, self).__init__()
        for name, value in kwargs.items():
            setattr(self, name, value)


class PersonPageRank(RepositoryObject):
    """Класс описыввает сущность PersonPageRank"""
    name = 'PersonPageRank'

    def __init__(self, **kwargs):
        super(RepositoryObject, self).__init__()
        for name, value in kwargs.items():
            setattr(self, name, value)


class FakeKeywordRepository:
    """Класс фейкогового репозитория Krywords"""

    def __init__(self):
        pass

    def getkeywordbypersonid(self, personid):
        keywords = [
            Keyword(ID=1, Name='Путина', PersonID=1),
            Keyword(ID=2, Name='Путине', PersonID=1),
            Keyword(ID=3, Name='Путину', PersonID=1),
            Keyword(ID=4, Name='Медведев', PersonID=2)
        ]
        for item in keywords:
            if item.value['PersonID'] == personid:
                yield item.value


class DbKeywordRepository(DbRepositoryConnect):
    """Класс репозитория Krywords работающий с БД"""

    def __init__(self):
        DbRepositoryConnect.__init__(self)

    def getkeywordbypersonid(self, personid):
        """Получает записи из БД всех ключевых слов для заданной персоны PersonID"""
        sql = "select * from `Keywords` where `Keywords`.`PersonID` = %s"
        self.cursor.execute(sql, (personid, ))      
        for row in self.cursor:
            yield row


class DbPersonRepository(DbRepositoryConnect):
    """Класс репозитория Person работающий с БД"""

    def __init__(self):
        DbRepositoryConnect.__init__(self)

    def getpersons(self):
        """Получает записи из БД всех станиц из таблицы Persons"""
        sql = "select * from `Persons`"
        self.cursor.execute(sql)
        for row in self.cursor:
            yield row


class SiteRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Site"""

    def __init__(self, repository):
        self.repository = repository

    def getsites(self):
        """Получает из БД записи всех станиц из таблицы Sites"""
        for item in self.repository.getpersites():
            yield item

    def getsitestorank(self):
        """Получает из БД записи станиц из таблицы Sites которые отсутствуют в таблице Pages"""
        for item in self.repository.getsitestorank():
            yield item


class DbSiteReposytory(DbRepositoryConnect):
    """Класс репозитория Site работающий с БД"""

    def __init__(self):
        DbRepositoryConnect.__init__(self)

    def getsites(self):
        """Получает записи из БД всех станиц из таблицы Sites"""
        sql = "select * from `Sites`"
        self.cursor.execute(sql)
        for row in self.cursor:
            yield row

    def getsitestorank(self):
        """Получает из БД записи станиц из таблицы Sites которые отсутствуют в таблице Pages"""
        sql = 'select * from `Sites` where ID not in (select distinct `SiteID` from `Pages`)'
        self.cursor.execute(sql)
        for row in self.cursor:
            yield row


class PersonRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Person"""

    def __init__(self, repository):
        self.repository = repository

    def getpersons(self):
        """Получает записи всех станиц из таблицы Persons"""
        for item in self.repository.getpersons():
            yield item


class KeywordRepositoryWorker():
    """Класс реализует взаимодействие с репозиторием Keyword"""

    def __init__(self, repository):
        self.repository = repository

    def getbypersonid(self, personid):
        """Получает записи ключевых слов из таблицы Keywords по заданному PersonID"""
        for item in self.repository.getkeywordbypersonid(personid):
            yield item


class PagesRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Pages"""

    def __init__(self, repository):
        self.repository = repository

    def getallpages(self):
        """Получает записи всех станиц из таблицы Pages"""
        for item in self.repository.getallpages():
            yield item

    def getpagesbysiteid(self, siteid):
        """Получает записи станиц из таблицы Pages по заданному SiteID"""
        for item in self.repository.getpagesbysiteid(siteid):
            yield item

    def getsiteidfrompages(self):
        """Получает записи всех уникальных SiteID из таблицы Pages"""
        for item in self.repository.getsiteidfrompages():
            yield item

    def getpagelastscandatenull(self):
        """Получает записи станиц из таблицы Pages которые не разу не сканировались LastScanDate is null"""
        for item in self.repository.getpagelastscandatenull():
            yield item

    def writepagestostore(self, page):
        """Передает данные для записи данных в Pages"""
        self.repository.writepagestostore(page)

    def updatepageinstore(self, page):
        """Передает данные для обновления данных в Pages"""
        self.repository.updatepageinstore(page)


class DbPageRepository(DbRepositoryConnect):
    """Класс репозитория Page работающий с БД"""

    def __init__(self):
        DbRepositoryConnect.__init__(self)

    def getallpages(self):
        """Получает из БД все записи из таблицы Pages"""
        sql = "select * from `Pages`"
        self.cursor.execute(sql)
        for row in self.cursor:
            yield row

    def getpagesbysiteid(self, pageid):
        """Получает из БД записи из таблицы Pages по определенному SiteID"""
        sql = "select `Url` from `Pages` where `Pages`.`SiteID` = %s"
        self.cursor.execute(sql, (pageid,))
        for row in self.cursor:
            yield row

    def getsiteidfrompages(self):
        """Получает из БД список записей с уникальным SiteID из таблицы Pages"""
        sql = "select distinct `Siteid` from `Pages`"
        self.cursor.execute(sql)
        for row in self.cursor:
            yield row

    def getpagelastscandatenull(self):
        """Получает из БД список записей из таблицы Pages где """
        sql = "select * from `Pages` where `Pages`.`LastScanDate` is null"
        self.cursor.execute(sql)
        for row in self.cursor:
            yield row

    def writepagestostore(self, page):
        """Записывает в БД в таблицу Pages данные по найденной ссылке на страницу"""
        sql = 'insert into `Pages` (Url, SiteID, FoundDateTime) values (%s, %s, %s)'
        param = (page.value['Url'], page.value['SiteID'], page.value['FoundDateTime'])
        try:
            self.cursor.execute(sql, param)
            self.conn.commit()
        except:
            print("Ошибка при записи в таблицу Pages")
            self.conn.rollback()

    def updatepageinstore(self, page):
        """Обновляет запись в БД по заданной Pages после анализа таблицы и задает время сканирования страницы"""
        sql = 'update `Pages` set `LastScanDate`=%s where `Pages`.`ID` = %s'
        param = (page.value['LastScanDate'], page.value['ID'])
        try:
            self.cursor.execute(sql, param)
            self.conn.commit()
        except:
            print("Ошибка при обновлении записи о Pages")
            self.conn.rollback()


class DbPersonPageRankRepository(DbRepositoryConnect):
    """Класс репозитория PersonPageRank работающий с БД"""

    def __init__(self):
        DbRepositoryConnect.__init__(self)

    def writeranktostore(self, personpagerank):
        """Записывает в БД статистику Rank упомнинания персоны PersonID на проаналлизированной странице PagesID"""
        sql = 'insert into `PersonPageRank` (PersonID, PageID, Rank) values (%s, %s, %s)'
        param = (personpagerank.value['PersonID'], personpagerank.value['PageID'], personpagerank.value['Rank'])
        try:
            self.cursor.execute(sql, param)
            self.conn.commit()
        except:
            print("Ошибка при записи в таблицу PersonPageRank")
            self.conn.rollback()

    def updaterankinstore(self, personpagerank):
        pass
        '''
        sql = 'update `Pages` set `LastScanDate`=%s where `Pages`.`ID` = %s'
        param = (page.value['LastScanDate'], page.value['ID'])
        cursor.execute(sql, (param))
        conn.commit()
        '''


class PersonPageRankRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием PersonPageRankRepository"""

    def __init__(self, repository):
        self.repository = repository

    def writeranktostore(self, personpagerank):
        """Передает данные для записи статитстики в PersonPageRank"""
        self.repository.writeranktostore(personpagerank)


def main():
    
    db = DbRepositoryConnect()

    repository_dict = {
        'keyword': DbKeywordRepository(),
        'sites': DbSiteReposytory(),
        'pages': DbPageRepository(),
        'person': DbPersonRepository(),
        'personpagerank': DbPersonPageRankRepository()
    }

    repository_worker_dict = {
        'keyword': KeywordRepositoryWorker(repository_dict['keyword']),
        'sites': SiteRepositoryWorker(repository_dict['sites']),
        'pages': PagesRepositoryWorker(repository_dict['pages']),
        'person': PersonRepositoryWorker(repository_dict['person']),
        'personpagerank': PersonPageRankRepositoryWorker(repository_dict['personpagerank'])
    }

    k = FakeKeywordRepository()
    k1 = KeywordRepositoryWorker(k)
    k2 = [x for x in k1.getbypersonid(1)]
    print(k2)


if __name__ == '__main__':
    main()
