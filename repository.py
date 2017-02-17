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
            password='root',  # Мои настройки для БД
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
            Keyword(ID=1, Name='Путина', PesronID=1),
            Keyword(ID=2, Name='Путине', PesronID=1),
            Keyword(ID=3, Name='Путину', PesronID=1),
            Keyword(ID=4, Name='Медведев', PesronID=2)
        ]
        return [item.value for item in keywords if item.value['PersonID'] == personid]


class DbKeywordRepository(DbRepositoryConnect):
    """Класс репозитория Krywords работающий с БД"""

    def __init__(self):
        DbRepositoryConnect.__init__(self)

    def getkeywordbypersonid(self, personid):
        sql = "select * from `Keywords` where `Keywords`.`PersonID` = %s"
        self.cursor.execute(sql, (personid,))
        result = self.cursor.fetchall()
        return result


class DbPersonRepository(DbRepositoryConnect):
    """Класс репозитория Person работающий с БД"""

    def __init__(self):
        DbRepositoryConnect.__init__(self)

    def getpersons(self):
        sql = "select * from `Persons`"
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result


class SiteRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Site"""

    def __init__(self, repository):
        self.repository = repository

    def getapersons(self):
        result = [item for item in self.repository.getpersites()]
        return result

    def getsitestorank(self):
        result = [item for item in self.repository.getsitestorank()]
        return result


class DbSiteReposytory(DbRepositoryConnect):
    """Класс репозитория Site работающий с БД"""

    def __init__(self):
        DbRepositoryConnect.__init__(self)

    def getsites(self):
        sql = "select * from `Sites`"
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def getsitestorank(self):
        sql = 'select * from sites where id not in (select distinct siteid from pages)'
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result


class PersonRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Person"""

    def __init__(self, repository):
        self.repository = repository

    def getpersons(self):
        result = [item for item in self.repository.getpersons()]
        return result


class KeywordRepositoryWorker():
    """Класс реализует взаимодействие с репозиторием Keyword"""

    def __init__(self, repository):
        self.repository = repository

    def getbypersonid(self, personid):
        return [item for item in self.repository.getkeywordbypersonid(personid)]


class PagesRepositoryWorker:
    """Класс реализует взаимодействие с репозиторием Pages"""

    def __init__(self, repository):
        self.repository = repository

    def getallpages(self):
        result = [item for item in self.repository.getallpages()]
        return result

    def getpagesbysiteid(self, siteid):
        for item in self.repository.getpagesbysiteid(siteid):
            yield item

    def getsiteidfrompages(self):
        result = [item for item in self.repository.getsiteidfrompages()]
        return result

    def getpagelastscandatenull(self):
        result = [item for item in self.repository.getpagelastscandatenull()]
        return result

    def writepagestostore(self, page):
        self.repository.writepagestostore(page)

    def updatepageinstore(self, page):
        self.repository.updatepageinstore(page)


class DbPageRepository(DbRepositoryConnect):
    """Класс репозитория Page работающий с БД"""

    def __init__(self):
        DbRepositoryConnect.__init__(self)

    def getallpages(self):
        sql = "select * from `Pages`"
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def getpagesbysiteid(self, pageid):
        sql = "select `Url` from `Pages` where `Pages`.`SiteID` = %s"
        self.cursor.execute(sql, (pageid,))
        for row in self.cursor:
            yield row

    def getsiteidfrompages(self):
        sql = "select distinct `siteid` from `Pages`"
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def getpagelastscandatenull(self):
        sql = "select * from `Pages` where `Pages`.`LastScanDate` is null"
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def writepagestostore(self, page):
        sql = 'insert into `Pages` (Url, SiteID, FoundDateTime) values (%s, %s, %s)'
        param = (page.value['Url'], page.value['SiteID'], page.value['FoundDateTime'])
        try:
            self.cursor.execute(sql, param)
            self.conn.commit()
        except:
            print("Ошибка при записи в таблицу Pages")
            self.conn.rollback()

    def updatepageinstore(self, page):
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
        sql = 'insert into `personpagerank` (personid, pageid, rank) values (%s, %s, %s)'
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

    k = DbKeywordRepository()
    k1 = KeywordRepositoryWorker(k)
    k2 = k1.getbypersonid(1)
    print(k2)


if __name__ == '__main__':
    main()
