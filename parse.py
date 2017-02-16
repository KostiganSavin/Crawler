import urllib.parse
import urllib.robotparser
import re


def whatisurl(url, headers):
    """
    :param headers:
    :param url: Ссылка для анализа, куда ведет
    :return: 'robots' или 'sitemap' в зависимости от того на что указывает ссылка.
    """
    print('Смотрим страницу', url)
    parse = urllib.parse.urlsplit(url)
    h = headers['Content-Type'].split(';')[0]
    if re.search(r'robots.txt', parse.path) and h == 'text/plain':
        print('h ->', h)
        return 'robots'
    elif re.search(r'sitemap', parse.path) and (re.search(r'xml', h) or re.search(r'octet-stream', h)):
        print('h ->', h)
        return 'sitemap'


def readrobots(file):
    """
    :param file: Файл robots.txt для аналза и извечения ссылки на  sitemap
    :return: Возвращает ссылку на sitemap
    """
    result = {}
    r = file.split('\n')
    for x in r:
        if x.startswith('Sitemap'):
            result['sitemap'] = x.split(':', maxsplit=1)[-1].strip()
        elif x.startswith('Host'):
            result['root'] = x.split(':', maxsplit=1)[-1].strip()
    if result.get('sitemap'):
        return result['sitemap']
    elif result.get('root'):
        return result['root']


def sitemapparse(soup):
    """
    :param soup: HTML страница sitemap для извлечения ссылок для дальнейшего обхода.
    :return: Список ссылок для записи в БД по которым необходимо совершать обход
    """
    for item in soup:
        yield item.text


def countstat(tree, word):
    """
    :param tree: Страница для подсчета статистики.
    :param word: Слово по которому подсчитываем статистику
    :return: Количество раз упоминания слован на странице
    """
    iterator = tree.itertext()
    c = r'\b{}\b'.format(word)
    w = re.compile(c)
    i = 0
    for string in iterator:
        if len(w.findall(repr(string))) > 0:
            i += len(w.findall(repr(string)))
    print('Rank ->', i)
    return i


def geturlfrompage(url, tree):
    """
    Извлекает ссылки со страницы в соответстви с правилами в robots.txt
    :param url:
    :param tree:
    :return:
    """
    tree.make_links_absolute(url, resolve_base_href=True)

    links = tree.iterlinks()
    links_set = set([item[2] for item in links if item[0].tag == 'a'])

    p = urllib.parse.urlparse(url)  # Парсим полученную ссылку
    r = urllib.robotparser.RobotFileParser()  # Парсинг robots.txt
    rurl = urllib.parse.urlunparse((p.scheme, p.netloc, 'robots.txt', '', '', ''))
    r.set_url(rurl)
    r.read()  # Читаем и парсим robots.txt
    hrefs = set()
    for link in links_set:
        u = urllib.parse.urlparse(link)
        u1 = urllib.parse.urlunparse((u.scheme, u.netloc, u.path, '', '', ''))
        if p.netloc == u.netloc:
            print('U1 ->', u1)
            if r.can_fetch("*", u1):
                hrefs.add(u1)
    return hrefs


def main():
    pass


if __name__ == '__main__':
    main()
