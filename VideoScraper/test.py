from urllib import request
from urllib.parse import urlencode
from html.parser import HTMLParser


class VimeoLinkParser(HTMLParser):
    def __init__(self, max_results=5):
        super().__init__()
        self.max_results = max_results
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag != "a" or len(self.links) >= self.max_results:
            return

        attrs = dict(attrs)
        href = attrs.get("href", "")
        class_attr = attrs.get("class", "")

        if "result__a" in class_attr and "vimeo.com" in href:
            self.links.append(href)


def search(baseurl, query, max_results=5):
    params = urlencode({'q': query}).encode()

    print(f"Searching videos for the query: {query}")

    req = request.Request(
        url=baseurl,
        data=params,
        headers={
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/35.0.1916.47 Safari/537.36'
            )
        }
    )

    with request.urlopen(req, timeout=10) as res:
        html = res.read().decode("utf-8")
        print(html)

    parser = VimeoLinkParser(max_results)
    parser.feed(html)

    return parser.links


if __name__ == "__main__":
    links = search(
        "https://html.duckduckgo.com/html/",
        "site:vimeo.com accepting children birthday",
        5
    )

    for link in links:
        print(link)
