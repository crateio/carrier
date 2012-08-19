import posixpath
import re
import urlparse


_url = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ...or ipv4
        r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ...or ipv6
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)


class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:

        (1) items added
        (2) items removed
        (3) keys same in both but changed values
        (4) keys same in both and unchanged values
    """

    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)

    def added(self):
        return self.set_current - self.intersect

    def removed(self):
        return self.set_past - self.intersect

    def changed(self):
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])

    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])


def splitext(path):
    """Like os.path.splitext, but take off .tar too"""
    base, ext = posixpath.splitext(path)
    if base.lower().endswith('.tar'):
        ext = base[-4:] + ext
        base = base[:-4]
    return base, ext


def clean_url(url):
    parts = list(urlparse.urlsplit(url))

    if not parts[0]:
        # If no URL scheme given, assume http://
        parts[0] = "http"

    if not parts[1]:
        # Assume that if no domain is provided, that the path segment
        # contains the domain.
        parts[1] = parts[2]
        parts[2] = ""
        # Rebuild the url_fields list, since the domain segment may now
        # contain the path too.
        parts = list(urlparse.urlsplit(urlparse.urlunsplit(parts)))

    if not parts[2]:
        # the path portion may need to be added before query params
        parts[2] = "/"

    cleaned_url = urlparse.urlunsplit(parts)

    if not _url.search(cleaned_url):
        # Trivial Case Failed. Try for possible IDN domain
        if cleaned_url:
            scheme, netloc, path, query, fragment = urlparse.urlsplit(cleaned_url)

            try:
                netloc = netloc.encode("idna").decode("ascii")  # IDN -> ACE
            except UnicodeError:  # invalid domain part
                raise ValueError

            cleaned_url = urlparse.urlunsplit((scheme, netloc, path, query, fragment))

            if not _url.search(cleaned_url):
                raise ValueError
        else:
            raise ValueError

    return cleaned_url
