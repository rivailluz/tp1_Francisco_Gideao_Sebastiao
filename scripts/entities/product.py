class Produto:
    def __init__(self, product_id, ASIN=None, title=None, group=None, salesrank=None):
        self._id = product_id
        self._ASIN = ASIN
        self._title = title
        self._group = group
        self._salesrank = salesrank

    @property
    def id(self):
        return self._id

    @property
    def ASIN(self):
        return self._ASIN

    @ASIN.setter
    def ASIN(self, value):
        self._ASIN = value

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, value):
        self._title = value

    @property
    def group(self):
        return self._group

    @group.setter
    def group(self, value):
        self._group = value

    @property
    def salesrank(self):
        return self._salesrank

    @salesrank.setter
    def salesrank(self, value):
        self._salesrank = value

    def __str__(self):
        return (f"Produto(id={self._id}, ASIN={self._ASIN},"
                f"title={self._title}, group={self._group}, salesrank={self._salesrank})")
