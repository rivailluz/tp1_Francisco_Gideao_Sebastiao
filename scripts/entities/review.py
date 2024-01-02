class Review:
    def __init__(self, id_product, date=None, customer_id=None, rating=None, votes=None, helpful=None):
        self._id_product = id_product
        self._date = date
        self._customer_id = customer_id
        self._rating = rating
        self._votes = votes
        self._helpful = helpful

    @property
    def id_product(self):
        return self._id_product

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, value):
        self._date = value

    @property
    def customer_id(self):
        return self._customer_id

    @customer_id.setter
    def customer_id(self, value):
        self._customer_id = value

    @property
    def rating(self):
        return self._rating

    @rating.setter
    def rating(self, value):
        self._rating = value

    @property
    def votes(self):
        return self._votes

    @votes.setter
    def votes(self, value):
        self._votes = value

    @property
    def helpful(self):
        return self._helpful

    @helpful.setter
    def helpful(self, value):
        self._helpful = value

    def __str__(self):
        return f"Review(id_product={self._id_product}, date={self._date}, customer_id={self._customer_id}, rating={self._rating}, votes={self._votes}, helpful={self._helpful})"
