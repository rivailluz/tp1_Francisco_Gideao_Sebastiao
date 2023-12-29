class Categoria:
    def __init__(self, id_categoria, nome=None, ancester_id=None):
        self._id_categoria = id_categoria
        self._nome = nome
        self._ancester_id = ancester_id

    @property
    def id_categoria(self):
        return self._id_categoria

    @property
    def nome(self):
        return self._nome

    @nome.setter
    def nome(self, value):
        self._nome = value

    @property
    def ancester_id(self):
        return self._ancester_id

    @ancester_id.setter
    def ancester_id(self, value):
        self._ancester_id = value

    def __eq__(self, other):
        if isinstance(other, Categoria):
            return (self._id_categoria, self._ancester_id) == (other._id_categoria, other._ancester_id)
        return False

    def __hash__(self):
        return hash(self._id_categoria)

    def __str__(self):
        return f"Categoria(id_categoria={self._id_categoria}, nome={self._nome}, ancester_id={self._ancester_id})"
