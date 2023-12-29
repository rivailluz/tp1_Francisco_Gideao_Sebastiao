class CategoriaProduto:
    def __init__(self, id_produto, id_categoria):
        self._id_produto = id_produto
        self._id_categoria = id_categoria

    @property
    def id_produto(self):
        return self._id_produto

    @property
    def id_categoria(self):
        return self._id_categoria

    def __str__(self):
        return f"CategoriaProduto(id_produto={self._id_produto}, id_categoria={self._id_categoria})"
