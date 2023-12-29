class ProdutosSimilares:
    def __init__(self, id_product, id_products_similares=None):
        self._id_product = id_product
        self._id_products_similares = id_products_similares if id_products_similares is not None else []

    @property
    def id_product(self):
        return self._id_product

    @property
    def id_products_similares(self):
        return self._id_products_similares

    def adicionar_produto_similar(self, id_produto_similar):
        self._id_products_similares.append(id_produto_similar)

    def remover_produto_similar(self, id_produto_similar):
        if id_produto_similar in self._id_products_similares:
            self._id_products_similares.remove(id_produto_similar)

    def __str__(self):
        return (f"ProdutosSimilares(id_product={self._id_product}, "
                f"id_products_similares={self._id_products_similares})")

