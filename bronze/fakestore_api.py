import requests
from typing import List, Dict, Optional


class FakeStoreAPI:
    """
    API ESCOLHIDA PARA TESTES DO PROJETO
    Documentação: https://fakestoreapi.com/docs
    """
    
    BASE_URL = "https://fakestoreapi.com"
    
    def __init__(self):
        """Inicializa a sessão HTTP."""
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json"
        })
    
    def standard_request(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        CRIA UMA  REQUISIÇÃO PADRÃO PARA TODOS OS ENDPOINTS DA API EM QUESTÃO.
        """
   
        url = f"{self.BASE_URL}/{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    
    def get_products(self, limit: Optional[int] = None, sort: Optional[str] = None) -> List[Dict]:
        """
        Endpoint: GET /products
                   
        Returns:
            Lista de produtos
        """
        params = {}
        if limit:
            params['limit'] = limit
        if sort:
            params['sort'] = sort
        
        return self.standard_request("products", params=params if params else None)
    
    def get_categories(self) -> List[str]:
        """
        Endpoint: GET /products/categories
        
        Returns:
            Lista de categorias disponíveis
        """
        return self.standard_request("products/categories")
    
    def get_carts(self, limit: Optional[int] = None, sort: Optional[str] = None) -> List[Dict]:
        """
        Endpoint: GET /carts
        
        Returns:
            Lista de carrinhos de compras
        """
        params = {}
        if limit:
            params['limit'] = limit
        if sort:
            params['sort'] = sort
        
        return self.standard_request("carts", params=params if params else None)
