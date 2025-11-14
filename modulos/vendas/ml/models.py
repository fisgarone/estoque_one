import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional

class AnuncioML:
    def __init__(self, db_path: str = 'fisgarone.db'):
        self.db_path = db_path
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def get_all(self, limit: int = 100, offset: int = 0, filters: Dict = None) -> List[Dict]:
        """Busca todos os anúncios com filtros opcionais"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = "SELECT * FROM anuncios_ml"
        params = []
        
        if filters:
            conditions = []
            if filters.get('status'):
                conditions.append("status = ?")
                params.append(filters['status'])
            if filters.get('categoria'):
                conditions.append("id_categoria LIKE ?")
                params.append(f"%{filters['categoria']}%")
            if filters.get('preco_min'):
                conditions.append("preco >= ?")
                params.append(filters['preco_min'])
            if filters.get('preco_max'):
                conditions.append("preco <= ?")
                params.append(filters['preco_max'])
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY atualizado_em DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    def get_by_id(self, id_anuncio: str) -> Optional[Dict]:
        """Busca anúncio por ID"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM anuncios_ml WHERE id_anuncio = ?", (id_anuncio,))
        row = cursor.fetchone()
        
        if row:
            columns = [description[0] for description in cursor.description]
            result = dict(zip(columns, row))
        else:
            result = None
        
        conn.close()
        return result
    
    def get_dashboard_stats(self) -> Dict:
        """Estatísticas para dashboard"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Total de anúncios
        cursor.execute("SELECT COUNT(*) FROM anuncios_ml")
        total_anuncios = cursor.fetchone()[0]
        
        # Anúncios ativos
        cursor.execute("SELECT COUNT(*) FROM anuncios_ml WHERE status = 'active'")
        ativos = cursor.fetchone()[0]
        
        # Total vendido
        cursor.execute("SELECT SUM(quantidade_vendida) FROM anuncios_ml")
        total_vendido = cursor.fetchone()[0] or 0
        
        # Receita total
        cursor.execute("SELECT SUM(preco * quantidade_vendida) FROM anuncios_ml")
        receita_total = cursor.fetchone()[0] or 0
        
        # Top categorias
        cursor.execute("""
            SELECT id_categoria, COUNT(*) as count 
            FROM anuncios_ml 
            GROUP BY id_categoria 
            ORDER BY count DESC 
            LIMIT 5
        """)
        top_categorias = cursor.fetchall()
        
        # Status distribution
        cursor.execute("""
            SELECT status, COUNT(*) as count 
            FROM anuncios_ml 
            GROUP BY status
        """)
        status_dist = cursor.fetchall()
        
        conn.close()
        
        return {
            'total_anuncios': total_anuncios,
            'anuncios_ativos': ativos,
            'total_vendido': total_vendido,
            'receita_total': receita_total,
            'top_categorias': top_categorias,
            'status_distribution': status_dist
        }
    
    def create(self, data: Dict) -> bool:
        """Cria novo anúncio"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            data['atualizado_em'] = datetime.now().isoformat()
            
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?' for _ in data])
            
            cursor.execute(
                f"INSERT INTO anuncios_ml ({columns}) VALUES ({placeholders})",
                list(data.values())
            )
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Erro ao criar anúncio: {e}")
            return False
        finally:
            conn.close()
    
    def update(self, id_anuncio: str, data: Dict) -> bool:
        """Atualiza anúncio existente"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            data['atualizado_em'] = datetime.now().isoformat()
            
            set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
            values = list(data.values()) + [id_anuncio]
            
            cursor.execute(
                f"UPDATE anuncios_ml SET {set_clause} WHERE id_anuncio = ?",
                values
            )
            
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Erro ao atualizar anúncio: {e}")
            return False
        finally:
            conn.close()
    
    def delete(self, id_anuncio: str) -> bool:
        """Remove anúncio"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("DELETE FROM anuncios_ml WHERE id_anuncio = ?", (id_anuncio,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Erro ao deletar anúncio: {e}")
            return False
        finally:
            conn.close()
    
    def clone(self, id_anuncio: str, new_data: Dict = None) -> Optional[str]:
        """Clona anúncio existente"""
        original = self.get_by_id(id_anuncio)
        if not original:
            return None
        
        # Remove campos únicos
        clone_data = original.copy()
        clone_data.pop('id_anuncio', None)
        clone_data['titulo'] = f"CÓPIA - {clone_data.get('titulo', '')}"
        clone_data['status'] = 'paused'
        
        if new_data:
            clone_data.update(new_data)
        
        # Gera novo ID
        import uuid
        new_id = f"MLB{uuid.uuid4().hex[:10].upper()}"
        clone_data['id_anuncio'] = new_id
        
        if self.create(clone_data):
            return new_id
        return None
