# -*- coding: utf-8 -*-
"""
MLAIService — Modo tolerante à ausência de OPENAI_API_KEY

- Se OPENAI_API_KEY não estiver definida ou a lib `openai` não estiver disponível,
  o serviço entra em "modo simples" (self.enabled = False), NÃO faz chamadas externas
  e retorna resultados determinísticos básicos, evitando quebrar o sistema.

- Não cria, altera ou depende de nenhuma tabela. Apenas lógica de texto.
- Padrão Brasil mantido na linguagem; sem formatação monetária aqui.

Uso:
    from .ai_service import MLAIService
    ai = MLAIService()  # sempre seguro, com ou sem chave
"""

import os
from typing import Dict, Any, List

# Tenta importar a lib openai; se não existir, seguimos sem ela.
OPENAI_AVAILABLE = True
try:
    from openai import OpenAI  # pacote openai>=1.0
except Exception:
    OPENAI_AVAILABLE = False


class MLAIService:
    def __init__(self):
        """
        Inicializa o cliente apenas se houver chave e a lib openai estiver disponível.
        Caso contrário, entra em modo simples (self.enabled = False).
        """
        api_key = os.getenv("OPENAI_API_KEY") or ""
        self.enabled = bool(api_key) and OPENAI_AVAILABLE
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        if self.enabled:
            # Cliente oficial, somente se houver chave e lib.
            self.client = OpenAI(api_key=api_key)
        else:
            # Modo simples: não cria cliente, não chama API.
            self.client = None

    # ---------------------------
    # Métodos utilitários internos
    # ---------------------------
    @staticmethod
    def _normalize_text(txt: str) -> str:
        if not txt:
            return ""
        # limpeza leve, sem "achar" regras de negócio
        return " ".join(txt.split()).strip()

    # --------------------------------
    # Métodos públicos usados nas rotas
    # --------------------------------
    def otimizar_titulo(self, titulo: str, categoria: str = "") -> str:
        """
        Retorna um título levemente ajustado.
        - Com IA: faz uma chamada ao modelo configurado.
        - Sem IA: aplica normalizações simples (sem inventar nada).
        """
        titulo = self._normalize_text(titulo or "")
        categoria = self._normalize_text(categoria or "")

        if not titulo:
            return ""

        if not self.enabled:
            # Modo simples: corta espaços extras e capitaliza primeira letra
            base = titulo[:120]  # limite de segurança
            return base[:1].upper() + base[1:]

        # IA ligada
        prompt = (
            "Ajuste o título a seguir mantendo o sentido, sem inventar dados, "
            "com foco em claridade e corte de ruído. Responda APENAS com o título.\n\n"
            f"Título: {titulo}\n"
            f"Categoria (opcional): {categoria}\n"
        )
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=64,
            )
            out = resp.choices[0].message.content.strip()
            return self._normalize_text(out)[:120]
        except Exception:
            # fallback silencioso
            return titulo[:120]

    def gerar_descricao(self, dados: Dict[str, Any]) -> str:
        """
        Gera uma descrição objetiva a partir de chaves comuns. Sem suposições.
        """
        titulo = self._normalize_text(str(dados.get("titulo", "")))
        categoria = self._normalize_text(str(dados.get("categoria", "")))
        atributos = dados.get("atributos") or {}

        if not self.enabled:
            # Modo simples: montagem básica a partir do que foi fornecido.
            linhas: List[str] = []
            if titulo:
                linhas.append(f"Título: {titulo}")
            if categoria:
                linhas.append(f"Categoria: {categoria}")
            if isinstance(atributos, dict) and atributos:
                linhas.append("Atributos:")
                for k, v in atributos.items():
                    linhas.append(f"- {k}: {v}")
            return "\n".join(linhas).strip()

        # IA ligada
        partes = ["Monte uma descrição breve, sem inventar dados, a partir do JSON abaixo."]
        partes.append("Use frases curtas e objetivas. Sem preços. Sem promessas.")
        partes.append("JSON:")
        partes.append(str({k: v for k, v in dados.items()}))
        prompt = "\n".join(partes)

        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=300,
            )
            out = resp.choices[0].message.content.strip()
            return out
        except Exception:
            # fallback
            return self.gerar_descricao({"titulo": titulo, "categoria": categoria})

    def gerar_sugestoes(self, dados: Dict[str, Any]) -> Dict[str, Any]:
        """
        Retorna um pacote de sugestões (título/descricao/tags).
        """
        titulo = self._normalize_text(str(dados.get("titulo", "")))
        categoria = self._normalize_text(str(dados.get("categoria", "")))
        palavras = dados.get("palavras") or []

        if not self.enabled:
            # Modo simples: eco mínimo e cortes leves
            base_tags = []
            if isinstance(palavras, list):
                base_tags = [self._normalize_text(p) for p in palavras if p]
            base_tags = [t for t in base_tags if t][:8]  # limite
            return {
                "titulo": self.otimizar_titulo(titulo, categoria),
                "descricao": self.gerar_descricao({"titulo": titulo, "categoria": categoria}),
                "tags": base_tags,
            }

        prompt = (
            "Gere sugestões para anúncio a partir do JSON abaixo. "
            "Campos: titulo, descricao, tags (máx 12). "
            "NÃO invente dados.\n"
            f"JSON: {dados}\n"
        )
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=220,
            )
            out = resp.choices[0].message.content.strip()
            # Saída do modelo pode ser texto livre; como não podemos depender de parsing
            # perfeito, retornamos um pacote mínimo usando também os helpers locais:
            return {
                "titulo": self.otimizar_titulo(titulo, categoria),
                "descricao": self.gerar_descricao(dados),
                "tags": (palavras[:12] if isinstance(palavras, list) else []),
                "observacao_modelo": out[:300],
            }
        except Exception:
            return {
                "titulo": self.otimizar_titulo(titulo, categoria),
                "descricao": self.gerar_descricao(dados),
                "tags": (palavras[:12] if isinstance(palavras, list) else []),
            }

    def analisar_concorrencia(self, categoria: str = "", palavra_chave: str = "") -> Dict[str, Any]:
        """
        Placeholder de análise de concorrência.
        - Sem IA: devolve eco dos parâmetros.
        - Com IA: retorna um resumo textual simples (não opinativo).
        """
        categoria = self._normalize_text(categoria or "")
        palavra_chave = self._normalize_text(palavra_chave or "")

        if not self.enabled:
            return {
                "categoria": categoria,
                "palavra_chave": palavra_chave,
                "resumo": "Modo simples ativo (sem IA). Forneça OPENAI_API_KEY para análises ampliadas.",
            }

        prompt = (
            "Resuma, em um parágrafo objetivo (sem opiniões), o foco competitivo para "
            f"categoria='{categoria}' com palavra_chave='{palavra_chave}'. "
            "Não invente preços, marcas ou números."
        )
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=140,
            )
            out = resp.choices[0].message.content.strip()
            return {
                "categoria": categoria,
                "palavra_chave": palavra_chave,
                "resumo": out,
            }
        except Exception:
            return {
                "categoria": categoria,
                "palavra_chave": palavra_chave,
                "resumo": "Falha ao consultar IA. Tente novamente mais tarde.",
            }

    def otimizar_anuncio(self, dados: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pipeline simples: título otimizado + descrição gerada.
        - Não altera chaves desconhecidas.
        - Não inventa dados.
        """
        saida = dict(dados or {})
        saida["titulo"] = self.otimizar_titulo(str(dados.get("titulo", "")), str(dados.get("categoria", "")))
        saida["descricao"] = self.gerar_descricao(dados)
        return saida
