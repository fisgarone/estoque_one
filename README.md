# Projeto Fisgarone - Sistema de Gestão ERP

**Versão:** 1.0.0
**Status:** Em Desenvolvimento Ativo
**Módulo Principal:** Gestão de Estoque Avançada
**Próximo Módulo:** Leitor e Processador de NF-e

---

## 1. Visão Geral do Projeto

O **Projeto Fisgarone** é uma iniciativa para construir um sistema de gestão integrada (ERP) moderno, robusto e modular, projetado para atender às necessidades específicas de uma pequena empresa em crescimento. O objetivo final é criar uma plataforma centralizada que unifique e automatize operações críticas do negócio, incluindo:

*   **Estoque:** Gestão avançada de produtos, custos, inventários e movimentações.
*   **Vendas:** Integração com APIs de marketplaces e gestão de pedidos.
*   **Financeiro:** Controle de contas a pagar e receber, fluxo de caixa e relatórios.
*   **Compras:** Processamento de notas fiscais de entrada (NF-e) e sugestão de compras.
*   **Recursos Humanos:** Funções como cartão de ponto e gestão de colaboradores.

O sistema está sendo construído com uma filosofia de **"cada coisa no seu lugar"**, utilizando uma arquitetura modular em Flask que garante escalabilidade, organização e facilidade de manutenção.

---

## 2. Arquitetura do Sistema

A fundação do Fisgarone é uma arquitetura de software limpa e desacoplada, projetada para suportar o crescimento futuro.

*   **`app.py` (O Maestro):** O arquivo principal é extremamente enxuto. Sua única responsabilidade é configurar a aplicação Flask, inicializar extensões globais (como o Banco de Dados e o Babel para internacionalização) e registrar os *Blueprints* de cada módulo. Ele não contém nenhuma lógica de negócio.

*   **`modulos/` (Os Pilares):** Cada grande funcionalidade do sistema (Estoque, NF-e, Vendas, etc.) é um módulo independente dentro desta pasta. Cada módulo é um *Blueprint* do Flask.

*   **`templates/` (A Fachada):** Todos os arquivos HTML residem aqui, organizados em subpastas que espelham a estrutura dos módulos (ex: `templates/estoque/`, `templates/home/`). Isso mantém a lógica de apresentação separada da lógica de negócios.

*   **Módulo `home` (O Cockpit):** Este é o ponto de entrada do sistema. Ele é responsável pelo dashboard principal (que no futuro consolidará informações de todos os outros módulos) e pela geração do menu de navegação global.

---

## 3. Módulos Implementados

### 3.1. Módulo `home`

*   **Responsabilidade:** Ponto de entrada e navegação principal do ERP.
*   **Funcionalidades:**
    *   **Dashboard Principal (`/`):** Atualmente, serve como uma página de boas-vindas e um mapa do desenvolvimento do projeto. No futuro, exibirá KPIs (Key Performance Indicators) de todos os módulos.
    *   **API de Menu (`/api/menu`):** Gera dinamicamente a estrutura do menu lateral, garantindo que novos módulos possam ser adicionados à navegação de forma centralizada.

### 3.2. Módulo `estoque`

Este é o primeiro e mais avançado pilar do sistema Fisgarone.

*   **Responsabilidade:** Gerenciamento completo do ciclo de vida dos produtos e do seu valor.
*   **Funcionalidades Implementadas:**
    *   **Dashboard de Estoque:** Visão geral com cards de resumo (Custo Total, Itens, Produtos Críticos), gráficos de Top 10 Produtos por Custo e um widget de Alertas de Reposição.
    *   **CRUD de Produtos:** Funcionalidade completa para Criar, Ler, Atualizar e Excluir produtos.
    *   **Movimentações de Estoque:** Registro de entradas, saídas e ajustes manuais.
    *   **Inventário Físico Avançado:**
        *   Criação de eventos de inventário (geral, cíclico).
        *   Tela de contagem interativa com filtros e resumo em tempo real.
        *   Lógica de finalização que calcula as divergências e cria **movimentações de ajuste** para garantir 100% de rastreabilidade, atualizando o estoque para a quantidade física contada.
        *   Relatório pós-inventário que resume as perdas e sobras.
    *   **Processador de Entradas de NF-e:** O coração da automação de compras.

---

## 4. Destaque: O Processador de Entradas e o Cálculo de Custo Fracionado

Esta é a funcionalidade mais crítica e complexa implementada até agora, projetada para resolver um problema do mundo real: **comprar em caixas, vender em unidades.**

### O Problema
Notas fiscais de fornecedores raramente especificam o custo unitário do produto final. Uma nota pode listar "1 CX de Pirulito" por R$ 50,00, mas o sistema precisa saber o custo de "1 UN de Pirulito".

### A Solução Fisgarone

Criamos um fluxo de trabalho inteligente e desacoplado para resolver isso:

1.  **Fila de Processamento:** Uma tabela (`ProdutoNF`) atua como uma "caixa de entrada" para itens de notas fiscais que precisam ser processados. O futuro Módulo NF-e irá popular esta tabela.

2.  **Tela de Processamento:** Uma interface dedicada (`/estoque/processar-entradas`) mostra ao usuário todos os itens pendentes.

3.  **A Calculadora de Conversão Inteligente:** Ao clicar em "Processar", o usuário é levado a uma tela de cálculo, que é o cérebro da operação:
    *   **Contexto:** O sistema mostra o item da compra (Ex: 1 CX por R$ 50,00) e o produto correspondente no estoque (Ex: Pirulito Super Pop, vendido em UN).
    *   **Entrada Dinâmica:** O usuário informa como a unidade de compra se decompõe. O sistema não assume nada. Ele pergunta:
        *   "Quantos pacotes vêm na **CX**?" (Ex: 20)
        *   "Quantas **UN** vêm em cada pacote?" (Ex: 50)
    *   **Cálculo em Tempo Real:** Com base nesses dados, o sistema calcula e exibe instantaneamente:
        *   **Fator de Conversão Total:** 20 * 50 = **1000**.
        *   **Quantidade a Entrar no Estoque:** 1 (CX) * 1000 = **1000 UN**.
        *   **Custo Unitário Real:** R$ 50,00 / 1000 UN = **R$ 0,05**.

4.  **Custo Médio Ponderado:** Ao confirmar a entrada, o sistema não simplesmente substitui o custo. Ele recalcula o custo unitário do produto usando o método do Custo Médio Ponderado, garantindo uma avaliação de estoque precisa e em conformidade com as práticas contábeis.
    *   `Novo Custo Médio = (Valor do Estoque Antigo + Valor da Nova Compra) / (Qtd Antiga + Qtd Nova)`

5.  **Aprendizado Contínuo (Inteligência):** O sistema inclui um checkbox: **"Salvar esta regra de cálculo?"**. Se marcado, a regra (Ex: "CX -> 1000 UN") é salva na tabela `ProdutoFornecedor`, associada àquele produto. Na próxima vez que um item similar for processado, o sistema poderá usar essa regra para preencher a calculadora automaticamente, transformando um processo de múltiplos passos em um único clique de confirmação.

Este fluxo garante que o custo e a quantidade dos produtos no sistema Fisgarone sejam **precisos, rastreáveis e auditáveis**, independentemente de como o produto é comprado.

---

## 5. Próximos Passos e Futuro do Projeto

Com a arquitetura organizada e o Módulo de Estoque estabilizado, o caminho está livre para os próximos grandes avanços:

1.  **Desenvolver o Módulo NF-e:**
    *   Criar a lógica para monitorar a `pasta_xml/`.
    *   Implementar um leitor de XML para extrair os dados da nota fiscal.
    *   Alimentar a tabela `ProdutoNF` com os dados lidos, entregando os itens para o nosso já funcional Processador de Entradas.
    *   Construir a tela de "Painel de Controle de NF-e" para visualizar, gerenciar e processar as notas lidas.

2.  **Implementar a Camada de IA:**
    *   Ativar a análise por IA no relatório de inventário para sugerir causas de divergências.
    *   Criar o gerador de inventário cíclico inteligente no dashboard.

3.  **Desenvolver os Módulos Subsequentes:**
    *   Módulo de Vendas.
    *   Módulo Financeiro.
    *   E todos os outros pilares do ERP Fisgarone.

Este documento servirá como nosso guia. O Projeto Fisgarone está bem fundamentado e pronto para crescer.
