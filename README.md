# Teste Técnico: Processamento de Dados ANS
**Candidata:** Madelu Lopes

## 📝 Visão Geral
Este projeto realiza o pipeline de ETL (Extração, Transformação e Carga) dos dados de demonstrações contábeis da ANS, consolidando informações trimestrais e enriquecendo os dados com informações cadastrais de operadoras ativas.

---

## 🛠️ Decisões Técnicas e Estratégia

### 1. Ingestão e Resiliência (Tarefa 1.1)
* **Abordagem:** Implementação utilizando a biblioteca `requests` com tratamento de exceções (`try-except`).
* **Resiliência:** O script foi desenhado para ignorar falhas pontuais de download e prosseguir com o processamento dos trimestres disponíveis.
* **Trade-off:** Devido a instabilidades no servidor FTP da ANS durante o desenvolvimento, optei pelo download manual dos últimos 3 trimestres de 2025 para garantir a integridade dos dados e o cumprimento do prazo.

### 2. Processamento em Memória (KISS)
Como analista vinda do Power BI, priorizei a simplicidade e performance. Utilizei o **Pandas** para processamento em memória. Dado o volume atual (3 trimestres), esta abordagem oferece a melhor relação entre rapidez de entrega e facilidade de manutenção.

### 3. Normalização e Limpeza (Tarefa 1.2 e 1.3)
* **Mapeamento Flexível:** Implementei um dicionário de mapeamento para tratar a inconsistência nos nomes das colunas (ex: `VL_SALDO_FINAL` vs `Valor Despesas`).
* **Tratamento de Inconsistências:**
    * Removi valores negativos/zerados por serem contabilmente inconsistentes para despesas de sinistros.
    * Mantive o primeiro registro em caso de CNPJs duplicados para assegurar a integridade da chave primária.

### 4. Validação e Enriquecimento (Tarefa 2)
* **Validação de CNPJ:** Adotei a estratégia de não descartar dados. Criei a coluna `Status_CNPJ` para sinalizar registros "Suspeitos", preservando o volume financeiro total para auditoria.
* **Join (Enriquecimento):** Realizei um `left join` entre as despesas e o cadastro CADOP. Operadoras não encontradas no cadastro foram sinalizadas como **"Operadora não identificada"** (o que internamente apelidamos de "Gato Laranja" para monitoramento de inconsistências).

### 5. Estatísticas e Carga SQL (Tarefa 3)
* **Agregação:** Calculei soma, média e desvio padrão agrupados por Razão Social e UF.
* **Persistência:** Os dados finais foram carregados em um banco de dados **SQLite** (`teste_ans_madelu.db`), garantindo que as informações estejam prontas para consumo por ferramentas de BI ou consultas SQL.

---

## 📈 Insights Observados
Durante a análise estatística, notei que algumas UFs apresentam um **Desvio Padrão muito elevado**. Isso sugere uma alta concentração de gastos em operadoras específicas ou uma forte sazonalidade nos sinistros daquela região, ponto que mereceria uma investigação profunda em um cenário real de auditoria.

---

## 📁 Estrutura do Projeto
- `main.py`: Script principal de execução.
- `dados_extraidos/`: Arquivos CSV processados.
- `dados_cadastrais/`: Relatório CADOP de referência.
- `consolidado_despesas.zip`: Entrega compactada da Etapa 1.
- `teste_ans_madelu.db`: Banco de dados gerado.
