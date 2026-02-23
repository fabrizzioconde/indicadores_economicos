# Macro Insights MVP

MVP de produto de dados macroeconômicos para visualização de **SELIC**, **IPCA**, **IPCA-15**, **câmbio USD/BRL** e **IBC-Br**, desenvolvido em Python com dashboard em Streamlit. Os dados são obtidos das APIs do Banco Central (BACEN) e do IBGE, processados por um ETL e exibidos em gráficos e métricas.

### Quick start

Na raiz do projeto, com **Python 3.12+** instalado:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python run_etl.py --mode full
streamlit run dash/app.py
```

(O segundo comando no Linux/macOS é `source .venv/bin/activate`.) O navegador abrirá o dashboard em `http://localhost:8501`.

---

## Arquitetura do Projeto

A estrutura do repositório segue uma organização por função:

```
macro_insights_mvp/
├── etl/                    # Extração, transformação e carga dos dados
│   ├── bacen.py            # ETL das séries do BACEN (SELIC, câmbio, IBC-Br)
│   ├── ibge_ipca.py        # ETL do IPCA e IPCA-15 (API SIDRA do IBGE)
│   └── pipeline.py         # Orquestração: executa os ETLs e salva em data/gold
├── config/
│   └── settings.py         # Configurações: códigos de séries, datas padrão, caminhos
├── dash/
│   └── app.py              # Aplicação Streamlit (dashboard com abas)
├── data/                   # Dados em camadas (raw → processed → gold)
│   ├── raw/                # Dados brutos (se necessário no futuro)
│   ├── processed/          # Dados tratados (intermediários)
│   └── gold/               # Dados prontos para o dashboard (parquet)
├── tests/
│   └── test_etl_basic.py   # Testes de estrutura do ETL (pytest)
├── scripts/                  # Scripts para automação
│   ├── run_etl_daily.bat     # ETL diário no Windows (Agendador de Tarefas)
│   ├── run_etl_daily.sh      # ETL diário no Linux/macOS (cron)
│   ├── test_automation.bat   # Teste: roda o ETL e exibe OK/Falhou (Windows)
│   ├── test_automation.ps1   # Teste: idem, com cores (PowerShell)
│   └── test_automation.sh    # Teste: idem (Linux/macOS)
├── logs/                    # Log da execução diária do ETL (gerado automaticamente)
├── run_etl.py               # Script de linha de comando para rodar o ETL
├── requirements.txt         # Dependências (pip)
├── pyproject.toml           # Metadados e dependências do projeto
└── README.md
```

### Camadas de dados

- **raw**: Dados exatamente como recebidos da fonte (hoje o ETL grava direto em *gold*; essa pasta pode ser usada para armazenar respostas brutas da API).
- **processed**: Dados já limpos e padronizados, ainda não no formato final do produto.
- **gold**: Dados prontos para consumo pelo dashboard. Os arquivos parquet aqui (por exemplo `selic.parquet`, `usdbrl.parquet`, `ipca.parquet`) são os que o Streamlit lê para exibir gráficos e métricas.

---

## Instalação

Recomenda-se usar um ambiente virtual para isolar as dependências.

### 1. Criar e ativar o ambiente virtual

No terminal, na pasta raiz do projeto (`macro_insights_mvp`):

**Windows (PowerShell ou cmd):**
```bash
python -m venv .venv
.venv\Scripts\activate
```

**Linux/macOS:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Instalar as dependências

Com o ambiente ativado:

```bash
pip install -r requirements.txt
```

Ou, se preferir instalar o projeto em modo editável (recomendado para desenvolvimento):

```bash
pip install -e .
```

As dependências incluem: `pandas`, `numpy`, `requests`, `pyarrow`, `streamlit`, `plotly`, `pytest` (para rodar os testes), entre outras listadas em `requirements.txt` ou `pyproject.toml`. Requer **Python 3.12 ou superior**.

---

## Como atualizar os dados (ETL)

O ETL baixa séries do **BACEN** (SELIC, câmbio, IBC-Br) e do **IBGE** (IPCA, IPCA-15) e grava arquivos parquet em `data/gold`.

### Rodar o ETL completo

Na raiz do projeto, com o ambiente ativado:

```bash
python run_etl.py --mode full
```

Isso executa, em sequência, o ETL de todas as séries e gera (ou atualiza) os arquivos em `data/gold`, por exemplo: `selic.parquet`, `usdbrl.parquet`, `ibcbr.parquet`, `ipca.parquet`, `ipca15.parquet`.

### Modo mínimo (testes rápidos)

Para atualizar apenas SELIC, câmbio e IPCA (útil para testar sem esperar todas as séries):

```bash
python run_etl.py --mode minimal
```

---

## Atualização diária automática

Para que o dashboard esteja sempre com dados recentes sem rodar o ETL manualmente, você pode agendar a execução do ETL uma vez por dia. O projeto inclui scripts prontos na pasta `scripts/`:

- **Windows:** `scripts/run_etl_daily.bat`
- **Linux/macOS:** `scripts/run_etl_daily.sh`

Eles ativam o ambiente virtual, executam `python run_etl.py --mode full` e registram a saída em `logs/etl_daily.log`. Assim, sempre que você abrir o dashboard, os parquets em `data/gold` terão sido atualizados na última execução agendada.

### Windows: Agendador de Tarefas

1. Abra o **Agendador de Tarefas** (busque por "Agendador de Tarefas" no menu Iniciar).
2. Clique em **Criar Tarefa Básica**.
3. Nome: por exemplo `Macro Insights - ETL diário`. Avançar.
4. Gatilho: **Diariamente**. Avançar e escolha um horário (ex.: 7h00, após as divulgações do BACEN/IBGE).
5. Ação: **Iniciar um programa**.
6. Programa/script: informe o caminho completo do `.bat`, por exemplo:
   `C:\Users\fabri\OneDrive\Documentos\Dashboard_Indicadores\macro_insights_mvp\scripts\run_etl_daily.bat`
7. "Iniciar em" (opcional): pasta raiz do projeto, ex.:
   `C:\Users\fabri\OneDrive\Documentos\Dashboard_Indicadores\macro_insights_mvp`
8. Concluir. A tarefa passará a rodar todo dia no horário definido.

Para conferir se rodou, abra `macro_insights_mvp\logs\etl_daily.log`.

### Linux / macOS: cron

1. Torne o script executável (uma vez):
   ```bash
   chmod +x scripts/run_etl_daily.sh
   ```
2. Edite a crontab:
   ```bash
   crontab -e
   ```
3. Adicione uma linha para rodar todo dia às 7h (ajuste o caminho para o seu usuário):
   ```bash
   0 7 * * * /caminho/completo/para/macro_insights_mvp/scripts/run_etl_daily.sh
   ```
   Exemplo em um diretório em casa:
   ```bash
   0 7 * * * /home/seu_usuario/Dashboard_Indicadores/macro_insights_mvp/scripts/run_etl_daily.sh
   ```
4. Salve e saia. O log ficará em `macro_insights_mvp/logs/etl_daily.log`.

Recomenda-se um horário em que as fontes (BACEN, IBGE) já tenham publicado os dados do dia (por exemplo início da manhã).

### Como testar se a automatização está funcionando

Rode o mesmo fluxo que o agendamento executa e veja o resultado em uma linha:

**Windows (CMD ou PowerShell), na raiz do projeto:**

```bash
scripts\test_automation.bat
```

No **Git Bash**, use barra normal: `scripts/test_automation.bat`

Ou em PowerShell, com **OK** em verde e **Falhou** em vermelho:

```bash
.\scripts\test_automation.ps1
```

**Linux/macOS (na raiz do projeto):**

```bash
chmod +x scripts/test_automation.sh   # só na primeira vez
./scripts/test_automation.sh
```

- Se aparecer **OK**, o ETL concluiu com sucesso; os parquets em `data/gold` e o `logs/etl_daily.log` foram atualizados.
- Se aparecer **Falhou**, confira `logs/etl_daily.log` para ver o erro (por exemplo timeout ou API indisponível).

Para testar o agendamento no Windows: Agendador de Tarefas → clique direito na tarefa → **Executar**; depois verifique o log e a data de modificação dos arquivos em `data/gold`.

---

## Como iniciar o dashboard

Com os dados já gerados (ou pelo menos parte deles) em `data/gold`, inicie o dashboard com:

```bash
streamlit run dash/app.py
```

O comando deve ser executado na **raiz do projeto** (`macro_insights_mvp`). O navegador abrirá em `http://localhost:8501`.

### O que você verá no dashboard

- **Barra lateral**: filtro de data inicial e final, e opção para mostrar variações percentuais quando aplicável.
- **Aba Resumo**: KPIs rápidos (última SELIC, IPCA acumulado 12 meses, variação do câmbio no período) e um texto explicativo sobre a origem dos dados.
- **Aba Juros**: gráfico de linha da taxa SELIC ao longo do tempo.
- **Aba Inflação**: gráficos do IPCA e IPCA-15 (variação mensal) e, se habilitado, inflação acumulada em 12 meses.
- **Aba Câmbio**: cotação USD/BRL e, opcionalmente, variação percentual em janela de 30 dias.
- **Aba Atividade**: gráfico do IBC-Br e métrica simples de crescimento ano contra ano.

Se algum arquivo parquet estiver faltando em `data/gold`, o dashboard exibirá um aviso orientando a rodar `python run_etl.py --mode full`.

---

## Testes

Os testes focam em validações estruturais do ETL (colunas, tipos, criação de arquivos), usando pytest e mocks para não depender da API em tempo de teste. O **pytest** já vem listado em `requirements.txt` e no `pyproject.toml`, então basta ter o ambiente ativado e as dependências instaladas.

Na raiz do projeto:

```bash
python -m pytest tests/test_etl_basic.py -v
```

Para rodar apenas os testes marcados como básicos:

```bash
python -m pytest tests/test_etl_basic.py -m basic -v
```

---

## Próximos passos

Sugestões de evolução do MVP para um produto mais completo:

1. **Autenticação e monetização**  
   Adicionar login/senha (por exemplo com OAuth ou sessão) e integração com **Stripe** (ou similar) para planos pagos ou acesso premium.

2. **Dashboards setoriais**  
   Criar visões específicas por setor: varejo, indústria, agro, serviços, com indicadores e séries relevantes para cada um.

3. **Insights automáticos com IA**  
   Integrar modelos de linguagem ou análise automática para gerar textos explicativos, alertas (ex.: “IPCA acumulado em 12 meses subiu X pontos”) e resumos a partir dos dados exibidos.

4. **Dados em tempo real e alertas**  
   Agendar o ETL (cron ou tarefa agendada) e notificar o usuário quando houver atualizações ou quando métricas ultrapassarem limites configuráveis.

5. **Exportação e API**  
   Permitir exportar tabelas (CSV/Excel) e expor os dados gold via API REST para integração com outras ferramentas.

---

## Licença

Projeto de uso didático e interno; definir licença conforme a política da organização.
