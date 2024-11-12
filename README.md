# Projeto-LagoMago
Projeto de migração e construção de DataLake utilizando Databricks Azure.

# Etapas
- [x] DB
- [x] Raw
- [x] Bronze
- [ ] Silver
- [ ] Gold

# Configuração do Databricks
Cluster:
- Contém a variável de ambiente BLOB_KEY que garante o acesso ao storage do Microsoft Azure.

Workflow:
- Cada tarefa possui os parâmetros catalog, schema e tablename. Também é disponibilizado uma cópia do JSON reset para manipulação local.

# Arquivo .ENV
No arquivo .ENV, foram utilizadas duas variáveis de ambiente que garantem o acesso ao Workflow dentro do Databricks

<h3>Variáveis:</h3>

DATABRICKS_TOKEN:
- Gerado a partir da API do Databricks.

DATABRICKS_HOST:
- URL do Databricks.

<h3>Requisição:</h3>

``` ruby
import dotenv
import requests

def reset_job(settings):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/reset"
    header = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    
    resp = requests.post(url=url, headers=header, json=settings)
    return resp
    
```

