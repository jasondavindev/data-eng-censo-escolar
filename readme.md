# Censo Escolar

## Arquitetura

![Arquitetura](./images/censo_escolar.png)

- HDFS pela natureza distribuída e tolerante a falhas e usado de base pelo Apache Hadoop.
- Hive Metastore pela capacidade de armazenar metadados das estrutura, localização, partições etc e também pela compatibilidade com HDFS.
- Spark pela natureza distribuída, por usar uma técnica de processamento in-memory que reduz significativamente o tempo de processamento e pela interface com Python, PySpark que auxilia na escrita de scripts simples.
- Trino pela natureza distribuída, focada em consultas low-latency pela técnica de processamento in-memory e semântica SQL ANSI.

### Pontos de melhoria

- Subir o dado bruto total para reprocessamento
- Com o aumento do volume de dados e da frequência da geração de relatórios, substituir o Trino por Apache Hive, assim, **suportará maior carga de dados a ser processado** porém terá um tempo de processamento maior, por usar MapReduce (in disk)
- **Com o aumento do volume de dados**, substituir a camada de armazenamento para um Object Storage em nuvem, por exemplo, AWS S3. Isso minimiza a responsabilidade de gerenciamento e tem um escalonamento de disco de forma "orgânica"
- Repensar a forma com que os dados estão particionados

Observação: das tecnologias utilizadas, leva-se em conta migrar para um ambiente gerenciado a fim de minimizar preocupações com infra-estrutura.

## Schema

### Tabela censo.alunos

| Coluna          | Tipo    |
| --------------- | ------- |
| ID_ALUNO        | INTEGER |
| CO_MUNICIPIO    | INTEGER |
| CO_UF           | INTEGER |
| TP_COR_RACA     | INTEGER |
| TP_ETAPA_ENSINO | INTEGER |

A escolha foi manter todos os dados em somente uma tabela para otimização da leitura dos dados. Foi escolhido o formato Apache Parquet para compactação dos dados, e armazenamento do schema, partições e outros metadados que otimizam a leitura.

A decisão de particionar a tabela por Etapa de ensino foi tomada para otimizar a consulta do Passo 3 - requisito 1.

### Colunas adicionais

- IN_PROFISSIONALIZANTE e CO_REGIAO para saber as distribuição de pessoas por região que estão em cursos profissionalizantes
- TP_LOCALIZACAO para fazer um estudo de necessidades por tipo de localização

## Setup

O setup foi baseado na infra-estrutura usada pelo meu projeto de graduação que pode ser visualizado [aqui](https://github.com/jasondavindev/open-dataplatform).

Em todos os componentes, há um arquivo `docker-compose.yml` que facilita subir os containers das aplicações.

Para facilitar, listado os passos para subir o ambiente:

```bash
cd hdfs && docker-compose up
cd hive && docker-compose up hive-metastore
cd spark && docker-compose up && make import_script file=/path/para/arquivo/spark/transform.py # importe o arquivo para HDFS
cd trino && docker-compose up
cd airflow && docker-compose up
```

- Mova o arquivo `airflow/dags/censo_dag.py` para a respectiva pasta de dags no projeto `open-dataplatform`
- Navegue na rota http://localhost:9090 e habilite a execução da dag `censo_escolar`
- Após a execução completa da dag, navegue pela rota http://localhost:3000
- Execute as queries que estão na pasta `trino`
