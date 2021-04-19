import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *

parser = argparse.ArgumentParser(description="Parse Censo data")

parser.add_argument('--database', required=True)
parser.add_argument('--table', required=True)
parser.add_argument('--csv-path', required=True)  # /spark/data/matricula/
# hdfs://namenode:8020/user/hive/warehouse/censo
parser.add_argument('--dwh-table-path', required=True)
args = parser.parse_args()

database = args.database
table = args.table
csv_path = args.csv_path
dwh_table_path = args.dwh_table_path

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Censo") \
    .enableHiveSupport() \
    .getOrCreate()

df = StructType([StructField("NU_ANO_CENSO", StringType(), True),
                 StructField("ID_ALUNO", IntegerType(), True),
                 StructField("ID_MATRICULA", StringType(), True),
                 StructField("NU_MES", StringType(), True),
                 StructField("NU_ANO", StringType(), True),
                 StructField("NU_IDADE_REFERENCIA", StringType(), True),
                 StructField("NU_IDADE", StringType(), True),
                 StructField("TP_SEXO", StringType(), True),
                 StructField("TP_COR_RACA", IntegerType(), True),
                 StructField("TP_NACIONALIDADE", StringType(), True),
                 StructField("CO_PAIS_ORIGEM", StringType(), True),
                 StructField("CO_UF_NASC", StringType(), True),
                 StructField("CO_MUNICIPIO_NASC", StringType(), True),
                 StructField("CO_UF_END", StringType(), True),
                 StructField("CO_MUNICIPIO_END", StringType(), True),
                 StructField("TP_ZONA_RESIDENCIAL", StringType(), True),
                 StructField("TP_LOCAL_RESID_DIFERENCIADA",
                             StringType(), True),
                 StructField("IN_NECESSIDADE_ESPECIAL", StringType(), True),
                 StructField("IN_BAIXA_VISAO", StringType(), True),
                 StructField("IN_CEGUEIRA", StringType(), True),
                 StructField("IN_DEF_AUDITIVA", StringType(), True),
                 StructField("IN_DEF_FISICA", StringType(), True),
                 StructField("IN_DEF_INTELECTUAL", StringType(), True),
                 StructField("IN_SURDEZ", StringType(), True),
                 StructField("IN_SURDOCEGUEIRA", StringType(), True),
                 StructField("IN_DEF_MULTIPLA", StringType(), True),
                 StructField("IN_AUTISMO", StringType(), True),
                 StructField("IN_SUPERDOTACAO", StringType(), True),
                 StructField("IN_RECURSO_LEDOR", StringType(), True),
                 StructField("IN_RECURSO_TRANSCRICAO", StringType(), True),
                 StructField("IN_RECURSO_INTERPRETE", StringType(), True),
                 StructField("IN_RECURSO_LIBRAS", StringType(), True),
                 StructField("IN_RECURSO_LABIAL", StringType(), True),
                 StructField("IN_RECURSO_AMPLIADA_18", StringType(), True),
                 StructField("IN_RECURSO_AMPLIADA_24", StringType(), True),
                 StructField("IN_RECURSO_CD_AUDIO", StringType(), True),
                 StructField("IN_RECURSO_PROVA_PORTUGUES", StringType(), True),
                 StructField("IN_RECURSO_VIDEO_LIBRAS", StringType(), True),
                 StructField("IN_RECURSO_BRAILLE", StringType(), True),
                 StructField("IN_RECURSO_NENHUM", StringType(), True),
                 StructField("IN_AEE_LIBRAS", StringType(), True),
                 StructField("IN_AEE_LINGUA_PORTUGUESA", StringType(), True),
                 StructField("IN_AEE_INFORMATICA_ACESSIVEL",
                             StringType(), True),
                 StructField("IN_AEE_BRAILLE", StringType(), True),
                 StructField("IN_AEE_CAA", StringType(), True),
                 StructField("IN_AEE_SOROBAN", StringType(), True),
                 StructField("IN_AEE_VIDA_AUTONOMA", StringType(), True),
                 StructField("IN_AEE_OPTICOS_NAO_OPTICOS", StringType(), True),
                 StructField("IN_AEE_ENRIQ_CURRICULAR", StringType(), True),
                 StructField("IN_AEE_DESEN_COGNITIVO", StringType(), True),
                 StructField("IN_AEE_MOBILIDADE", StringType(), True),
                 StructField("TP_OUTRO_LOCAL_AULA", StringType(), True),
                 StructField("IN_TRANSPORTE_PUBLICO", StringType(), True),
                 StructField("TP_RESPONSAVEL_TRANSPORTE", StringType(), True),
                 StructField("IN_TRANSP_BICICLETA", StringType(), True),
                 StructField("IN_TRANSP_MICRO_ONIBUS", StringType(), True),
                 StructField("IN_TRANSP_ONIBUS", StringType(), True),
                 StructField("IN_TRANSP_TR_ANIMAL", StringType(), True),
                 StructField("IN_TRANSP_VANS_KOMBI", StringType(), True),
                 StructField("IN_TRANSP_OUTRO_VEICULO", StringType(), True),
                 StructField("IN_TRANSP_EMBAR_ATE5", StringType(), True),
                 StructField("IN_TRANSP_EMBAR_5A15", StringType(), True),
                 StructField("IN_TRANSP_EMBAR_15A35", StringType(), True),
                 StructField("IN_TRANSP_EMBAR_35", StringType(), True),
                 StructField("TP_ETAPA_ENSINO", IntegerType(), True),
                 StructField("IN_ESPECIAL_EXCLUSIVA", StringType(), True),
                 StructField("IN_REGULAR", StringType(), True),
                 StructField("IN_EJA", StringType(), True),
                 StructField("IN_PROFISSIONALIZANTE", StringType(), True),
                 StructField("ID_TURMA", StringType(), True),
                 StructField("CO_CURSO_EDUC_PROFISSIONAL", StringType(), True),
                 StructField("TP_MEDIACAO_DIDATICO_PEDAGO",
                             StringType(), True),
                 StructField("NU_DURACAO_TURMA", StringType(), True),
                 StructField("NU_DUR_ATIV_COMP_MESMA_REDE",
                             StringType(), True),
                 StructField("NU_DUR_ATIV_COMP_OUTRAS_REDES",
                             StringType(), True),
                 StructField("NU_DUR_AEE_MESMA_REDE", StringType(), True),
                 StructField("NU_DUR_AEE_OUTRAS_REDES", StringType(), True),
                 StructField("NU_DIAS_ATIVIDADE", StringType(), True),
                 StructField("TP_UNIFICADA", StringType(), True),
                 StructField("TP_TIPO_ATENDIMENTO_TURMA", StringType(), True),
                 StructField("TP_TIPO_LOCAL_TURMA", StringType(), True),
                 StructField("CO_ENTIDADE", StringType(), True),
                 StructField("CO_REGIAO", StringType(), True),
                 StructField("CO_MESORREGIAO", StringType(), True),
                 StructField("CO_MICRORREGIAO", StringType(), True),
                 StructField("CO_UF", IntegerType(), True),
                 StructField("CO_MUNICIPIO", IntegerType(), True),
                 StructField("CO_DISTRITO", StringType(), True),
                 StructField("TP_DEPENDENCIA", StringType(), True),
                 StructField("TP_LOCALIZACAO", StringType(), True),
                 StructField("TP_CATEGORIA_ESCOLA_PRIVADA",
                             StringType(), True),
                 StructField("IN_CONVENIADA_PP", StringType(), True),
                 StructField("TP_CONVENIO_PODER_PUBLICO", StringType(), True),
                 StructField("IN_MANT_ESCOLA_PRIVADA_EMP", StringType(), True),
                 StructField("IN_MANT_ESCOLA_PRIVADA_ONG", StringType(), True),
                 StructField("IN_MANT_ESCOLA_PRIVADA_OSCIP",
                             StringType(), True),
                 StructField("IN_MANT_ESCOLA_PRIV_ONG_OSCIP",
                             StringType(), True),
                 StructField("IN_MANT_ESCOLA_PRIVADA_SIND",
                             StringType(), True),
                 StructField("IN_MANT_ESCOLA_PRIVADA_SIST_S",
                             StringType(), True),
                 StructField("IN_MANT_ESCOLA_PRIVADA_S_FINS",
                             StringType(), True),
                 StructField("TP_REGULAMENTACAO", StringType(), True),
                 StructField("TP_LOCALIZACAO_DIFERENCIADA",
                             StringType(), True),
                 StructField("IN_EDUCACAO_INDIGENA", StringType(), True)])

df = spark.read \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv(csv_path)

df = df.select("ID_ALUNO", "CO_MUNICIPIO", "TP_ETAPA_ENSINO",
               "CO_UF", "TP_COR_RACA")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

df \
    .repartition('TP_ETAPA_ENSINO') \
    .write.mode("overwrite") \
    .partitionBy(['TP_ETAPA_ENSINO']) \
    .saveAsTable(
        f"{database}.{table}",
        path=dwh_table_path,
        overwrite=True)
