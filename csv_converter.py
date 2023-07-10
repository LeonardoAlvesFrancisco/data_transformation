import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


class CsvTransformations:
    
    def __init__(self) -> None:
        self.__directory = ["empresas", "estabelecimentos", "socios"]
        self.spark = SparkSession.builder \
                    .appName("MyApp") \
                    .config("spark.driver.memory", "50g") \
                    .config("spark.executor.memory", "4g") \
                    .getOrCreate()
    
    def unify_files(self):
        for directory in self.__directory:
            filepath = os.path.join('files', directory)
            files = [file for file in os.listdir(filepath) if file.endswith('.csv')]
            if directory == 'empresas':
                schema = StructType([
                    StructField('CNPJ BÁSICO', StringType(), True),
                    StructField('RAZÃO SOCIAL / NOME EMPRESARIAL', StringType(), True),
                    StructField('NATUREZA JURÍDICA', StringType(), True),
                    StructField('QUALIFICAÇÃO DO RESPONSÁVEL', StringType(), True),
                    StructField('CAPITAL SOCIAL DA EMPRESA', StringType(), True),
                    StructField('PORTE DA EMPRESA', StringType(), True),
                    StructField('ENTE FEDERATIVO RESPONSÁVEL', StringType(), True)
                ])
            elif directory == 'estabelecimentos':
                schema = StructType([
                    StructField('CNPJ BÁSICO', StringType(), True),
                    StructField('CNPJ ORDEM', StringType(), True),
                    StructField('CNPJ DV', StringType(), True),
                    StructField('IDENTIFICADOR MATRIZ/FILIAL', StringType(), True),
                    StructField('NOME FANTASIA', StringType(), True),
                    StructField('SITUAÇÃO CADASTRAL', StringType(), True),
                    StructField('DATA SITUAÇÃO CADASTRAL', StringType(), True),
                    StructField('MOTIVO SITUAÇÃO CADASTRAL', StringType(), True),
                    StructField('NOME DA CIDADE NO EXTERIOR', StringType(), True),
                    StructField('PAIS', StringType(), True),
                    StructField('DATA DE INÍCIO ATIVIDADE', StringType(), True),
                    StructField('CNAE FISCAL PRINCIPAL', StringType(), True),
                    StructField('CNAE FISCAL SECUNDÁRIA', StringType(), True),
                    StructField('TIPO DE LOGRADOURO', StringType(), True),
                    StructField('LOGRADOURO', StringType(), True),
                    StructField('NÚMERO', StringType(), True),
                    StructField('COMPLEMENTO', StringType(), True),
                    StructField('BAIRRO', StringType(), True),
                    StructField('CEP', StringType(), True),
                    StructField('UF', StringType(), True),
                    StructField('MUNICÍPIO', StringType(), True),
                    StructField('DDD 1', StringType(), True),
                    StructField('TELEFONE 1', StringType(), True),
                    StructField('DDD 2', StringType(), True),
                    StructField('TELEFONE 2', StringType(), True),
                    StructField('DDD DO FAX', StringType(), True),
                    StructField('FAX', StringType(), True),
                    StructField('CORREIO ELETRÔNICO', StringType(), True),
                    StructField('SITUAÇÃO ESPECIAL', StringType(), True),
                    StructField('DATA DA SITUAÇÃO ESPECIAL', StringType(), True)
                ])
            elif directory == 'socios':
                schema = StructType([
                    StructField('CNPJ BÁSICO', StringType(), True),
                    StructField('IDENTIFICADOR DE SÓCIO', StringType(), True),
                    StructField('NOME DO SÓCIO (NO CASO PF) OU RAZÃO SOCIAL (NO CASO PJ)', StringType(), True),
                    StructField('CNPJBÁSICO', StringType(), True),
                    StructField('QUALIFICAÇÃO DO SÓCIO', StringType(), True),
                    StructField('DATA DE ENTRADA SOCIEDADE', StringType(), True),
                    StructField('PAIS', StringType(), True),
                    StructField('REPRESENTANTE LEGAL', StringType(), True),
                    StructField('NOME DO REPRESENTANTE', StringType(), True),
                    StructField('QUALIFICAÇÃO DO REPRESENTANTE LEGAL', StringType(), True),
                    StructField('FAIXA ETÁRIA', StringType(), True)
                ])

            df_combined = None
            for file in files:
                df = self.spark.read.csv(
                    os.path.join(filepath, file),
                    schema=schema,
                    sep=";",
                    encoding="latin1",
                    header=False
                )
                if df_combined is None:
                    df_combined = df
                else:
                    df_combined = df_combined.union(df)

            df_combined.write.parquet(f"parquet_files/{directory}.parquet")

            
    



    def merge_with_join(self):

        empresas = self.spark.read.parquet('parquet_files/empresas.parquet')
        estabelecimentos = self.spark.read.parquet('parquet_files/estabelecimentos.parquet')
        socios = self.spark.read.parquet('parquet_files/socios.parquet')

        empresas = empresas.drop("__index_level_0__")
        estabelecimentos = estabelecimentos.drop("__index_level_0__", "PAIS")
        socios = socios.drop("__index_level_0__")

        empresas = empresas.withColumnRenamed("CNPJ BÁSICO", "CNPJ_BÁSICO_empresas")
        estabelecimentos = estabelecimentos.withColumnRenamed("CNPJ BÁSICO", "CNPJ_BÁSICO_estabelecimentos")
        socios = socios.withColumnRenamed("CNPJ BÁSICO", "CNPJ_BÁSICO_socios")

        merged_df = empresas.join(estabelecimentos, empresas['CNPJ_BÁSICO_empresas'] ==estabelecimentos['CNPJ_BÁSICO_estabelecimentos'], 'inner')
        merged_df = merged_df.join(socios, merged_df['CNPJ_BÁSICO_empresas'] == socios['CNPJ_BÁSICO_socios'], 'inner') 
            
        merged_df.write.mode("overwrite").parquet("parquet_files/merged_files.parquet")



    def send_sql_result(self):
        merged_df = self.spark.read.parquet("parquet_files/merged_files.parquet")
        merged_df.createOrReplaceTempView("CNPjs")
        return_table = self.spark.sql("SELECT CONCAT(`CNPJ_BÁSICO_empresas`,`CNPJ ORDEM`,`CNPJ DV`) AS CNPJ_COMPLETO, * FROM CNPjs")
        return return_table


    def sql_filter(self, column, filter):
        query = f"SELECT CONCAT(`CNPJ_BÁSICO_empresas`,`CNPJ ORDEM`,`CNPJ DV`) AS CNPJ_COMPLETO, * FROM CNPjs WHERE CNPjs.`{column}` = '{filter}'"
        result_df = self.spark.sql(query)
        return result_df
