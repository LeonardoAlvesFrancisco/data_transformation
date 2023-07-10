import time
import streamlit as st
from streamlit_option_menu import option_menu
import os
from csv_converter import CsvTransformations
columns  =  ['CNPJ_COMPLETO','CNPJ_BÁSICO_empresas', 'RAZÃO SOCIAL / NOME EMPRESARIAL', 'NATUREZA JURÍDICA',
                        'QUALIFICAÇÃO DO RESPONSÁVEL', 'CAPITAL SOCIAL DA EMPRESA', 'PORTE DA EMPRESA',
                        'ENTE FEDERATIVO RESPONSÁVEL', 'CNPJ_BÁSICO_estabelecimentos', 'CNPJ ORDEM', 'CNPJ DV', 'IDENTIFICADOR MATRIZ/FILIAL',
                        'NOME FANTASIA', 'SITUAÇÃO CADASTRAL', 'DATA SITUAÇÃO CADASTRAL',
                        'MOTIVO SITUAÇÃO CADASTRAL', 'NOME DA CIDADE NO EXTERIOR', 'PAIS',
                        'DATA DE INÍCIO ATIVIDADE', 'CNAE FISCAL PRINCIPAL', 'CNAE FISCAL SECUNDÁRIA',
                        'TIPO DE LOGRADOURO', 'LOGRADOURO', 'NÚMERO', 'COMPLEMENTO', 'BAIRRO', 'CEP',
                        'UF', 'MUNICÍPIO', 'DDD 1', 'TELEFONE 1', 'DDD 2', 'TELEFONE 2', 'DDD DO FAX',
                        'FAX', 'CORREIO ELETRÔNICO', 'SITUAÇÃO ESPECIAL', 'DATA DA SITUAÇÃO ESPECIAL','CNPJ_BÁSICO_socios', 'IDENTIFICADOR DE SÓCIO',
                        'NOME DO SÓCIO (NO CASO PF) OU RAZÃO SOCIAL (NO CASO PJ)',
                        'CNPJBÁSICO', 'QUALIFICAÇÃO DO SÓCIO', 'DATA DE ENTRADA SOCIEDADE',
                        'PAIS', 'REPRESENTANTE LEGAL', 'NOME DO REPRESENTANTE',
                        'QUALIFICAÇÃO DO REPRESENTANTE LEGAL', 'FAIXA ETÁRIA']

st.set_page_config("CNPJs Data Transformation",
                   page_icon="🤠",
                   layout="wide",
                   
                   )


with st.sidebar:
    selected = (option_menu(None, ["CNPJs", "Upload de Arquivos"], 
    icons=[ 'globe', "columns-gap"], 
    menu_icon="menu-up", default_index=0, orientation="vertical"))


if selected == "CNPJs":
    dynamic_selects = []
    dynamic_inputs = []

    cbColumn = st.selectbox(label="Coluna", options=columns)
    sb_filter = st.text_input(label="Digite o item que deseja filtrar:")
    df = CsvTransformations().send_sql_result()
    col1, col2 = st.columns(2)
    if col1.button("Filtrar"):
        df = CsvTransformations().sql_filter(str(cbColumn), str(sb_filter))
    st.write(df)
    if col2.button("Limpar"):
        if dynamic_selects:
            dynamic_selects.pop()
            dynamic_inputs.pop()
    

elif selected == "Upload de Arquivos":


    def unify_columns():
        CsvTransformations().unify_files()

    directory = ""
    option = st.selectbox("Selecione em qual diretório deseja subir os arquivos:", options=["Empresa", "Estabelecimentos", "Sócios"])
    if option == "Empresa":
        directory = "files/empresas"
    elif option == "Estabelecimentos":
        directory = "files/estabelecimentos"
    elif option == "Sócios":
        directory = "files/socios"

    files = st.file_uploader(label="Adicione o arquivo que deseja:", accept_multiple_files=True, type="csv")

    upload_button = st.button("Upload")

    if upload_button and files is not None:
        for file in files:
            file_path = os.path.join(directory, file.name)
            with open(file_path, "wb") as f:
                f.write(file.getbuffer())
        st.success("Arquivos carregados com sucesso!")

    delete_items = st.button("Remover todos os itens")
    if delete_items: 
        directories = ["files/empresas", "files/estabelecimentos", "files/socios", "parquet_files/merged_files.parquet"]
        for directory in directories: 
            for file_name in os.listdir(directory):
                file_path = os.path.join(directory, file_name)
                os.remove(file_path)
    st.info("Todos os arquivos foram excluídos.")
    unify_columns_button = st.button("Unificar tabelas")
    if unify_columns_button:
        progress_bar = st.progress(0)
        for i in range(101):
            time.sleep(0.1)
            progress_bar.progress(i)
        unify_columns()
        st.success("Colunas unificadas com sucesso!")

        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if file_path.endswith(".csv"):
                os.remove(file_path)
        st.info("Todos os arquivos CSV foram excluídos.")
    st.warning("Lembre-se: Os itens precisam estar na mesma quantidade para unificar as tabelas!")
