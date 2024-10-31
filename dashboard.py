import streamlit as st
import pyodbc
from azure import identity
import struct
import pandas as pd
import requests
import base64
import os
from dotenv import load_dotenv

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Obter o token do GitHub
github_token = os.getenv("GITHUB_TOKEN")

# Função para executar a consulta SQL, salvar em parquet e fazer upload para o GitHub
def execute_query_and_upload(query, connection_string, repo, file_name_on_github, token, file_name="resultado.parquet"):
    try:
        # Obter credenciais e conectar ao banco de dados
        credential = identity.DefaultAzureCredential(
            exclude_interactive_browser_credential=False
        )
        token_bytes = credential.get_token(
            "https://database.windows.net/.default"
        ).token.encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        
        # Conectar ao banco de dados usando o gerenciador de contexto 'with'
        with pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}) as conn:
            # Executar a consulta e armazenar o resultado em um DataFrame
            df = pd.read_sql_query(query, conn)
            
            # Salvar o DataFrame como arquivo parquet
            df.to_parquet(file_name, index=False)
            print(f"Arquivo salvo como {file_name}")
        
        # Fazer o upload do arquivo para o GitHub
        with open(file_name, "rb") as f:
            content = f.read()

        # Codificar o conteúdo em base64
        encoded_content = base64.b64encode(content).decode()

        # URL da API do GitHub para o repositório e caminho do arquivo
        url = f'https://api.github.com/repos/{repo}/contents/{file_name_on_github}'

        # Dados da requisição
        headers = {
            'Authorization': f'token {token}',
            'Content-Type': 'application/json'
        }

        # Primeiro, verificar se o arquivo já existe para obter o sha
        get_response = requests.get(url, headers=headers)
        if get_response.status_code == 200:
            sha = get_response.json().get('sha')
            data = {
                'message': 'Atualização do arquivo parquet',
                'content': encoded_content,
                'sha': sha
            }
        else:
            data = {
                'message': 'Criação do arquivo parquet',
                'content': encoded_content
            }

        # Fazer o PUT para criar/atualizar o arquivo
        response = requests.put(url, headers=headers, json=data)

        if response.status_code in [200, 201]:
            print(f"Arquivo '{file_name_on_github}' atualizado com sucesso no GitHub!")
        else:
            print(f"Falha ao atualizar o arquivo no GitHub: {response.json()}")
    except Exception as e:
        print(f"Erro ao executar a consulta ou fazer upload para o GitHub: {e}")

# Exemplo de uso com Streamlit
st.title("Atualizar Dados e Dashboard")

# Parâmetros fornecidos pelo usuário
github_repo = st.text_input("Repositório do GitHub", 'usuario/repo')  # Ex: 'meu_usuario/meu_repo'
file_name_on_github = st.text_input("Caminho do arquivo no repositório", 'caminho/no/repositorio/resultado.parquet')
consultaSQL = st.text_area("Consulta SQL", "SELECT TOP 10 Nome, RA, Projeto FROM dbo.Aluno WHERE Projeto LIKE 'Ensino Superior'")
connection_string = st.text_area("String de conexão", 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:ismart-server.database.windows.net,1433;Database=ismart-db;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')

# Botão para executar a consulta e atualizar o arquivo
if st.button("Atualizar Dados e Fazer Upload para o GitHub"):
    with st.spinner('Consultando dados, salvando em parquet e fazendo upload para o GitHub...'):
        execute_query_and_upload(consultaSQL, connection_string, github_repo, file_name_on_github, github_token)
