import pandas as pd
import json
import re
from datetime import datetime
import requests
import os
from typing import Any
import time

dados = "dados.xlsx"
sistema = "sistema.xlsx"

class DataService: 
    """
    Classe responsável por carregar os dados de duas fontes (arquivos Excel).
    
    Atributos:
        dados_path (str): O caminho do arquivo de dados dos clientes.
        sistema_path (str): O caminho do arquivo contendo os clientes já cadastrados no sistema.
    """
    
    def __init__(self, dados_path, sistema_path) -> None: 
        self.dados_path =  dados_path
        self.sistema_path = sistema_path
        
    def get_data(self) -> list:
        """
        Lê os arquivos de dados e do sistema e retorna uma lista de DataFrames.
        
        Retorna:
            list: Uma lista contendo dois DataFrames, um com os dados dos clientes e outro com os clientes já cadastrados.
        """
        dfs = []
        sources = [self.dados_path, self.sistema_path]
        
        for source in sources:
            file_name, file_extension = os.path.splitext(source)
            
            try: 
                if file_name == "dados":
                    df_dados = pd.read_excel(source)
                    dfs.append(df_dados)
                else:
                    df_sistema = pd.read_excel(source)
                    dfs.append(df_sistema)
            except TypeError:
                print(f"{file_extension} is not a valid extension.")
                
        return dfs
    
    def __call__(self) -> list:
        """
        Método que torna a instância da classe chamável para obter os dados.
        
        Retorna:
            list: A lista de DataFrames retornada por `get_data()`.
        """
        return self.get_data()
    
class RowValidator:
    """
    Classe responsável por validar os dados de cada linha de cliente.
    
    Atributos:
        data (dict): Dicionário contendo os dados do cliente a ser validado.
    """
    
    def __init__(self, data):
        self.data = data
        
    def validate_cpf(self) -> bool:
        """
        Valida o CPF do cliente usando as regras de verificação de dígitos e formato.

        Retorna:
            bool: True se o CPF for válido, False caso contrário.
        """
        cpf = self.data.get('cpf')
        cpf = re.sub(r'\D', '', cpf)
        if len(cpf) != 11 or cpf == cpf[0] * 11:
            return False
        for i in range(9, 11):
            cpf_sum = sum(int(cpf[num]) * (i + 1 - num) for num in range(i))
            key_value = (cpf_sum * 10 % 11) % 10
            if key_value != int(cpf[i]):
                return False
        return True

    def validate_name(self) -> bool:
        """
        Valida se o nome do cliente tem pelo menos dois nomes.

        Retorna:
            bool: True se o nome for válido, False caso contrário.
        """
        name = self.data.get('nome')
        return len(name.split()) >= 2

    def validate_birthday_and_age(self) -> bool:
        """
        Valida a data de nascimento e a idade do cliente (deve ser maior ou igual a 17 anos).

        Retorna:
            bool: True se o cliente tiver 17 anos ou mais e a data for válida, False caso contrário.
        """
        birthday = self.data.get('data_nasc')
        today = datetime.today()
        
        if isinstance(birthday, datetime):
            birth_date = birthday
        else:
            formats = ['%Y-%m-%d', '%d/%m/%Y']
            birth_date = None
            for valid_format in formats:
                try:
                    birth_date = datetime.strptime(birthday, valid_format)
                    break
                except ValueError:
                    continue
            
            if birth_date is None:
                return False 

        age = (today - birth_date).days / 365.25
        return age >= 17

    def validate_email(self) -> bool:
        """
        Valida o email do cliente.

        Retorna:
            bool: True se o email for válido, False caso contrário.
        """
        email = self.data.get('email', "")
        regex = r'^\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        return re.match(regex, email) is not None

    def validate_phone(self) -> bool:
        """
        Valida o número de telefone celular do cliente.

        Retorna:
            bool: True se o telefone for válido, False caso contrário.
        """
        phone_number = self.data.get('celular')
        regex = r'^\([1-9]{2}\) (?:[2-8]|9[0-9])[0-9]{3}\-[0-9]{4}$'  
        return re.match(regex, phone_number) is not None

    def validate_cep(self) -> tuple:
        """
        Valida o CEP do cliente usando a API ViaCEP.

        Retorna:
            tuple: Um booleano indicando se o CEP é válido e, se válido, um dicionário com os dados do endereço.
        """
        cep = self.data.get('cep')
        url = f"https://viacep.com.br/ws/{cep}/json/"
        response = requests.get(url)
        if response.status_code == 200:
            dados_cep = response.json()
            if "erro" not in dados_cep:
                return True, dados_cep
        return False, None

    def validar_cliente(self) -> dict:
        """
        Realiza todas as validações de cliente.

        Retorna:
            dict: Um dicionário com os resultados de cada validação.
        """
        resultado = {
            'cpf_validation': self.validate_cpf(),
            'name_validation': self.validate_name(),
            'birthdate_and_age_validation': self.validate_birthday_and_age(),
            'email_validation': self.validate_email(),
            'phone_validation': self.validate_phone(),
            'cep_validation': self.validate_cep()[0]
        }
        return resultado

    def pipeline(self) -> dict:
        """
        Executa o pipeline de validação do cliente.

        Retorna:
            dict: O resultado das validações.
        """
        validacoes = self.validar_cliente()
        return validacoes

    def __call__(self) -> dict: 
        """
        Torna a instância da classe chamável para realizar a validação do cliente.

        Retorna:
            dict: O resultado das validações do pipeline.
        """
        return self.pipeline()

class ValidatorService:
    """
    Classe responsável por normalizar os dados e validar os clientes.

    Atributos:
        dataframe (pd.DataFrame): O DataFrame contendo os dados dos clientes.
    """
    
    def __init__(self, dataframe) -> None:
        self.dataframe = dataframe
    
    def normalize_dataframe(self) -> pd.DataFrame:
        """
        Normaliza o DataFrame renomeando as colunas de acordo com o formato esperado.

        Retorna:
            pd.DataFrame: O DataFrame normalizado.
        """
        columns = {
            "NOME": "nome",
            "CPF": "cpf",
            "Data de Nascimento": "data_nasc",
            "Email": "email",
            "CEP": "cep",
            "Endereço": "endereco",
            "Numero": "numero",
            "Bairro": "bairro",
            "Cidade": "cidade",
            "Estado": "estado",
            "Telefone": "celular",
            "RA": "ra",
            "Curso": "curso",
            "Faculdade": "faculdade"
        }
        normalized_dataframe = self.dataframe.rename(columns=columns)
        return normalized_dataframe
    
    def create_valid_and_detached_list(self, dataframe: pd.DataFrame) -> Any:
        """
        Valida as linhas do DataFrame e cria listas de clientes válidos e rejeitados.

        Args:
            dataframe (pd.DataFrame): O DataFrame a ser validado.

        Retorna:
            list: Duas listas, uma com os clientes válidos e outra com os rejeitados.
        """
        valid_list = []
        detached_list = []

        for _, line in dataframe.iterrows():
            l = line.to_dict()
            
            row_validator = RowValidator(l)
            is_valid = row_validator()
            
            if all(condition == True for condition in is_valid.values()):
                valid_list.append(l)
            else: 
                detach_reasons = [key for key, value in is_valid.items() if not value]
                l["detach_reason"] = detach_reasons
                detached_list.append(l)
                
        complete_lists = [valid_list, detached_list]
        return complete_lists
    
    def pipeline(self) -> Any:
        """
        Executa o pipeline de normalização e validação.

        Retorna:
            Any: Duas listas de clientes, uma válida e outra rejeitada.
        """
        df = self.normalize_dataframe()
        complete_lists = self.create_valid_and_detached_list(dataframe=df)
        return complete_lists
    
    def __call__(self) -> Any:
        """
        Torna a instância da classe chamável para executar o pipeline.

        Retorna:
            Any: Duas listas de clientes, uma válida e outra rejeitada.
        """
        return self.pipeline()

class UpdateOrDuplicatedChecker:
    """
    Classe responsável por verificar se há necessidade de atualização ou duplicação de clientes no sistema.

    Atributos:
        lists (list): Duas listas de clientes, uma válida e outra rejeitada.
        df_system (pd.DataFrame): O DataFrame contendo os clientes já cadastrados no sistema.
    """
    
    def __init__(self, lists: list, df_system: pd.DataFrame) -> None:
        self.valid, self.detached = lists
        self.df_system = df_system
    
    def check_update_needed(self):
        """
        Verifica se os clientes válidos precisam ser atualizados no sistema ou inseridos como novos.

        Retorna:
            list: Dois DataFrames, um com os clientes válidos e outro com os rejeitados após a verificação de duplicação.
        """
        df_detached = pd.DataFrame(self.detached)
        df_valid = pd.DataFrame(self.valid)
        df_system = self.df_system.copy()
        df_valid['cpf'] = df_valid['cpf'].str.replace(r'\D', '', regex=True)
        df_system['cpf'] = df_system['cpf'].str.replace(r'\D', '', regex=True)
        
        common_cpf = set(df_valid['cpf']).intersection(set(df_system['cpf']))
        removed_from_valid = []
        
        for cpf in common_cpf:
            df_valid_row = df_valid[df_valid['cpf'] == cpf].drop(columns='cpf').reset_index(drop=True)
            df_system_row = df_system[df_system['cpf'] == cpf].drop(columns='cpf').reset_index(drop=True)
            
            common_cols = df_valid_row.columns.intersection(df_system_row.columns)
            df_valid_row_common = df_valid_row[common_cols]
            df_system_row_common = df_system_row[common_cols]
            
            if df_valid_row_common.equals(df_system_row_common):
                valid_row_dict = df_valid[df_valid['cpf'] == cpf].to_dict('records')[0]
                valid_row_dict['detach_reason'] = ['update_validation']
                removed_from_valid.append(valid_row_dict)
                df_valid = df_valid[df_valid['cpf'] != cpf]
            else:
                df_valid.loc[df_valid['cpf'] == cpf, 'tipo'] = 'A'
                    
        df_detached = pd.concat([df_detached, pd.DataFrame(removed_from_valid)], ignore_index=True)
        df_valid.loc[~df_valid['cpf'].isin(df_system['cpf']), 'tipo'] = 'I'
        df_detached.reset_index(drop=True, inplace=True)
        
        return [df_valid, df_detached]
    
    def __call__(self) -> Any:
        """
        Torna a instância da classe chamável para verificar a necessidade de atualização ou inserção.

        Retorna:
            Any: Dois DataFrames, um com os clientes válidos e outro com os rejeitados.
        """
        return self.check_update_needed()

def transformar_em_json(df):
    """
    Transforma um DataFrame em uma lista de objetos JSON para upload no sistema.

    Args:
        df (pd.DataFrame): O DataFrame a ser convertido.

    Retorna:
        list: Uma lista de dicionários representando os clientes em formato JSON.
    """
    json_list = []
    for _, row in df.iterrows():
        agrupador = row['faculdade'].lower()  
        ddd, telefone = row['celular'][1:3], row['celular'][5:].replace('-', '')
        
        if isinstance(row['data_nasc'], datetime):
            data_nascimento = row['data_nasc'].strftime('%d/%m/%Y')
        else:
            data_nascimento = row['data_nasc']
        
        json_obj = {
            "id": f"{agrupador}-{row['cpf']}",
            "agrupador": agrupador,
            "tipoPessoa": "FISICA",
            "nome": re.sub(r'\d+','',row['nome'].upper()),
            "cpf": row['cpf'],
            "dataNascimento": data_nascimento,
            "tipo": row['tipo'],
            "enderecos": [
                {
                    "cep": row['cep'],
                    "logradouro": re.sub(r'\d+','',row['endereco'].upper()),
                    "bairro": re.sub(r'\d+','',row['bairro'].upper()),
                    "cidade": re.sub(r'\d+','',row['cidade'].upper()),
                    "numero": str(row['numero']),
                    "uf": re.sub(r'\d+','',row['estado'].upper())
                }
            ],
            "emails": [
                {
                    "email": row['email']
                }
            ],
            "telefones": [
                {
                    "tipo": "CELULAR",
                    "ddd": ddd,
                    "telefone": telefone
                }
            ],
            "informacoesAdicionais": [
                {
                    "campo": "cpf_aluno",
                    "linha": "1",
                    "coluna": "1",
                    "valor": row['cpf']
                },
                {
                    "campo": "registro_aluno",
                    "linha": "1",
                    "coluna": "1",
                    "valor": str(row['ra'])
                },
                {
                    "campo": "nome_aluno",
                    "linha": "1",
                    "coluna": "1",
                    "valor": re.sub(r'\d+','',row['nome'].upper())
                }
            ]
        }
        json_list.append(json_obj)
    return json_list

# Execução do pipeline
get_data = DataService(dados,sistema)
data = get_data()

get_validated_data = ValidatorService(dataframe = data[0])
validated_data = get_validated_data()

update_or_duplicate = UpdateOrDuplicatedChecker(validated_data, data[1])
separated_data = update_or_duplicate()

data_json = transformar_em_json(separated_data[0])
c_time = time.strftime("%Y%m%d")
detached_data_filename = f"dados_descartados-{c_time}.xlsx"
datajson_filename = f"dados-{c_time}.json"
separated_data[1]['data_nasc'] = pd.to_datetime(separated_data[1]['data_nasc'], errors='coerce').dt.strftime('%d/%m/%Y')
detached_data = separated_data[1].to_excel(detached_data_filename)

with open(datajson_filename, 'w', encoding='utf-8') as json_file:
    json.dump(data_json, json_file, ensure_ascii=False, indent=4)