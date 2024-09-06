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
    def __init__(self, dados_path, sistema_path) -> None: 
        self.dados_path =  dados_path
        self.sistema_path = sistema_path
        
    def get_data(self) -> list:
        
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
        return self.get_data()
    
class RowValidator:
    def __init__(self, data):
        self.data = data
        
    def validate_cpf(self):
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

    def validate_name(self):
        name = self.data.get('nome')
        return len(name.split()) >= 2

    def validate_birthday_and_age(self):
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

    def validate_email(self):
        email = self.data.get('email', "")
        regex = r'^\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        return re.match(regex, email) is not None

    def validate_phone(self):
        phone_number = self.data.get('celular')
        regex = r'^\([1-9]{2}\) (?:[2-8]|9[0-9])[0-9]{3}\-[0-9]{4}$'  
        return re.match(regex, phone_number) is not None

    def validate_cep(self):
        cep = self.data.get('cep')
        url = f"https://viacep.com.br/ws/{cep}/json/"
        response = requests.get(url)
        if response.status_code == 200:
            dados_cep = response.json()
            if "erro" not in dados_cep:
                return True, dados_cep
        return False, None

    def validar_cliente(self):
        resultado = {
            'cpf_validation': self.validate_cpf(),
            'name_validation': self.validate_name(),
            'birthdate_and_age_validation': self.validate_birthday_and_age(),
            'email_validation': self.validate_email(),
            'phone_validation': self.validate_phone(),
            'cep_validation': self.validate_cep()[0]
        }
        return resultado

    def pipeline(self):
        validacoes = self.validar_cliente()
        return validacoes

    def __call__(self): 
        return self.pipeline()

class ValidatorService:
    def __init__(self,dataframe) -> None:
        self.dataframe = dataframe
    
    def normalize_dataframe(self) -> pd.DataFrame:
        columns = {"NOME" : "nome",
            "CPF" : "cpf",
            "Data de Nascimento" : "data_nasc",
            "Email" : "email",
            "CEP" : "cep",
            "Endereço" : "endereco",
            "Numero" : "numero",
            "Bairro" : "bairro",
            "Cidade" : "cidade",
            "Estado" : "estado",
            "Telefone" : "celular",
            "RA" : "ra",
            "Curso" : "curso",
            "Faculdade" : "faculdade"
            }
            
        normalized_dataframe = self.dataframe.rename(columns = columns)
        return normalized_dataframe
    
    def create_valid_and_detached_list(self, dataframe: pd.DataFrame) -> Any:
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
                
        print(valid_list)
        print(detached_list)
                
        complete_lists = [valid_list, detached_list]
                
        return complete_lists
    
    def pipeline(self) -> Any:
        df = self.normalize_dataframe()
        complete_lists = self.create_valid_and_detached_list(dataframe= df)
        print(complete_lists)
        return complete_lists
    
    def __call__(self) -> Any:
        return self.pipeline()
        
class UpdateOrDuplicatedChecker:
    def __init__(self, lists : list, df_system : pd.DataFrame) -> None:
        self.valid, self.detached = lists
        self.df_system = df_system
    
    def check_update_needed(self):
        print(self.valid)
        print(self.detached)
        df_detached = pd.DataFrame(self.detached)
        df_valid = pd.DataFrame(self.valid)
        df_system = self.df_system.copy()
        df_valid['cpf'] = df_valid['cpf'].str.replace(r'\D', '', regex=True)
        df_system['cpf'] = df_system['cpf'].str.replace(r'\D', '', regex=True)
        
        common_cpf = set(df_valid['cpf']).intersection(set(df_system['cpf']))
        
        removed_from_valid = []
        
        for cpf in common_cpf:
            df_valid_row = df_valid[df_valid['cpf'] == cpf].drop(columns='cpf').reset_index(drop = True)
            df_system_row = df_system[df_system['cpf'] == cpf].drop(columns='cpf').reset_index(drop = True)
            
            common_cols = df_valid_row.columns.intersection(df_system_row.columns)
            
            print(common_cols)
            
            df_valid_row_common = df_valid_row[common_cols]
            df_system_row_common = df_system_row[common_cols]
            
            print(df_valid_row_common)
            print(df_system_row_common)
            
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
        return self.check_update_needed()
        
def transformar_em_json(df):
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
