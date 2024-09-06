Resolução do Problema: Validação e Inserção de Clientes no Sistema
Contexto
Este exercício envolve a inserção de novos clientes em um sistema a partir de um arquivo JSON. Antes de realizar a inserção, várias validações precisam ser feitas, incluindo CPF, nome completo, data de nascimento (verificação de idade mínima), email, telefone e CEP. Também é necessário verificar se o cliente já existe no sistema e, caso positivo, atualizar suas informações. Ao final, o resultado esperado inclui um arquivo JSON para os clientes aprovados, um relatório em Excel com os clientes rejeitados e suas razões, e o código em Python para realizar esse processo.

Etapas de Resolução
1. Leitura dos Dados
Utilizamos a classe DataService para ler dois arquivos principais:

dados.xlsx: contendo as informações dos novos clientes.
sistema.xlsx: contendo os clientes já cadastrados no sistema.
A função get_data() lê ambos os arquivos e retorna dois DataFrames, um para cada fonte de dados.

2. Validação dos Dados dos Clientes
A classe RowValidator foi implementada para realizar as validações de cada linha no DataFrame de clientes. Essas validações incluem:

CPF: Usamos expressões regulares para validar o formato e aplicamos a lógica de verificação de dígitos.
Nome: Verificamos se o cliente possui nome completo (dois ou mais nomes).
Data de Nascimento: Garantimos que a data de nascimento esteja correta e que o cliente tenha ao menos 17 anos.
Email: Verificamos o formato de email com uma expressão regular.
Telefone: Validamos o formato de telefone com DDD no padrão brasileiro.
CEP: Utilizamos a API do ViaCEP para verificar a existência do CEP e validar o endereço.
3. Normalização dos Dados
A classe ValidatorService foi responsável por normalizar as colunas do DataFrame de acordo com as necessidades do sistema, renomeando as colunas para os formatos exigidos. Essa etapa é crucial para manter a consistência e garantir que as validações e comparações sejam feitas corretamente.

4. Geração de Listas de Clientes Validados e Rejeitados
Após a normalização, a função create_valid_and_detached_list() percorre o DataFrame de clientes. Ela valida cada cliente individualmente utilizando a RowValidator e separa os clientes em duas listas:

valid_list: Clientes que passaram em todas as validações.
detached_list: Clientes que falharam em alguma validação. Além disso, cada cliente rejeitado recebe um campo adicional chamado detach_reason, contendo a lista de razões pelas quais a validação falhou.
5. Verificação de Duplicidade ou Atualização
A classe UpdateOrDuplicatedChecker realiza a verificação entre os clientes validados e os clientes já cadastrados no sistema. Ela faz isso comparando o CPF dos clientes. Quando um cliente já existe no sistema:

Se seus dados são idênticos, ele é movido para a lista de clientes rejeitados com a razão "update_validation".
Se há diferenças, o cliente é marcado para atualização com o tipo "A" (atualização). Clientes não existentes são classificados com o tipo "I" (inserção).
6. Transformação dos Dados em JSON
A função transformar_em_json() transforma os clientes validados (tanto os de inserção quanto os de atualização) em objetos JSON no formato exigido pelo sistema. Esta função constrói um dicionário com as informações pessoais, endereços, emails e telefones dos clientes, pronto para ser inserido no sistema.

7. Geração de Arquivos Finais
Ao final do processo, o código gera dois arquivos:

Um arquivo JSON (dados-{data}.json)com os clientes validados e prontos para serem inseridos ou atualizados no sistema.
Um arquivo Excel (dados_descartados-{data}.xlsx) com os clientes rejeitados e as razões pelas quais não puderam ser aceitos.
Considerações Finais
O código foi escrito para ser modular e de fácil manutenção. As classes responsáveis por cada parte do processo são independentes, o que facilita modificações futuras ou a introdução de novas regras de negócio. Cada validação foi cuidadosamente implementada para garantir a conformidade com as regras fornecidas, assegurando a precisão dos dados antes de subir ao sistema.
