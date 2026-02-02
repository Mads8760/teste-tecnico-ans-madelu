import pandas as pd
import glob
import os
import zipfile
import sqlite3

PASTA_ORIGEM = "dados_extraidos"
ARQUIVO_FINAL = "consolidado_despesas.csv"

def consolidar_dados():
    print("🚀 Iniciando a consolidação...")
    lista_de_tabelas = []
    arquivos = glob.glob(os.path.join(PASTA_ORIGEM, "*"))

    for arquivo in arquivos:
        try:
            df = pd.read_csv(arquivo, sep=None, engine='python', encoding='latin1')
            mapeamento = {
                'VL_SALDO_FINAL': 'Valor Despesas',
                'REG_ANS': 'RegistroANS',
                'DATA': 'Data'
            }
            df.rename(columns=mapeamento, inplace=True)

            
            if 'Valor Despesas' in df.columns:
                lista_de_tabelas.append(df)
            else:
                print(f"⚠️ Aviso: Não achei a coluna de valor no arquivo {arquivo}")

        except Exception as e:
            print(f"❌ Erro ao ler {arquivo}: {e}")

    if lista_de_tabelas:
        df_consolidado = pd.concat(lista_de_tabelas, ignore_index=True)
        
        
        df_consolidado['Valor Despesas'] = df_consolidado['Valor Despesas'].astype(str).str.replace(',', '.')
        df_consolidado['Valor Despesas'] = pd.to_numeric(df_consolidado['Valor Despesas'], errors='coerce')
        
        # Remover negativos e zeros 
        df_consolidado = df_consolidado[df_consolidado['Valor Despesas'] > 0]
        
        df_consolidado.to_csv('consolidado_despesas.csv', index=False, encoding='utf-8')
        print("✅ Sucesso! O arquivo consolidado nasceu.")


def zipar_resultado():
    with zipfile.ZipFile('consolidado_despesas.zip', 'w') as zipf:
        zipf.write('consolidado_despesas.csv')
    print("📦 Arquivo ZIP consolidado gerado com sucesso!")

def zipar_resultado_etapa_1():
    nome_csv = 'consolidado_despesas.csv'
    nome_zip = 'consolidado_despesas.zip'
    
   
    if os.path.exists(nome_csv):
        with zipfile.ZipFile(nome_zip, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(nome_csv)
        print(f"📦 Sucesso! Arquivo {nome_zip} gerado para entrega.")
    else:
        print("⚠️ Erro: O CSV consolidado não foi encontrado para ser zipado.")


if __name__ == "__main__":
    consolidar_dados()
    zipar_resultado_etapa_1()


def validar_e_agregar():
    print("🚀 Iniciando Enriquecimento e Agregação...")
    
    # 1. Carregando os dados
    df_despesas = pd.read_csv('consolidado_despesas.csv')
    df_cadastro = pd.read_csv('dados_cadastrais/Relatorio_cadop.csv', sep=';', encoding='latin1')

    # 2. Padronizando o Cadastro
    df_cadastro.rename(columns={'REGISTRO_OPERADORA': 'RegistroANS', 'CNPJ': 'CNPJ_Real'}, inplace=True)

    # 3. O JOIN (Tarefa 2.2)
    # Uni as despesas com o cadastro usando o Registro da ANS 
    df_enriquecido = pd.merge(df_despesas, df_cadastro, on='RegistroANS', how='left')

    # 4. AGREGAÇÃO (Tarefa 2.3)
    # Agrupei por Razão Social e UF para calcular as estatísticas 
    df_agregado = df_enriquecido.groupby(['Razao_Social', 'UF']).agg({
        'Valor Despesas': ['sum', 'mean', 'std'] # Total, Média e Desvio Padrão 
    }).reset_index()

    # Ajustando os nomes das colunas novas
    df_agregado.columns = ['RazaoSocial', 'UF', 'Total_Despesas', 'Media_Despesas', 'Desvio_Padrao']

    # 5. Ordenação (Trade-off: maior para o menor) 
    df_agregado = df_agregado.sort_values(by='Total_Despesas', ascending=False)

    # 6. Salvando (Tarefa 2.3) 
    df_agregado.to_csv('despesas_agregadas.csv', index=False, encoding='utf-8')
    print("✅ Sucesso! Arquivo despesas_agregadas.csv gerado.")

if __name__ == "__main__":
    validar_e_agregar()

def carregar_no_banco():
    print("🗄️ Iniciando o carregamento no banco de dados SQL...")
    
    if not os.path.exists('despesas_agregadas.csv'):
        print("❌ Erro: O arquivo de despesas agregadas não existe.")
        return

    df = pd.read_csv('despesas_agregadas.csv')

    
    conexao = sqlite3.connect('teste_ans_madelu.db')
    
    df.to_sql('estatisticas_despesas', conexao, index=False, if_exists='replace')

    print("✅ Sucesso! Dados salvos no arquivo: teste_ans_madelu.db")
    
    conexao.close()

if __name__ == "__main__":
    carregar_no_banco()
