import time
import googlemaps
import pandas as pd
from tkinter import filedialog
from pathlib import Path
from datetime import datetime, timedelta
from pandas import ExcelWriter
from sqlalchemy import create_engine, text

oracle_conn = 'oracle+cx_oracle://USER_ID:PSW@HOST/DB'
api_key = "token"
gmaps = googlemaps.Client(key=api_key)
df = []
valor_pago = 0
total_km = 0

pd.set_option('mode.chained_assignment', None)  # to remove it after some updates


def main():
    global oracle_conn
    global gmaps
    global df
    global valor_pago
    global total_km

    df_exp = pd.DataFrame()
    max_dist = 0
    total_km = 0
    print('Bem-vindo ao nosso programa de cálculo de combustível e análise de rota para vendedores e promotores.')

    while True:
        while True:  # Starting project inserting dates
            data_inicial = input('\nPara iniciarmos, por favor digite a data inicial no formato '
                                 'dd/mm/aaaa (ou digite "sair" para encerrar): ').strip()

            if data_inicial.lower() == 'sair':
                print('Encerrando o programa.')
                exit()

            try:
                datetime.strptime(data_inicial, '%d/%m/%Y')
                break
            except ValueError:
                print('Formato de data inválido. ')

        while True:
            data_final = input('\nPor gentileza, agora digite a data final no mesmo formato dd/mm/aaaa: ').strip()

            try:
                if datetime.strptime(data_final, '%d/%m/%Y').date() < datetime.strptime(data_inicial,
                                                                                        '%d/%m/%Y').date():
                    print('\nData final menor que a data inicial. Tente novamente.')
                else:
                    break
            except ValueError:
                print('Formato de data inválido. ')

        data_inicio = datetime.strptime(data_inicial, '%d/%m/%Y').date()
        data_fim = datetime.strptime(data_final, '%d/%m/%Y').date()

        delta = timedelta(days=1)  # Define o intervalo de um dia

        # Verificar se a entrada é um número válido

        rca = input('\nAgora, digite o RCA desejado: ').strip()
        while rca.isdigit() is False:
            print('\nOpção inválida. ')
            rca = input('\nFavor digitar o número do RCA desejado: ').strip()

        print('\nCom base nos cálculos de menor distância, eficiência em trajeto de visita e '
              'excluindo pedidos fora de rota, o vendedor(a) saiu de casa e efetuou pedidos percorrendo:')

        while data_inicio <= data_fim:
            data_formatada = data_inicio.strftime('%d/%m/%Y')

            print(f'\nNo dia {data_formatada}: ')
            # Chamar a função location com os valores fornecidos
            df = location(data_formatada, rca)

            for j in range(len(df)):
                distance_diff = df['distance_diff'].replace('.', ',')[j]
                codcli = df['codcli'][j]
                cliente = df['cliente'][j]
                total_km += distance_diff
                max_dist = df['distance'].replace('.', ',').max()
                if j == 0:
                    print(
                        f"{distance_diff:.3f}".replace('.', ',') + " km até o cliente " + str(codcli)
                        + " - " + cliente)
                else:
                    print(
                        f"{distance_diff:.3f}".replace('.', ',') + " km até o cliente " + str(codcli) +
                        " - " + cliente)
            total_km += max_dist
            if max_dist == 0:
                print(f'Nenhum pedido foi efetuado neste dia.')
            else:
                print("e " + f"{max_dist:.3f}".replace('.', ',') + " km retornando para sua residência")
                print("\nTotalizando " + f"{total_km:.3f}".replace('.', ',') + " Km")

            data_inicio += delta
            max_dist = 0
            df_exp = pd.concat([df_exp, df], axis=0, ignore_index=True)

        if total_km > 0:
            valor_pago = 0
            calc = input('\nVocê deseja calcular o valor do combustível? S/N: ').lower().strip()
            while calc != 's' and calc != 'n':
                print('Opção inválida. Por favor, digite "S" para Sim ou "N" para Não.')
                calc = input('Você deseja calcular o valor do combustível? S/N: ').lower().strip()

            if calc == 's':
                while True:
                    combust = input('Qual o valor do combustível por km a ser usado? ').strip()

                    # Verificar se a entrada é um número válido
                    if combust.replace('.', '', 1).isdigit() or combust.replace(',', '',
                                                                                1).isdigit():
                        # Remover apenas o primeiro ponto (caso exista)
                        valor_pago = total_km * float(combust.replace(',', '.'))
                        print(f'\nO valor a ser pago ao vendedor é de R$ {valor_pago:.2f}'.replace('.',
                                                                                                   ','))
                        break
                    else:
                        print("Entrada inválida. Digite um valor numérico para o combustível.")

            exp = input('\nVocê deseja exportar os dados para arquivo Excel? S/N: ').lower().strip()
            while exp != 's' and exp != 'n':
                print('\nOpção inválida. Por favor, digite "S" para Sim ou "N" para Não.')
                exp = input('\nVocê deseja exportar os dados para arquivo Excel? S/N: ').lower().strip()

            if exp == 's':
                export_results_to_excel(df_exp, calc)

            reiniciar = input('\nDeseja reiniciar o processo com uma nova data e/ou novo RCA? S/N: ').lower().strip()
            while reiniciar != 's' and reiniciar != 'n':
                print('\nOpção inválida. Por favor, digite "S" para Sim ou "N" para Não.')
                reiniciar = input('\nDeseja reiniciar o processo com uma nova data ou novo RCA? S/N: ').lower().strip()

            if reiniciar == 's':
                total_km = 0
                max_dist = 0
                df = []
                df_exp = pd.DataFrame()
                continue
            else:
                print('\nEncerrando o programa.')
                time.sleep(2)
                break


def process_route(origem, destino):  # def where the distance is calculated
    now = datetime.now()
    directions_result = gmaps.directions(origem, destino, departure_time=now)
    if origem == destino:
        distance = 0
    else:
        distance = (directions_result[0]['legs'][0]['distance']['value']) / 1000
    return distance


def export_results_to_excel(dados, calculo):  # def where all results are saved in Excel file
    global total_km
    dados.loc[0, 'total_km'] = round(total_km, 3)
    if calculo == 's':
        dados.loc[0, 'Valor a pagar'] = round(valor_pago, 2)
        col_order = [
            "dtatend", "diasemana", "agendaev", "codusur", "codsupervisor", "codcli", "cliente",
            "bairroent", "municent", "distance", "distance_diff", "total_km", "Valor a pagar"
        ]

    else:
        col_order = [
            "dtatend", "diasemana", "agendaev", "codusur", "codsupervisor", "codcli", "cliente",
            "bairroent", "municent", "distance", "distance_diff", "total_km"
        ]
    dados.drop(['latitude', 'longitude'], axis=1, inplace=True)

    # Sorting columns
    dados = dados[col_order]

    # Changing date format to 'dd/mm/yyyy'
    dados['dtatend'] = pd.to_datetime(dados['dtatend']).dt.strftime('%d/%m/%Y')

    # Renaming columns
    dados.rename(columns={"dtatend": "Data", "diasemana": "Dia Semana", "agendaev": "Agenda", "codusur": "RCA",
                          "codsupervisor": "Supervisor", "codcli": "Cód Cliente", "cliente": "Cliente",
                          "bairroent": "Bairro", "municent": "Município", "distance": "Distância Casa-Clie",
                          "distance_diff": "Difer Distância", "total_km": "Total KM"}, inplace=True)

    filename = filedialog.asksaveasfilename(defaultextension='.xlsx')

    # Setting df into Excel file
    if filename:
        with ExcelWriter(filename, engine='xlsxwriter') as writer:
            dados.style.set_properties(**{'text-align': 'center'}).to_excel(writer, sheet_name='Contents', index=False)
            worksheet = writer.sheets['Contents']
            for col in range(13):
                if col == 6:  # Setting columns size
                    worksheet.set_column(col, col, 55)
                else:
                    worksheet.set_column(col, col, 18)
    print("Arquivo salvo com sucesso em:", filename)


def location(dat, usuar):  # def where all datas is obtained from database
    usur = ("SELECT "
            "U.CODUSUR, "
            "E.NOME, "
            "E.ENDERECO, "
            "E.BAIRRO, "
            "E.CIDADE, "
            "E.ESTADO, "
            "U.CODSUPERVISOR "

            "FROM "
            "PCEMPR E, "
            "PCUSUARI U "

            "WHERE "
            "U.NOME = E.NOME "
            "AND U.CODUSUR = :rca")

    rota = ("SELECT "
            "D.DTATEND, "
            "D.AGENDAEV, "
            "U.CODUSUR, "
            "U.CODSUPERVISOR, "
            "D.DIASEMANA, "
            "D.TIPOEVENT, "
            "D.JUSTIFIC, "
            "C.CODCLI, "
            "C.CLIENTE, "
            "C.BAIRROENT, "
            "C.MUNICENT, "
            "D.LOCALEVENT, "
            "C.LATITUDE, "
            "C.LONGITUDE "

            "FROM "
            "C2DESLOC D "
            "LEFT JOIN PCCLIENT C ON D.CODCLI = C.CODCLI "
            "LEFT JOIN PCUSUARI U ON D.CODUSUR = U.CODUSUR "

            "WHERE "
            "D.TIPOEVENT IN ('PEDIDO', 'JUSTIFICATIVA') "
            "AND D.CODUSUR = :rca AND D.DTATEND = :data_formatada "
            "AND ((CASE WHEN INSTR(D.LOCALEVENT, 'km') > 0 THEN "
            "   TO_NUMBER(REGEXP_SUBSTR(D.LOCALEVENT, '\\d+(\\,\\d+)?')) "
            "WHEN INSTR(D.LOCALEVENT, 'm') > 0 THEN "
            "   TO_NUMBER(REGEXP_SUBSTR(D.LOCALEVENT, '\\d+(\\,\\d+)?')) / 1000 "
            "ELSE NULL END) <= 5 OR D.LOCALEVENT = 'Dentro do Cliente')")

    ilha = "SELECT CODCLI FROM PCCLIENT WHERE BAIRROENT = 'ABRAAO' AND MUNICENT = 'ANGRA DOS REIS'"

    # Create dataframes from database
    with create_engine(oracle_conn).begin() as conn:
        df_rca = pd.DataFrame(pd.read_sql_query(sql=text(usur), con=conn, params={"rca": usuar}))
        df_rota = pd.DataFrame(pd.read_sql_query(sql=text(rota), con=conn, params={'rca': usuar,
                                                                                   'data_formatada': dat}))
        df_ilhagrande = pd.DataFrame(pd.read_sql_query(sql=text(ilha), con=conn))

    origem = ', '.join(df_rca.iloc[:, 2:5].astype(str).values.flatten())
    i = 0

    ilhagrande_latitude = '-23,031453'
    ilhagrande_longitude = '-44,161124'

    df_rota.loc[df_rota['codcli'].isin(df_ilhagrande['codcli']), 'latitude'] = ilhagrande_latitude
    df_rota.loc[df_rota['codcli'].isin(df_ilhagrande['codcli']), 'longitude'] = ilhagrande_longitude

    while i < int(df_rota.shape[0]):  # Solving problems with latitude or longitude with null values
        if df_rota.loc[i, 'latitude'] is None:
            df_rota.loc[i, 'latitude'] = '0'
        if df_rota.loc[i, 'longitude'] is None:
            df_rota.loc[i, 'longitude'] = '0'
        lat = df_rota.loc[i, 'latitude'].replace(',', '.')
        lon = df_rota.loc[i, 'longitude'].replace(',', '.')

        destino = f"{lat}, {lon}"
        if destino == '0, 0':
            destino = origem

        km = process_route(origem, destino)

        df_rota.loc[i, 'distance'] = km
        df_rota = df_rota.sort_values('distance')
        df_rota['distance_diff'] = df_rota['distance'].diff()
        df_rota['distance_diff'].fillna(df_rota['distance'].iloc[0], inplace=True)
        df_rota.reset_index(drop=True, inplace=True)

        i += 1
    return df_rota


if __name__ == "__main__":
    main()
