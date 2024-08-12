import requests
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
from datetime import datetime, timedelta, timezone

# Конфигурация
TENANT_ID = 'your_tenant'
CLIENT_ID = 'ea0616ba-638b-4df5-95b9-636659ae5121'  
USERNAME = 'your_username'
PASSWORD = 'you_pass'

mx_dwh_engine = create_engine('mssql+pyodbc://log:pass@ip/DWH?driver=ODBC Driver 17 for SQL Server') 
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=ip;DATABASE=database;UID=log;PWD=pass')
cursor = conn.cursor()

# Получение токена доступа
def get_access_token(tenant_id, client_id, username, password):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'password',
        'client_id': client_id,
        'username': username,
        'password': password,
        'scope': 'https://analysis.windows.net/powerbi/api/.default'
    }
    
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()['access_token']

# Запрос данных активности за прошлый час
def get_activity_for_last_full_hour(token):
    # Определяем начало и конец прошлого часа
    now = datetime.now(timezone.utc)
    end_datetime = now.replace(minute=0, second=0, microsecond=0)
    start_datetime = end_datetime - timedelta(hours=1)
    
    url = "https://api.powerbi.com/v1.0/myorg/admin/activityevents"
    
    # Параметры запроса
    params = {
        "startDateTime": start_datetime.strftime("'%Y-%m-%dT%H:%M:%SZ'"),
        "endDateTime": end_datetime.strftime("'%Y-%m-%dT%H:%M:%SZ'"),
        "$filter": "Activity eq 'ViewReport'"
    }
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get('activityEventEntities', [])

# Главная функция
def main():
    # Получаем токен доступа
    token = get_access_token(TENANT_ID, CLIENT_ID, USERNAME, PASSWORD)
    
    # Получаем данные активности за прошлый полный час
    activities = get_activity_for_last_full_hour(token)

    # Преобразуем данные в DataFrame и оставляем только нужные столбцы
    df = pd.DataFrame(activities)
    df = df[['UserId', 'WorkSpaceName', 'ArtifactName', 'CreationTime']]
    df.rename(columns={'CreationTime': 'ActivityTime'}, inplace=True)

    # Выводим DataFrame
    print(df)
    for index, row in df.iterrows():
        sql = f"INSERT INTO [DWH].[py].[activity_log] values (N'{row['UserId']}', N'{row.WorkSpaceName}', N'{row.ArtifactName}' ,'{row['ActivityTime']}')"
        cursor.execute(sql)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
