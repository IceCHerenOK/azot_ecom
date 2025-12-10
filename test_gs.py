import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
gc = gspread.authorize(creds)

sheet_id = "1Gbf7CCWVn2Lwi3O3PTzEqJBC5a2AMPKCgQYWF16MqV0"
ws = gc.open_by_key(sheet_id).worksheet("Юнит экономика оз")

# читаем первую строку
print(ws.row_values(1))

