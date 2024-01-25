import os
import time
import gspread
import schedule
from oauth2client.service_account import ServiceAccountCredentials
from dateutil.parser import parse
from datetime import datetime, timedelta
from googleapiclient import discovery
from dotenv import load_dotenv


load_dotenv()

# SET UP GOOGLE SPREADSHEET
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name('creds_service_acc.json', SCOPES)
client = gspread.authorize(CREDENTIALS)


# Open the spreadsheet using the correct spreadsheet key
spreadsheet = client.open_by_key(SPREADSHEET_ID)

# index of table (123)
worksheet_index = 0


def create_request(worksheet, date_column_index, first_part_date, new_date) -> dict:
    """
    This function uses to create a request worksheet

    :param worksheet:
    :param date_column_index:
    :param first_part_date:
    :param new_date:
    :return: dict
    """
    request_to_sheet = {
                "updateCells": {
                    "rows": [
                        {
                            "values": [
                                {
                                    "userEnteredValue": {
                                        "stringValue": f"{first_part_date.date().strftime('%d.%m.%Y')} - {new_date.strftime('%d.%m.%Y')}"
                                    }
                                },
                            ]
                        }
                    ],
                    "fields": "userEnteredValue",
                    "start": {"sheetId": worksheet.id, "rowIndex": 1,
                              "columnIndex": date_column_index - 1},
                }
            }
    return request_to_sheet


def update_dates():

    """
    This function runs every 24 hours and check if promotion date is expired. If it is,
    the function changes G and L cols in a way of adding 5 days to today date and continues the
    promotion
    :return: None
    """

    try:
        # Get the specified worksheet
        worksheet = spreadsheet.get_worksheet(worksheet_index)

        # Get the value in G2
        g2_value = worksheet.cell(2, 7).value

        first_part, second_part = g2_value.split(' - ')
        first_part_date = parse(first_part.strip())
        second_part_date = parse(second_part.strip())

        #print(f'{first_part_date} - {second_part_date}')
        #print(f'The value in G2 is: {g2_value}')

        # Counting records in M column
        all_values = worksheet.col_values(13)

        # Find the number of rows
        num_rows = len(all_values) - 1
        print(f'Number of rows: {num_rows}')

        date_column_index_g = 7
        date_column_index_l = 12

        if second_part_date.date() < datetime.today().date():
            new_date = datetime.today() + timedelta(days=5)

            # Construct the update request for both columns G and L
            update_request_g = create_request(worksheet, date_column_index_g, first_part_date, new_date)
            update_request_l = create_request(worksheet, date_column_index_l, first_part_date, new_date)

            # Duplicate the update request for the remaining rows
            update_request_g["updateCells"]["rows"] *= num_rows
            update_request_l["updateCells"]["rows"] *= num_rows

            # Execute the batch update
            body = {"requests": [update_request_g, update_request_l]}
            service = discovery.build('sheets', 'v4', credentials=CREDENTIALS)
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet.id, body=body).execute()

            print("Dates updated successfully.")

        else:
            print("Date in G2 is greater than today's date. Skipping update.")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    schedule.every(24).hours.do(update_dates)

    # Run the loop to continuously check the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)
