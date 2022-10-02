import pygsheets as gc

def get_single_row(workbook, n):
    return workbook.get_row(n, include_tailing_empty=False)


def get_single_column(workbook, n):
    return workbook.get_col(n, include_tailing_empty=False)


def get_first_column(spreadsheet, workbook, header):
    ws = spreadsheet.worksheet_by_title(workbook)
    data = get_single_column(ws, 1)

    if header:
        return data
    else:
        return data[1:]

def gsheets_df(spreadsheet, n):
    wk = spreadsheet[n]
    return wk.get_as_df(include_tailing_empty_rows=False)


def gsheets_to_json(spreadsheet):
    spreadsheet.get_as_df().to_json()

