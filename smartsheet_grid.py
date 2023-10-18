#!/usr/bin/env python

import smartsheet, pandas as pd

class grid:

    """
    Global Variable
    ____________
    token --> MUST BE SET BEFORE PROCEEDING. >>> grid.token = {SMARTSHEET_ACCES_TOKEN}

    Dependencies
    ------------
    smartsheet as smart (smartsheet-python-sdk)
    pandas as pd

    Attributes
    __________
    grid_id: int
        sheet id of an existing Smartsheet sheet. terst 1

    Methods
    -------
    grid_id --> returns the grid_id
    grid_content ---> returns the content of a sheet as a dictionary.
    grid_columns ---> returns a list of the column names.
    grid_rows ---> returns a list of lists. each sub-list contains all the 'display values' of each cell in that row.
    grid_row_ids---> returns a list o
    f all the row ids
    grid_column_ids ---> returns a list of all the column ids
    df ---> returns a pandas DataFrame of the sheet.
    delete_all_rows ---> deletes all rows in the sheet (in preperation for updating).

    """

    token = None

    def __init__(self, grid_id):
        self.grid_id = grid_id
        self.grid_content = None
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            self.smart = smartsheet.Smartsheet(access_token=self.token)
            self.smart.errors_as_exceptions(True)
    
    def get_column_df(self):
        '''returns a df with data on the columns: title, type, options, etc...'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            return pd.DataFrame.from_dict(
                (self.smart.Sheets.get_columns(
                    self.grid_id, 
                    level=2, 
                    include='objectValue', 
                    include_all=True)
                ).to_dict().get("data"))

    def df_id_by_col(self, column_names):
        '''I never use, name sounds like it returns an id per column'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            columnids = []
            col_index = []
            for col in column_names:
                col1 = smart.Sheets.get_column_by_title(self.grid_id, col)
                columnids.append(col1.to_dict().get("id"))
                col_index.append(col1.to_dict().get("index"))
            sorted_col = [x for y, x in sorted(zip(col_index, column_names))]
            sfetch = self.smart.Sheets.get_sheet(self.grid_id, column_ids=columnids)
            cols = ["id"] + sorted_col
            c = []
            p = sfetch.to_dict()
            for i in p.get("rows"):
                l = []
                l.append(i.get("id"))
                for i in i.get("cells"):
                    l.append(i.get("displayValue"))
                c.append(l)
            return pd.DataFrame(c, columns=cols)

    def fetch_content(self):
        '''this fetches data, ask coby why this is seperated
        when this is done, there are now new objects created for various scenarios-- column_ids, row_ids, and the main sheet df'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            self.grid_content = (self.smart.Sheets.get_sheet(self.grid_id)).to_dict()
            self.grid_name = (self.grid_content).get("name")
            self.grid_url = (self.grid_content).get("permalink")
            # this attributes pulls the column headers
            self.grid_columns = [i.get("title") for i in (self.grid_content).get("columns")]
            # note that the grid_rows is equivelant to the cell's 'Display Value'
            self.grid_rows = []
            if (self.grid_content).get("rows") == None:
                self.grid_rows = []
            else:
                for i in (self.grid_content).get("rows"):
                    b = i.get("cells")
                    c = []
                    for i in b:
                        l = i.get("displayValue")
                        m = i.get("value")
                        if l == None:
                            c.append(m)
                        else:
                            c.append(l)
                    (self.grid_rows).append(c)
            
            # resulting fetched content
            self.grid_rows = self.grid_rows
            if (self.grid_content).get("rows") == None:
                self.grid_row_ids = []
            else:
                self.grid_row_ids = [i.get("id") for i in (self.grid_content).get("rows")]
            self.grid_column_ids = [i.get("id") for i in (self.grid_content).get("columns")]
            self.df = pd.DataFrame(self.grid_rows, columns=self.grid_columns)
            self.df["id"]=self.grid_row_ids
            self.column_df = self.get_column_df()

    def fetch_summary_content(self):
        '''builds the summary df for summary columns'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            self.grid_content = (self.smart.Sheets.get_sheet_summary_fields(self.grid_id)).to_dict()
            # this attributes pulls the column headers
            self.summary_params=['title','createdAt', 'createdBy', 'displayValue', 'formula', 'id', 'index', 'locked', 'lockedForUser', 'modifiedAt', 'modifiedBy', 'objectValue', 'type']
            self.grid_rows = []
            if (self.grid_content).get("data") == None:
                self.grid_rows = []
            else:
                for summary_field in (self.grid_content).get("data"):
                    row = []
                    for param in self.summary_params:
                        row_value = summary_field.get(param)
                        row.append(row_value)
                    self.grid_rows.append(row)
            if (self.grid_content).get("rows") == None:
                self.grid_row_ids = []
            else:
                self.grid_row_ids = [i.get("id") for i in (self.grid_content).get("data")]
            self.df = pd.DataFrame(self.grid_rows, columns=self.summary_params)
    
    def reduce_columns(self,exclusion_string):
        """a method on a grid{sheet_id}) object
        take in symbols/characters, reduces the columns in df that contain those symbols"""
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            regex_string = f'[{exclusion_string}]'
            self.column_reduction =  self.column_df[self.column_df['title'].str.contains(regex_string,regex=True)==False]
            self.reduced_column_ids = list(self.column_reduction.id)
            self.reduced_column_names = list(self.column_reduction.title)

    def prep_post(self, filtered_column_title_list="all_columns"):
        '''preps for ss post 
        creating a dictionary per column:
        { <title of column> : <column id> }
        filtered column title list is a list of column title str to prep for posting (if you are not posting to all columns)
        [NOT USED INDEPENDENTLY, BUT USED INSIDE OF POST_NEW_ROWS]'''

        column_df = self.get_column_df()

        if filtered_column_title_list == "all_columns":
            filtered_column_title_list = column_df['title'].tolist()
    
        self.column_id_dict = {title: column_df.loc[column_df['title'] == title]['id'].tolist()[0] for title in filtered_column_title_list}
    
    def delete_all_rows(self):
        '''deletes up to 400 rows in 200 row chunks by grabbing row ids and deleting them one at a time in a for loop
        [NOT USED INDEPENDENTLY, BUT USED INSIDE OF POST_NEW_ROWS]'''
        self.fetch_content()

        row_list_del = []
        for rowid in self.df['id'].to_list():
            row_list_del.append(rowid)
            # Delete rows to sheet by chunks of 200
            if len(row_list_del) > 199:
                self.smart.Sheets.delete_rows(self.grid_id, row_list_del)
                row_list_del = []
        # Delete remaining rows
        if len(row_list_del) > 0:
            self.smart.Sheets.delete_rows(self.grid_id, row_list_del)
    
    def post_new_rows(self, posting_data, post_fresh = False, post_to_top=False):
        '''posts new row to sheet, does not account for various column types at the moment
        posting data is a list of dictionaries, one per row, where the key is the name of the column, and the value is the value you want to post
        then this function creates a second dictionary holding each column's id, and then posts the data one dictionary at a time (each is a row)
        post_to_top = the new row will appear on top, else it will appear on bottom
        post_fresh = first delete the whole sheet, then post (else it will just update existing sheet)
        TODO: if using post_to_top==False, I should really delete the empty rows in the sheet so it will properly post to bottom'''
        
        posting_sheet_id = self.grid_id
        column_title_list = list(posting_data[0].keys())
        self.prep_post(column_title_list)
        if post_fresh:
            self.delete_all_rows()
        
        rows = []

        for item in posting_data:
            row = smartsheet.models.Row()
            row.to_top = post_to_top
            row.to_bottom= not(post_to_top)
            for key in self.column_id_dict:
                if item.get(key) != None:     
                    row.cells.append({
                    'column_id': self.column_id_dict[key],
                    'value': item[key]
                    })
            rows.append(row)

        self.post_response = self.smart.Sheets.add_rows(posting_sheet_id, rows)