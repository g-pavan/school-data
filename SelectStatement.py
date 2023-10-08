import pandas as pd

class SelectStatement:
    def __init__(self, dataframe):
        self.df = dataframe
        self.selected_columns = None
        self.conditions = None
        self.order_by = None
        self.limit = None
        self.join_type = None
        self.other_df = None
        self.on_columns = None

    def select(self, *columns):
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):
            columns = columns[0]
        elif len(columns) == 1 and isinstance(columns[0], pd.Index):
            columns = columns[0].tolist()

        self.selected_columns = columns
        return self

    def where(self, condition):
        self.conditions = condition
        return self

    def order_by_column(self, column, ascending=True):
        self.order_by = (column, ascending)
        return self

    def limit_rows(self, limit):
        self.limit = limit
        return self

    def join(self, other_df, on_columns, how='inner'):
        self.other_df = other_df
        self.on_columns = on_columns
        self.join_type = how
        return self

    def execute(self):
        result_df = self.df.copy()

        if not self.conditions.empty:
            result_df = result_df.loc[self.conditions]
            self.conditions = None

        if self.order_by:
            column, ascending = self.order_by
            result_df = result_df.sort_values(by=column, ascending=ascending)
            self.order_by = None

        if self.limit:
            result_df = result_df.head(self.limit)
            self.limit = None

        if self.on_columns and not self.other_df.empty:
            result_df = result_df.merge(self.other_df, on=self.on_columns, how=self.join_type)
            self.other_df = None
            self.on_columns = None
        
        if self.selected_columns:
            result_df = result_df[list(self.selected_columns)]
            self.selected_columns = None

        return result_df
