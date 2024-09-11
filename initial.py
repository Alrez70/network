import pandas as pd

class DataFrameLoader:
    # Initialize the loader with file paths and flags to control which data to load
    def __init__(self, hour_ending, path, path_row=None, ln=True, sp=True, gn=True, hb=True, ld=True, xf=True, bus=True):
        self.hour_ending_new_format = f"{hour_ending:03d}"  # Format hour ending
        self.path = path  # Path for loading CSVs
        self.path_row = path_row  # Optional path for bus data
        self.bus_data = []  # Store raw bus data

        # Conditionally load and modify dataframes
        if ln:
            self.df_line = self.modify_line_dataframe(self._load_dataframe("Ln"))
        if sp:
            self.df_sp = self.modify_sp_dataframe(self._load_dataframe("Sp"))
        if gn:
            self.df_gen = self.modify_gen_dataframe(self._load_dataframe("Gn"))
        if hb:
            self.df_hub = self.modify_hub_dataframe(self._load_dataframe("Hb"))
        if ld:
            self.df_load = self.modify_load_dataframe(self._load_dataframe("Ld"))
        if xf:
            self.df_tran = self.modify_tran_dataframe(self._load_dataframe("Xf"))
        if bus:
            self.parse_file()  # Load bus data from raw file
            self.df_bus = self._to_bus_dataframe()

    # Parse raw bus data from the file
    def parse_file(self):
        with open(self.path_row, 'r') as file:
            for _ in range(3):
                next(file)
            for line in file:
                if line.startswith(" 0"):
                    break
                self.bus_data.append(line.strip())

    # Convert bus data into a structured DataFrame
    def _to_bus_dataframe(self):
        columns_bus = ["PSS/E Bus Number", "Station Name/PSS/E Bus Name", "PSS/E KV", "IDE", "GL", "BL", "AREA", "ZONE", "VM", "VA", "OWNER"]
        bus_df = pd.DataFrame([line.split() for line in self.bus_data], columns=columns_bus)
        bus_df[["PSS/E Bus Number", "IDE"]] = bus_df[["PSS/E Bus Number", "IDE"]].apply(pd.to_numeric, errors="coerce")
        return bus_df.drop(columns=["GL", "BL", "AREA", "ZONE", "VM", "VA", "OWNER"])

    # Load CSV based on file type (line, generator, etc.)
    def _load_dataframe(self, file_type):
        return pd.read_csv(f"{self.path}_{file_type}_{self.hour_ending_new_format}.csv")

    # Modify and clean up line data
    def modify_line_dataframe(self, df):
        column_line = ["Hour", "PSS/E From Bus Number", "PSS/E To Bus Number", "PSS/E Ckt Id", "Branch Status",
                       "Monitored?", "Monitored and Secured?", "From Station Name/PSS/E Bus Name", "From PSS/E KV",
                       "To Station Name/PSS/E Bus Name", "To PSS/E KV", "Branch Name", "r (p.u)", "x (p.u)",
                       "b (p.u)", "RATEA", "RATEB", "RATEC"]
        df.columns = column_line
        df["SUS"] = 1 / df["x (p.u)"]
        return df.drop(columns=["PSS/E Ckt Id", "Monitored?", "Monitored and Secured?", "r (p.u)", "x (p.u)"])

    # Modify and clean up settlement point data
    def modify_sp_dataframe(self, df):
        column_sp = ["Hour", "Settlement Point Name", "Settlement Point Type", "Status",
                     "Number of energized components", "PSS/E Bus Number", "Station Name/PSS/E Bus Name", "PSS/E KV"]
        df.columns = column_sp
        return df

    # Modify and clean up generator data
    def modify_gen_dataframe(self, df):
        column_gen = ["Hour", "PSS/E Bus Number", "PSS/E Gen Id", "Station Name/PSS/E Bus Name", "PSS/E KV", "Generator Name",
                      "Generator Status", "Resource Node Settlement Point Name", "Resource Node PSS/E Bus Number"]
        df.columns = column_gen
        return df.drop(columns=["PSS/E Gen Id"])

    # Modify and clean up hub data
    def modify_hub_dataframe(self, df):
        column_hub = ["Hour", "PSS/E Bus Number", "Station Name/PSS/E Bus Name", "PSS/E KV", "Bus Status", "Hub Bus Name", "Hub Name"]
        df.columns = column_hub
        return df

    # Modify and clean up load data
    def modify_load_dataframe(self, df):
        column_load = ["Hour", "PSS/E Bus Number", "PSS/E Load Id", "Station Name/PSS/E Bus Name", "PSS/E KV", "Load Name", "Load Status"]
        df.columns = column_load
        return df.drop(columns=["PSS/E Load Id"])

    # Modify and clean up transformer data
    def modify_tran_dataframe(self, df):
        column_trans = ["Hour", "PSS/E From Bus Number", "PSS/E To Bus Number", "PSS/E Ckt Id", "Transformer Status",
                        "Monitored?", "Monitored and Secured?", "From Station Name/PSS/E Bus Name", "From PSS/E KV",
                        "To Station Name/PSS/E Bus Name", "To PSS/E KV", "Branch Name", "r (p.u)", "x (p.u)", "RATEA"]
        df.columns = column_trans
        df["SUS"] = 1 / df["x (p.u)"]
        return df.drop(columns=["PSS/E Ckt Id"])

# Example Usage:
# loader = DataFrameLoader(hour_ending, path, path_row=path_row, ln=True, sp=True, gn=True, hb=True, ld=True, xf=True, bus=True)
# df_line = loader.df_line
# df_gen = loader.df_gen
# df_load = loader.df_load
# df_tran = loader.df_tran
# df_bus = loader.df_bus
