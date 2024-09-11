import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.linalg import inv
from initial import DataFrameLoader

class ShiftFactor:
    def __init__(self, hour_ending, path, path_row, ref_bus_ide=3):
        self.hour_ending = hour_ending
        self.path = path
        self.path_row = path_row
        self.ref_bus_ide = ref_bus_ide  # Reference bus identifier
        self.df_branch_trans = None
        self.adjacency_matrix = None
        self.df_shift_factor = None
        self.unique_bus_list = None
        self.df_bus = None
        self.load_data()  # Load initial data

    def load_data(self):
        # Load line, transformer, and bus data using DataFrameLoader
        loader = DataFrameLoader(self.hour_ending, self.path, path_row=self.path_row, ln=True, xf=True, bus=True)
        df_line = loader.df_line[loader.df_line["Branch Status"] == "In-Service"]
        df_trans = loader.df_tran[loader.df_tran["Transformer Status"] == "In-Service"]
        self.df_bus = loader.df_bus[loader.df_bus["IDE"] != 4]  # Filter buses with IDE != 4

        # Aggregate branch and transformer data
        df_branch = df_line.groupby(["PSS/E From Bus Number", "PSS/E To Bus Number"]).agg({
            "Hour": "mean", "Branch Status": "first", "From PSS/E KV": "mean", "To PSS/E KV": "mean",
            "Branch Name": lambda x: "+".join(x), "b (p.u)": "sum", "RATEA": "sum", "SUS": "sum"
        }).reset_index()

        df_transformer = df_trans.groupby(["PSS/E From Bus Number", "PSS/E To Bus Number"]).agg({
            "Hour": "mean", "Transformer Status": "first", "From PSS/E KV": "mean", "To PSS/E KV": "mean",
            "Branch Name": lambda x: "+".join(x), "SUS": "sum"
        }).reset_index()

        # Combine branch and transformer data
        df_transformer.rename(columns={"Transformer Status": "Status"}, inplace=True)
        df_branch.rename(columns={"Branch Status": "Status"}, inplace=True)
        df_transformer = df_transformer.reindex(columns=df_branch.columns, fill_value=0)
        self.df_branch_trans = pd.concat([df_branch, df_transformer], ignore_index=True)

        # Extract unique buses from branch and transformer data
        self.unique_bus_list = np.sort(pd.unique(self.df_branch_trans[["PSS/E From Bus Number", "PSS/E To Bus Number"]].values.ravel("K")))

        difference = set(self.df_bus["PSS/E Bus Number"]) - set(self.unique_bus_list)
        print(f"Buses not in the branch or transformer data: {difference}")

    def compute_matrices(self):
        # Create adjacency and admittance matrices
        self.adjacency_matrix = pd.DataFrame(0, index=range(len(self.df_branch_trans)), columns=self.unique_bus_list)
        admittance_matrix = pd.DataFrame(0.0, index=self.unique_bus_list, columns=self.unique_bus_list)

        for i, row in self.df_branch_trans.iterrows():
            # Build adjacency and admittance matrices
            self.adjacency_matrix.loc[i, row["PSS/E From Bus Number"]] = 1
            self.adjacency_matrix.loc[i, row["PSS/E To Bus Number"]] = -1
            admittance_matrix.loc[row["PSS/E From Bus Number"], row["PSS/E From Bus Number"]] += row["SUS"]
            admittance_matrix.loc[row["PSS/E To Bus Number"], row["PSS/E To Bus Number"]] += row["SUS"]
            admittance_matrix.loc[row["PSS/E From Bus Number"], row["PSS/E To Bus Number"]] -= row["SUS"]
            admittance_matrix.loc[row["PSS/E To Bus Number"], row["PSS/E From Bus Number"]] -= row["SUS"]

        # Drop the reference bus from the admittance matrix
        ref_bus = int(self.df_bus[self.df_bus["IDE"] == self.ref_bus_ide]["PSS/E Bus Number"].values)
        admittance_matrix.drop(index=ref_bus, columns=ref_bus, inplace=True)

        # Compute inverse of admittance matrix
        sparse_matrix = csr_matrix(admittance_matrix.values)
        inverse_df = pd.DataFrame(inv(sparse_matrix).toarray())

        # Drop reference bus from adjacency matrix
        self.adjacency_matrix.drop(columns=ref_bus, inplace=True)

        # Calculate shift factor matrix
        sus_matrix = pd.DataFrame(np.diag(self.df_branch_trans["SUS"]))
        shift_factor = sus_matrix.values @ self.adjacency_matrix.values @ inverse_df.values
        unique_bus_list_update = self.unique_bus_list[self.unique_bus_list != ref_bus]
        self.df_shift_factor = pd.DataFrame(shift_factor, columns=unique_bus_list_update)

    def get_results(self):
        # Return the adjacency matrix, shift factor matrix, and branch data
        return self.adjacency_matrix, self.df_shift_factor, self.df_branch_trans

# Example Usage
# sf = ShiftFactor(hour_ending, path, path_row=path_row)
# sf.compute_matrices()
# adj_matrix, df_shift_factor, df_branch_trans = sf.get_results()
