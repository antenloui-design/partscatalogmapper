from datetime import datetime
import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
import logging
import traceback
import threading
from pathlib import Path

# Logging setup
logging.basicConfig(
    filename='parts_catalog_mapper.log',
    filemode='a',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

OUTPUT_COLUMNS = [
    "Supplier", "ItemCode", "Description", "PurchasePrice",
    "SalesPrice", "SV_ManufacturerId", "ListCategory",
    "MarinaLocationId", "AdditionDatetime"
]

NUMERIC_COLUMNS = ["PurchasePrice", "SalesPrice"]

class ColumnMapperApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Parts Catalog Column Mapper")
        self.source_file = None
        self.marina_catalog_file = None
        self.source_columns = []
        self.text_inputs = {}
        self.dropdowns = {}
        self.df = None
        self.df_mapped = None
        self.df_catalog = None

        self.setup_ui()

    def setup_ui(self):
        tk.Label(self.root, text="Step 1: Map Source File").pack(pady=(5, 0))
        tk.Button(self.root, text="Select Source File", command=self.load_file).pack(pady=5)

        self.mapping_frame = tk.Frame(self.root)
        self.mapping_frame.pack()

        btn_frame = tk.Frame(self.root)
        btn_frame.pack(pady=10)
        tk.Button(btn_frame, text="Generate Mapped Output", command=lambda: self.run_in_thread(self.generate_output)).grid(row=0, column=0, padx=5)

        tk.Label(self.root, text="Step 2: Compare & Export Updated Catalog").pack(pady=(15, 0))
        tk.Button(self.root, text="Load Marina Parts Catalog", command=self.load_marina_catalog).pack(pady=5)
        tk.Button(self.root, text="Export Updated Marina Catalog", command=self.start_export_updated_catalog).pack(pady=5)

    def run_in_thread(self, func):
        threading.Thread(target=func, daemon=True).start()

    def _on_ui_thread(self, callback, *args, **kwargs):
        self.root.after(0, lambda: callback(*args, **kwargs))

    def _show_info(self, title, message):
        self._on_ui_thread(messagebox.showinfo, title, message)

    def _show_warning(self, title, message):
        self._on_ui_thread(messagebox.showwarning, title, message)

    def _show_error(self, title, message):
        self._on_ui_thread(messagebox.showerror, title, message)

    def _read_input_file(self, file_path):
        suffix = Path(file_path).suffix.lower()
        if suffix == ".csv":
            return pd.read_csv(file_path, dtype=str)
        return pd.read_excel(file_path, dtype=str)

    def load_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel/CSV Files", "*.xlsx *.csv")])
        if not file_path:
            return
        try:
            self.df = self._read_input_file(file_path)
            self.source_columns = [""] + self.df.columns.tolist()
            self.build_mapping_ui()
        except Exception as e:
            logging.error("Failed to load file", exc_info=True)
            self._show_error("Error", f"Failed to load file: {e}")

    def build_mapping_ui(self):
        for widget in self.mapping_frame.winfo_children():
            widget.destroy()
        self.text_inputs.clear()
        self.dropdowns.clear()
        for i, col in enumerate(OUTPUT_COLUMNS):
            tk.Label(self.mapping_frame, text=col).grid(row=i, column=0, sticky="w", padx=5, pady=2)
            tk.Label(self.mapping_frame, text="→ Input:").grid(row=i, column=1, padx=3)
            if col == "MarinaLocationId":
                entry = tk.Entry(self.mapping_frame)
                entry.grid(row=i, column=2, padx=5)
                self.text_inputs[col] = entry
            elif col == "AdditionDatetime":
                today_str = datetime.today().strftime("%m/%d/%Y")
                label = tk.Label(self.mapping_frame, text=today_str)
                label.grid(row=i, column=2, padx=5)
                self.text_inputs[col] = today_str
            else:
                var = tk.StringVar()
                dropdown = ttk.Combobox(self.mapping_frame, textvariable=var, values=self.source_columns, state="readonly")
                dropdown.grid(row=i, column=2, padx=5)
                self.dropdowns[col] = var

    def generate_output(self):
        try:
            self.df_mapped = pd.DataFrame(columns=OUTPUT_COLUMNS)
            for col in OUTPUT_COLUMNS:
                if col in self.dropdowns and self.dropdowns[col].get():
                    self.df_mapped[col] = self.df[self.dropdowns[col].get()]
                elif col in self.text_inputs:
                    value = self.text_inputs[col]
                    self.df_mapped[col] = value.get() if isinstance(value, tk.Entry) else value
            self.df_mapped[NUMERIC_COLUMNS] = self.df_mapped[NUMERIC_COLUMNS].apply(pd.to_numeric, errors='coerce')
            self._show_info("Success", "Mapped data generated.")
        except Exception as e:
            logging.error("Error generating mapped output", exc_info=True)
            self._show_error("Error", f"Error generating mapped output: {e}")

    def load_marina_catalog(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel/CSV Files", "*.xlsx *.csv")])
        if not file_path:
            return
        try:
            self.df_catalog = self._read_input_file(file_path)
            self._show_info("Success", "Marina Parts Catalog loaded successfully.")
        except Exception as e:
            logging.error("Error loading marina catalog", exc_info=True)
            self._show_error("Error", f"Error loading marina catalog: {e}")

    def start_export_updated_catalog(self):
        folder = filedialog.askdirectory(title="Select Folder to Save Exported Files")
        if not folder:
            return
        self.run_in_thread(lambda: self.export_updated_catalog(folder))

    def export_updated_catalog(self, folder):
        if self.df_mapped is None or self.df_catalog is None:
            self._show_warning("Warning", "Mapped data or catalog not loaded.")
            return
        try:
            catalog = self.df_catalog.copy()
            original_catalog_columns = catalog.columns.tolist()
            mapped = self.df_mapped.copy()

            catalog["PurchasePrice"] = pd.to_numeric(catalog["PurchasePrice"], errors="coerce")
            catalog["SalesPrice"] = pd.to_numeric(catalog["SalesPrice"], errors="coerce")
            mapped["PurchasePrice"] = pd.to_numeric(mapped["PurchasePrice"], errors="coerce")
            mapped["SalesPrice"] = pd.to_numeric(mapped["SalesPrice"], errors="coerce")

            catalog = catalog.drop_duplicates(subset="ItemCode", keep="first")
            catalog = catalog.set_index("ItemCode")
            new_items = []

            for _, row in mapped.iterrows():
                itemcode = row["ItemCode"]
                if itemcode in catalog.index:
                    cat_row = catalog.loc[itemcode]
                    if (
                        not pd.isna(row["PurchasePrice"]) and
                        not pd.isna(row["SalesPrice"]) and
                        (
                            cat_row["PurchasePrice"] != row["PurchasePrice"] or
                            cat_row["SalesPrice"] != row["SalesPrice"]
                        )
                    ):
                        catalog.at[itemcode, "PurchasePrice"] = row["PurchasePrice"]
                        catalog.at[itemcode, "SalesPrice"] = row["SalesPrice"]
                else:
                    new_items.append(row)

            catalog.reset_index(inplace=True)
            catalog = catalog[original_catalog_columns]
            df_new = pd.DataFrame(new_items)

            if not df_new.empty:
                df_new.insert(0, "Id", "*")
                df_new.insert(df_new.columns.get_loc("MarinaLocationId"), "RecordStatusId", 1)
                df_new["ItemMaster_Id"] = "NULL"
                df_new["AspNetUser_Id"] = "NULL"
                df_new["SupersededItemCode"] = "NULL"

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            updated_path = os.path.join(folder, f"updated_catalog_{timestamp}.xlsx")
            new_items_path = os.path.join(folder, f"new_items_{timestamp}.xlsx")

            catalog.to_excel(updated_path, sheet_name="Updated Catalog", index=False)

            if not df_new.empty:
                df_export = df_new.drop(columns=[
                    "Id", "RecordStatusId", "ItemMaster_Id",
                    "AspNetUser_Id", "SupersededItemCode"
                ], errors="ignore")
                df_export.to_excel(new_items_path, sheet_name="New Items", index=False)

            self._show_info("Success", f"Exported files to:\n{folder}")
        except Exception as e:
            logging.error("Export error", exc_info=True)
            self._show_error("Error", f"Export error: {traceback.format_exc()}")

if __name__ == "__main__":
    root = tk.Tk()
    app = ColumnMapperApp(root)
    root.mainloop()
