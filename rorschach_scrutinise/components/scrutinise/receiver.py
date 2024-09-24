import pandas as pd
import time
from os import getenv
from nite_howl import NiteHowl, minute

class Receiver:
    def __init__(self):
        self.broker = getenv('BROKER')
        self.topic = getenv('TOPIC')
        self.group = getenv('GROUP')
        self.env =  getenv('ENV_PATH')
        self.howler = NiteHowl(self.broker, self.group, str(self.topic).split(","), "scrutinise")
        
    def catch(self):
        radar_generator = self.howler.radar()
        primary = None
        secondary = None
        while True:
            try:
                minute.register("info", f"Searching topics for scrutine...")
                table, topic, key, headers, type = next(radar_generator)
                if topic == "scrutinise":
                    if headers and "side" in headers.keys() and headers["side"] == "right":
                        secondary = table
                    elif headers and "side" in headers.keys() and headers["side"] == "left":
                        primary = table
                
                    if isinstance(primary, pd.DataFrame) and isinstance(secondary, pd.DataFrame) and not primary.empty and not secondary.empty:
                        self.merge(key, primary, secondary)
                        primary = None
                        secondary = None
                else:
                    minute.register("info", f"That larva '{key}' won't need me.")
            except StopIteration:
                radar_generator = self.howler.radar()
            time.sleep(0.1)
        
    def merge(self, key, policy_from_csv, policy_from_crm):
        left_inner, right_inner, left_outer, right_outer = self.diff(policy_from_csv, policy_from_crm)
        full_outer = pd.concat([left_outer, right_outer], axis=0).reset_index(drop=True)
        self.howler.send("statement", msg=left_inner, key=key, headers={"side": "left"})
        self.howler.send("statement", msg=right_inner, key=key, headers={"side": "right"})
        #pd.set_option('display.max_rows', 100)
        #pd.set_option('display.max_columns', 100)
        #diff_table = pd.concat(
        #    [
        #        left_inner.iloc[0],
        #        right_inner.iloc[0],
        #        left_inner.iloc[0] == right_inner.iloc[0]
        #    ],
        #    axis=1,
        #    keys=["LEFT", "RIGHT", "DIFF"],
        #)
        #print(diff_table)
        
        #self.commit(left_inner, right_inner)
        #self.commit(full_outer)
    
    def refactor(self, merged, suffix, columns, types):
        ind_rename = merged.drop("_merge", axis=1)
        if hasattr(ind_rename,f'salesorder_no{suffix}'):
            ind_rename[f'salesorder_no'] = ind_rename[f'salesorder_no{suffix}']
            ind_rename = ind_rename.drop(f'salesorder_no{suffix}', axis=1)
        if hasattr(ind_rename,f'member_id{suffix}'):
            ind_rename[f'member_id'] = ind_rename[f'member_id{suffix}']
            ind_rename = ind_rename.drop(f'member_id{suffix}', axis=1)
        if hasattr(ind_rename,f'ffm_subscriber_id{suffix}'):
            ind_rename[f'ffm_subscriber_id'] = ind_rename[f'ffm_subscriber_id{suffix}']
            ind_rename = ind_rename.drop(f'ffm_subscriber_id{suffix}', axis=1)
        ind_rename.columns = ind_rename.columns.str.replace(suffix, "")
        ind_rename = ind_rename.reindex(columns=columns)
        for col in ind_rename.columns:
            if pd.api.types.is_float_dtype(ind_rename[col]) and not pd.api.types.is_float_dtype(types[col]):
                ind_rename[col] = ind_rename[col].fillna(0)
            if pd.api.types.is_object_dtype(ind_rename[col]) and pd.api.types.is_object_dtype(types[col]):
                ind_rename[col] = ind_rename[col].fillna("")
        ind_rename = ind_rename.astype(types)
        ind_rename = ind_rename.reset_index(drop=True)
        return ind_rename

    def refactor_df(self, existing_statement, columns):
        policy_series = pd.DataFrame.from_dict(existing_statement.__dict__, orient='index').T
        columns_to_drop = ['_sa_instance_state', 'id']  # Añadir cualquier otra columna que no sea necesaria
        policy_df = policy_series.drop(columns=columns_to_drop)
        policy_df = policy_df.reindex(columns=columns)
        policy_df = policy_df.astype(bool)
        return policy_df.iloc[0]

    def split(self, left, right, id, suffix_left, suffix_right, columns, types):
        left_null_id, left_not_null_id = left[left[id] == ""], left[left[id] != ""]
        right_null_id, right_not_null_id = right[right[id] == ""], right[right[id] != ""]
        
        ind_outer = pd.merge(
            left_not_null_id,
            right_not_null_id,
            how="outer",
            on=[id],
            indicator=True,
            suffixes=(suffix_left, suffix_right),
        )

        ind_left = ind_outer[ind_outer["_merge"] == 'left_only'].copy()
        ind_left_rename = self.refactor(ind_left, suffix_left, columns, types)
        ind_left_rename = pd.concat([left_null_id, ind_left_rename], axis=0)
        
        ind_right = ind_outer[ind_outer["_merge"] == 'right_only'].copy()
        ind_right_rename = self.refactor(ind_right, suffix_right, columns, types)
        ind_right_rename = pd.concat([right_null_id, ind_right_rename], axis=0)
                
        ind_inner = ind_outer[ind_outer["_merge"] == 'both'].copy()
        ind_inner.loc[:, f'{id}{suffix_left}'] = ind_inner[id]
        ind_inner.loc[:, f'{id}{suffix_right}'] = ind_inner[id]
        
        return ind_left_rename, ind_right_rename, ind_inner
        
    def diff(self, left, right):
        ind_left_ffm_subscriber_id, ind_right_ffm_subscriber_id, ind_inner_ffm_subscriber_id = self.split(
            left, right, "ffm_subscriber_id", "_csv", "_crm", left.columns, left.dtypes.to_dict()
        )
        
        ind_left_member_id, ind_right_member_id, ind_inner_member_id = self.split(
            ind_left_ffm_subscriber_id, ind_right_ffm_subscriber_id, "member_id", "_csv", "_crm", left.columns, left.dtypes.to_dict()
        )
        
        #ind_left_csv_crm, ind_right_csv_crm, ind_inner_salesorder_no = self.split(
        #    ind_left_member_id, ind_right_member_id, "salesorder_no", "_csv", "_crm", left.columns, left.dtypes.to_dict()
        #)

        ind_inner_full = pd.concat([ind_inner_ffm_subscriber_id, ind_inner_member_id], axis=0).reset_index(drop=True)
        
        ind_inner_csv = self.refactor(ind_inner_full, '_csv', left.columns, left.dtypes.to_dict())
        ind_inner_crm = self.refactor(ind_inner_full, '_crm', right.columns, left.dtypes.to_dict())

        return ind_inner_csv, ind_inner_crm, ind_left_member_id, ind_right_member_id