### for loop a->z
count=0
for i in range(97, 123):
    for j in range(97, 123):
        for k in range(97, 123):
            if chr(i)==chr(j) and chr(j)==chr(k):
                print(chr(i)+chr(j)+chr(k), count)
                count=0
            else:
                count+=1




def get_inputs(timestamp: datetime, target_symbols: List[str], input_streams: Dict[str, DataStreamBase]) -> List[Node]:
    bctc_ck_3 = input_streams['wifeed_bctc_thuyet_minh_chung_khoan'].get_record_range(
        included_min_timestamp=timestamp + relativedelta(years=-3),
        included_max_timestamp=datetime.strftime(timestamp, '%Y-%m-%d 23:59:59'),
    ) 
    bctc_nam=input_streams['calculate_bctc_nam_chung_khoan'].get_record_range(
        included_min_timestamp=datetime(timestamp.year, 1, 1),
        included_max_timestamp=timestamp,
    )  
    bctc_ck_1 = input_streams['wifeed_bctc_thuyet_minh_chung_khoan'].get_record_range(
        included_min_timestamp=timestamp + relativedelta(years=-1),
        included_max_timestamp=datetime.strftime(timestamp, '%Y-%m-%d 23:59:59'),
    )
    return [Node(name="bctc_ck_1", dataframe=bctc_ck_1),
            Node(name="bctc_ck_3", dataframe=bctc_ck_3),
            Node(name="bctc_nam",dataframe=bctc_nam)]

def process_per_symbol(symbol: str, timestamp: datetime, inputs: Dict[str, Node]) -> Node:
    bctc_ck_1_data = inputs['bctc_ck_1'].dataframe
    bctc_ck_3_data=inputs['bctc_ck_3'].dataframe
    bctc_nam_data=inputs['bctc_nam'].dataframe
    bctc_ck_3_data["nam"]=bctc_ck_3_data["nam"]+1
    return Node(name="output", dataframe=bctc_ck_3_data)