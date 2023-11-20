from .data_stream_base import DataStreamBase

class LongtermLiabilitiesStream(DataStreamBase):
    def __init__(self, signal_name, timestep, version, timestamp_field='timestamp', symbol_field='symbol'):
        stream_fields = [('Symbol', 'object'),
                        ('Company ID', 'int64'),
                        ('Year Period', 'int64'),
                        ('Term Code', 'object'),
                        ('Term Name', 'object'),
                        ('Term Name En', 'object'),
                        ('Report Term ID', 'int64'),
                        ('Audited Status', 'object'),
                        ('Period Begin', 'int64'),
                        ('Period End', 'int64'),
                        ('Report Date', 'object'),
                        ('Created Date', 'object'),
                        ('Last Update', 'object'),
                        ('Date Pub Department', 'datetime64[ns]'),
                        ('Report Norm ID', 'int64'),
                        ('Name', 'object'),
                        ('Name En', 'object'),
                        ('Value', 'float64')]
        
        super().__init__(signal_name, timestep, version, stream_fields, timestamp_field, symbol_field)
        
#liabilities = hpg[['Symbol', 'Company ID', 'Year Period', 'Term Code',
#       'Term Name', 'Term Name En', 'Report Term ID', 
#       'Audited Status', 'Period Begin', 'Period End', 'Report Date',
#       'Created Date' , 'Last Update', 'Date Pub Department',
#       'Report Norm ID', 'Name',
#       'Name En', 'Value']]
#ltl_stream = LongtermLiabilitiesStream('longterm_liabilities', 
#                                  timedelta(seconds=0), 
#                                   version=0.1, 
#                                   description='abc',
#                                    owner='owner',
#                                  stream_record_fields=list(zip(liabilities.columns, liabilities.dtypes)))