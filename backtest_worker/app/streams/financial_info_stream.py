from .data_stream_base import DataStreamBase

class FinancialInfoStream(DataStreamBase):
    def __init__(self, signal_name, timestep, version, timestamp_field='timestamp', symbol_field='symbol'):
        financial_info_stream_fields = [('ID', 'int64'),
                                    ('Symbol', 'object'),
                                    ('Term Type', 'int64'),
                                    ('Company ID', 'int64'),
                                    ('Year Period', 'int64'),
                                    ('Term Code', 'object'),
                                    ('Term Name', 'object'),
                                    ('Term Name En', 'object'),
                                    ('Report Term ID', 'int64'),
                                    ('United', 'object'),
                                    ('Audited Status', 'object'),
                                    ('Period Begin', 'int64'),
                                    ('Period End', 'int64'),
                                    ('Report Date', 'object'),
                                    ('Created Date', 'object'),
                                    ('Audit Status ID', 'int64'),
                                    ('Last Update', 'object'),
                                    ('Date Pub Department', 'datetime64[ns]'),
                                    ('Business Type', 'int64'),
                                    ('Type', 'object'),
                                    ('Group Type', 'object'),
                                    ('Report Norm ID', 'int64'),
                                    ('Name', 'object'),
                                    ('Name En', 'object'),
                                    ('Parent Report Norm ID', 'int64'),
                                    ('Report Component Name', 'object'),
                                    ('Report Component Name En', 'object'),
                                    ('Report Component Type ID', 'float64'),
                                    ('Value', 'float64')]
        
        super().__init__(signal_name, timestep, version, financial_info_stream_fields, timestamp_field, symbol_field)
            
    