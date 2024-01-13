
from schemas.miner import MinerMetadata

def convert_miner_catalog_to_miner_cfg(minerCatalog:MinerMetadata)->dict:
    miner_cfg_dict=minerCatalog.model_dump()
    miner_cfg_dict['timestep_hours']=miner_cfg_dict['timestep']['hours']
    miner_cfg_dict['timestep_days']=miner_cfg_dict['timestep']['days']
    miner_cfg_dict['timestep_minutes']=miner_cfg_dict['timestep']['minutes']
    miner_cfg_dict['start_date_month']=miner_cfg_dict['start_date']['month']
    miner_cfg_dict['start_date_day']=miner_cfg_dict['start_date']['day']
    miner_cfg_dict['start_date_year']=miner_cfg_dict['start_date']['year']
    miner_cfg_dict['start_date_hour']=miner_cfg_dict['start_date']['hour']
    miner_cfg_dict['end_date_month']=miner_cfg_dict['end_date']['month']
    miner_cfg_dict['end_date_day']=miner_cfg_dict['end_date']['day']
    miner_cfg_dict['end_date_year']=miner_cfg_dict['end_date']['year']
    miner_cfg_dict['end_date_hour']=miner_cfg_dict['end_date']['hour']
    miner_cfg_dict['target_symbols']=','.join(miner_cfg_dict['target_symbols'])
    del miner_cfg_dict['timestep'],miner_cfg_dict['id'],miner_cfg_dict['start_date'],miner_cfg_dict['end_date']
    return miner_cfg_dict