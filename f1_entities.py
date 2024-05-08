class F1_Entities:
    def __init__(self):
        self.entities = \
        {
            'circuits': dict(), #k
            'seasons': dict(),
            'races': dict(),
            'constructors': dict(),
            'drivers': dict(),
            'status': dict(),
            'driver_standings': dict(),
            'constructor_results': dict(),
            'constructor_standings': dict(),
            'results': dict(),
            'pit_stops': dict(),
            'lap_times': dict(),
            'sprint_results': dict(),
            'qualifying': dict()
        }
        self.attributes = \
        {
            'schema': dict(), #v
            'silver_name': '',
            'gold_name': '',
            'hive_metastore_silver_path': 'dbfs:/user/hive/warehouse/silver.db',
            'hive_metastore_gold_path': 'dbfs:/user/hive/warehouse/gold.db',
            'blob_bronce_path': '/mnt/bronce',
            'blob_silver_path': '/mnt/silver',
            'blob_gold_path': '/mnt/gold',
            'is_dim': False,
            'dw_name': ''
        }
        self.set_attributes()
    
    def set_attributes(self):
        self.entities = {k:self.attributes.copy() for (k,_) in self.entities.items()}
        self.entities['circuits']['is_dim'] = True
        self.entities['seasons']['is_dim'] = True
        self.entities['races']['is_dim'] = True
        self.entities['constructors']['is_dim'] = True
        self.entities['status']['is_dim'] = True
        self.entities['drivers']['is_dim'] = True

        