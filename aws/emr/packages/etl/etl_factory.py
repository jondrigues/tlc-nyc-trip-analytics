from packages.business_logic.yellow.yellow_extractor import YellowExtractor
from packages.business_logic.yellow.yellow_transformer import YellowTransformer
from packages.business_logic.yellow.yellow_quality_checker import YellowQualityChecker
from packages.business_logic.green.green_extractor import GreenExtractor
from packages.business_logic.green.green_transformer import GreenTransformer
from packages.business_logic.green.green_quality_checker import GreenQualityChecker
from packages.business_logic.trip_data.trip_data_transformer import TripDataTransformer


class ETLFactory:

    etl_class = {
        'green': {
            'extract': GreenExtractor,
            'transform': GreenTransformer,
            'quality_check': GreenQualityChecker
        },
        'yellow': {
            'extract': YellowExtractor,
            'transform': YellowTransformer,
            'quality_check': YellowQualityChecker
        },
        'trip_data': {
            'transform': TripDataTransformer
        }
    }

    def __init__(self, entity_name, process):
        self.entity_name = entity_name
        self.process = process

    def get_etl_class(self):
        return self.etl_class[self.entity_name][self.process]
