from hdensity.mock.mock_sensor import *
from hdensity.util.util_func import *

if __name__ == '__main__':
    loadenv()
    client = get_mqtt_client()
    try:
        SENSOR_ID = str(int(sys.argv[1]))
        mock_sensor(client, SENSOR_ID)
    except:
        mock_all_sensor(client)
