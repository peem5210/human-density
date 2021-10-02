import json
ACTION_ENUM : list[int]= [0, 1]
class Payload:
    def __init__(self, *args):
        if len(args) == 2:
            self.sensor_id = str(args[0])
            self.action = args[1]
        else:
            obj = json.loads(args[0])
            self.sensor_id = obj['sensor_id']
            self.action = obj['action']
            
        if self.action not in ACTION_ENUM:
            raise ValueError(f"invalid action {self.action}")

    def serialize(self) -> str:
        return json.dumps(self.__dict__)

